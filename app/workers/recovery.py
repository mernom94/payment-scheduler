"""
app/workers/recovery.py — Recovery Worker.

Runs only on the elected leader.  On each tick:

  1. Detect RUNNING job_runs whose lock_expires_at has passed (stuck runs).
  2. CAS-style reclaim: atomically transition RUNNING → FAILED via a single
     UPDATE WHERE that re-checks expiry.  Only the worker that wins the CAS
     processes the run; concurrent recovery workers skip it silently.
  3. Route to retry (new JobRun row) or DLQ (policy exhausted).
  4. Detect READY/PENDING/BLOCKED jobs with a permanently-failed (DEAD) DAG
     dependency and cancel them.

CAS correctness
---------------
The CAS UPDATE pattern (UPDATE ... WHERE status=RUNNING AND lock_expires_at < now
... RETURNING) is the sole correctness mechanism for stuck-run recovery.  Unlike
SELECT FOR UPDATE SKIP LOCKED (which holds a lock for the entire processing
duration), the CAS UPDATE is a single atomic statement.  If two recovery workers
race on the same run:
  - One UPDATE matches the WHERE clause and returns the row.
  - The other UPDATE matches nothing (row no longer RUNNING) and returns nothing.

This means recovery is safe to run on multiple nodes simultaneously without
additional coordination, and does not require the recovery worker to hold leader
status — though it does so anyway for operational simplicity.

Orphan scan ordering
--------------------
The orphaned DAG job scan uses ORDER BY created_at to ensure the oldest orphans
are handled first (FIFO), preventing a large-offset starvation scenario where
the same non-deterministic LIMIT subset is processed on every tick.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

import structlog
from sqlalchemy import and_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.config import get_settings
from app.core.constants import (
    CONSECUTIVE_FAILURE_PAUSE_THRESHOLD,
    ERROR_MESSAGE_MAX_LEN,
)
from app.domain.models import JobRunStatus, JobStatus, RetryPolicy
from app.infrastructure.db.models import DeadLetterEntry, JobRun, ScheduledJob, Subscription
from app.infrastructure.db.session import get_session_factory
from app.infrastructure.messaging.workers import BaseWorker
from app.infrastructure.redis.leader import LeaderElection, create_redis
from observability.setup import RECOVERY_RUNS_TOTAL, RETRY_COUNTER, get_tracer, record_dlq_entry

logger = logging.getLogger(__name__)


class RecoveryWorker(BaseWorker):
    """
    Leader-only worker that reaps stuck RUNNING runs and cancels orphaned DAG jobs.
    """

    name = "recovery_worker"

    @property
    def poll_interval(self) -> float:
        return get_settings().RECOVERY_INTERVAL_S

    def __init__(
        self,
        session_factory=None,
    ) -> None:
        super().__init__()
        self._injected_factory = session_factory
        self._redis = create_redis()
        self._election = LeaderElection(self._redis)

    def _factory(self):
        """Return the active session factory (injected or global)."""
        return self._injected_factory if self._injected_factory is not None else get_session_factory()

    async def stop(self) -> None:
        """Extend BaseWorker.stop() to release leadership and close Redis."""
        super().stop()
        await self._election.release()
        await self._redis.aclose()

    # ── BaseWorker contract ────────────────────────────────────────────────────

    async def tick(self) -> None:
        if not self._election.is_leader:
            acquired = await self._election.try_acquire()
            if not acquired:
                logger.debug("recovery.not_leader")
                return

        if not await self._election.verify_still_leader():
            return

        tracer = get_tracer()
        with tracer.start_as_current_span(
            "recovery.tick",
            attributes={"epoch": self._election.epoch},
        ):
            await self._recover_stuck_runs()

            # Re-verify before the second write phase.
            if not await self._election.verify_still_leader():
                return

            await self._cancel_orphaned_dag_jobs()

    # ── Stuck RUNNING runs ─────────────────────────────────────────────────────

    async def _recover_stuck_runs(self) -> None:
        """
        Fetch candidate stuck-run IDs, then CAS-claim each one individually.

        ID fetch is a read-only snapshot — no locks held.  Each CAS UPDATE is
        atomic and safe to run concurrently with other recovery workers or the
        executor.
        """
        now = datetime.now(timezone.utc)

        async with self._factory()() as session:
            result = await session.execute(
                select(JobRun.id)
                .where(
                    and_(
                        JobRun.status == JobRunStatus.RUNNING,
                        JobRun.lock_expires_at < now,
                    )
                )
                .order_by(JobRun.lock_expires_at)   # oldest expiry first
                .limit(50)
            )
            candidate_ids = result.scalars().all()

        if not candidate_ids:
            return

        recovered = 0
        for run_id in candidate_ids:
            did_recover = await self._recover_single_run_by_id(run_id, now)
            if did_recover:
                recovered += 1

        logger.info(
            "recovery.stuck_runs_processed",
            extra={"candidates": len(candidate_ids), "recovered": recovered},
        )
        if recovered:
            RECOVERY_RUNS_TOTAL.inc(recovered)

    async def _recover_single_run_by_id(
        self, run_id: uuid.UUID, now: datetime
    ) -> bool:
        """
        CAS-claim a stuck run by transitioning RUNNING → FAILED atomically.

        Returns True if this worker claimed and processed the run.
        Returns False if another worker won the CAS (no-op UPDATE).
        """
        async with self._factory()() as session:
            try:
                cas_result = await session.execute(
                    update(JobRun)
                    .where(
                        and_(
                            JobRun.id == run_id,
                            JobRun.status == JobRunStatus.RUNNING,
                            JobRun.lock_expires_at < now,
                        )
                    )
                    .values(
                        status=JobRunStatus.FAILED,
                        error_message="Recovered: lock expired without completion",
                        finished_at=now,
                        lock_expires_at=None,
                        worker_id=None,
                    )
                    .returning(JobRun.job_id, JobRun.attempt)
                )
                claimed = cas_result.one_or_none()

                if claimed is None:
                    # Another worker beat us to this run.
                    return False

                job_id, attempt = claimed

                # Load the subscription for the policy check within this transaction
                # so we don't need a second round-trip.
                sub_result = await session.execute(
                    select(Subscription)
                    .join(ScheduledJob, ScheduledJob.subscription_id == Subscription.id)
                    .where(ScheduledJob.id == job_id)
                )
                subscription = sub_result.scalar_one_or_none()
                await session.commit()
            except Exception:
                await session.rollback()
                raise

        if subscription is None:
            logger.error(
                "recovery.subscription_not_found",
                extra={"job_id": str(job_id), "run_id": str(run_id)},
            )
            return True  # We claimed the run; nothing more we can do.

        structlog.contextvars.bind_contextvars(
            job_run_id=str(run_id),
            job_id=str(job_id),
            attempt=attempt,
        )
        try:
            policy = RetryPolicy.from_dict(subscription.retry_policy or {})

            if policy.is_exhausted(attempt):
                # attempt is the 1-based index of the run that just failed.
                # is_exhausted(N) is True when N >= max_attempts, meaning the
                # Nth attempt was the last allowed — route to DLQ.
                logger.warning("recovery.exhausted_moving_to_dlq")
                run_stub = _RunStub(id=run_id, job_id=job_id, attempt=attempt)
                await self._move_to_dlq(
                    run=run_stub,
                    subscription=subscription,
                    reason=(
                        f"Exhausted after {attempt} attempts "
                        f"(recovered from stuck RUNNING state)"
                    ),
                )
            else:
                next_attempt = attempt + 1
                async with self._factory()() as session:
                    try:
                        await session.execute(
                            pg_insert(JobRun)
                            .values(
                                id=uuid.uuid4(),
                                job_id=job_id,
                                attempt=next_attempt,
                                status=JobRunStatus.PENDING,
                                retry_after=None,
                            )
                            .on_conflict_do_nothing(constraint="uq_run_per_attempt")
                        )
                        await session.commit()
                    except Exception:
                        await session.rollback()
                        raise
                RETRY_COUNTER.inc()
                logger.info(
                    "recovery.run_reset",
                    extra={"next_attempt": next_attempt},
                )
        finally:
            structlog.contextvars.clear_contextvars()

        return True

    # ── Orphaned DAG job cancellation ─────────────────────────────────────────

    async def _cancel_orphaned_dag_jobs(self) -> None:
        """
        Cancel jobs whose DAG dependency has permanently failed (DEAD run).

        ORDER BY created_at ensures oldest orphans are processed first (FIFO),
        preventing a non-deterministic LIMIT from cycling over the same subset.
        """
        async with self._factory()() as session:
            result = await session.execute(
                select(ScheduledJob)
                .where(
                    and_(
                        ScheduledJob.status.in_(
                            [JobStatus.PENDING, JobStatus.READY, JobStatus.BLOCKED]
                        ),
                        ScheduledJob.depends_on != [],
                    )
                )
                .order_by(ScheduledJob.created_at)   # deterministic; oldest first
                .limit(100)
            )
            jobs = result.scalars().all()

        for job in jobs:
            await self._evaluate_dag_job(job)

    async def _evaluate_dag_job(self, job: ScheduledJob) -> None:
        """Cancel a job if any of its dependencies has a DEAD run."""
        if not job.depends_on:
            return

        async with self._factory()() as session:
            try:
                for dep_id_str in job.depends_on:
                    dep_id = uuid.UUID(dep_id_str)
                    dep_result = await session.execute(
                        select(ScheduledJob).where(ScheduledJob.id == dep_id)
                    )
                    dep = dep_result.scalar_one_or_none()
                    if dep is None:
                        continue

                    dead_result = await session.execute(
                        select(JobRun).where(
                            and_(
                                JobRun.job_id == dep.id,
                                JobRun.status == JobRunStatus.DEAD,
                            )
                        )
                    )
                    if dead_result.scalar_one_or_none() is None:
                        continue

                    # This dependency is permanently dead — cancel the downstream job.
                    await session.execute(
                        update(ScheduledJob)
                        .where(ScheduledJob.id == job.id)
                        .values(status=JobStatus.CANCELLED)
                    )
                    await session.execute(
                        update(JobRun)
                        .where(
                            and_(
                                JobRun.job_id == job.id,
                                JobRun.status == JobRunStatus.PENDING,
                            )
                        )
                        .values(
                            status=JobRunStatus.FAILED,
                            error_message=(
                                f"DAG dependency {dep_id_str} permanently failed"
                            ),
                            finished_at=datetime.now(timezone.utc),
                        )
                    )
                    await session.commit()
                    logger.info(
                        "dag.downstream_cancelled",
                        extra={
                            "job_id": str(job.id),
                            "failed_dep_id": dep_id_str,
                        },
                    )
                    return  # One dead dep is enough to cancel
            except Exception:
                await session.rollback()
                raise

    # ── DLQ routing ────────────────────────────────────────────────────────────

    async def _move_to_dlq(
        self,
        run: "_RunStub",
        subscription: Subscription,
        reason: str,
    ) -> None:
        now = datetime.now(timezone.utc)
        truncated_reason = reason[:ERROR_MESSAGE_MAX_LEN]

        async with self._factory()() as session:
            try:
                await session.execute(
                    update(JobRun)
                    .where(JobRun.id == run.id)
                    .values(
                        status=JobRunStatus.DEAD,
                        error_message=truncated_reason,
                        finished_at=now,
                        lock_expires_at=None,
                        worker_id=None,
                    )
                )
                session.add(
                    DeadLetterEntry(
                        job_run_id=run.id,
                        job_id=run.job_id,
                        subscription_id=subscription.id,
                        failure_reason=truncated_reason,
                        error_class="StuckRunRecovery",
                        payload_snapshot={
                            "payment_config": subscription.payment_config,
                            "attempt": run.attempt,
                        },
                    )
                )
                await session.execute(
                    update(Subscription)
                    .where(Subscription.id == subscription.id)
                    .values(
                        consecutive_failures=Subscription.consecutive_failures + 1
                    )
                )
                refreshed = await session.get(Subscription, subscription.id)
                if (
                    refreshed is not None
                    and refreshed.consecutive_failures >= CONSECUTIVE_FAILURE_PAUSE_THRESHOLD
                ):
                    await session.execute(
                        update(Subscription)
                        .where(Subscription.id == subscription.id)
                        .values(status="PAUSED", paused_at=now)
                    )
                    logger.warning(
                        "subscription.auto_paused",
                        extra={"subscription_id": str(subscription.id)},
                    )
                await session.commit()
            except Exception:
                await session.rollback()
                raise

        record_dlq_entry()
        logger.error(
            "recovery.moved_to_dlq",
            extra={"job_run_id": str(run.id), "reason": truncated_reason[:200]},
        )


class _RunStub:
    """
    Minimal stand-in for a JobRun ORM object used after a CAS claim.

    After the CAS UPDATE we only have the values from the RETURNING clause —
    not a full ORM object.  _RunStub carries exactly what _move_to_dlq needs.
    """

    __slots__ = ("id", "job_id", "attempt")

    def __init__(self, id: uuid.UUID, job_id: uuid.UUID, attempt: int) -> None:
        self.id = id
        self.job_id = job_id
        self.attempt = attempt


async def main() -> None:
    import asyncio
    import signal
    from app.core.logging import configure_logging
    from app.infrastructure.db.session import init_db, close_db

    configure_logging()
    await init_db()

    worker = RecoveryWorker()
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGTERM, lambda: worker.stop())

    try:
        await worker.run()
    finally:
        await close_db()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
