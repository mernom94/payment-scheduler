"""
app/workers/executor.py — Executor Worker.

Multiple instances run concurrently.  Each instance independently polls for
PENDING job_runs, claims them with SELECT FOR UPDATE SKIP LOCKED, and drives
them through to SUCCEEDED or DLQ.

Session factory injection
--------------------------
ExecutorWorker no longer calls get_db_session() (which reads the module-level
global factory) directly.  Instead it accepts an optional ``session_factory``
argument at construction time.  If none is provided it falls back to
``get_session_factory()`` at the moment of first use (production behaviour).

This is the key seam that lets integration tests inject the test-scoped factory
so that the executor writes into the same transaction/schema that the test
verifies — without any monkey-patching of global state.

Idempotency guarantees
----------------------
- Two-phase claim (fetch IDs → re-lock per-row) prevents operating on stale
  ORM objects detached after a bulk-claim commit.
- idempotency_keys INSERT ON CONFLICT DO NOTHING is the final duplicate gate.
- Idempotency pre-check prevents duplicate bunq calls on retried attempts.
- retry_after column enforces backoff: claim query filters future timestamps.
- All state updates (run, job, subscription) are in a single transaction.
- lock_expires_at ensures recovery worker can reap crashed executor runs.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import structlog
from opentelemetry import trace
from sqlalchemy import and_, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import get_settings
from app.core.constants import CONSECUTIVE_FAILURE_PAUSE_THRESHOLD, ERROR_MESSAGE_MAX_LEN
from app.core.exceptions import (
    BunqAmbiguousError,
    BunqPaymentError,
    BunqTransientError,
    ConfigurationError,
    IdempotencyConflictError,
)
from app.domain.models import IdempotencyKey, JobRunStatus, JobStatus, PaymentConfig, RetryPolicy
from app.infrastructure.bunq.client import BunqClient
from app.infrastructure.db.models import (
    DeadLetterEntry,
    IdempotencyKeyRecord,
    JobRun,
    ScheduledJob,
    Subscription,
)
from app.infrastructure.db.session import get_session_factory
from app.infrastructure.messaging.workers import BaseWorker
from observability.setup import (
    EXECUTOR_JOBS_TOTAL,
    EXECUTOR_THROUGHPUT,
    JOB_LATENCY_SECONDS,
    RETRY_COUNTER,
    get_tracer,
    record_dlq_entry,
)

logger = logging.getLogger(__name__)


class ExecutorWorker(BaseWorker):
    """
    Claims and executes PENDING job_runs, producing bunq payments.

    Multiple instances run concurrently (one per pod / process).  Each
    instance gets a unique worker_id that is stamped on claimed rows for
    traceability.

    Parameters
    ----------
    session_factory:
        An async_sessionmaker to use for all DB access.  When None (default,
        production), the factory is resolved lazily from get_session_factory()
        on first use.  Pass an explicit factory in integration tests to bind
        the executor to the test-scoped DB session.
    worker_id:
        Override the auto-generated worker identifier (useful in tests to
        produce deterministic log output).
    """

    name = "executor_worker"

    @property
    def poll_interval(self) -> float:
        return get_settings().EXECUTOR_POLL_INTERVAL_S

    def __init__(
        self,
        session_factory: Optional[async_sessionmaker[AsyncSession]] = None,
        worker_id: Optional[str] = None,
        bunq_client: Optional["BunqClient"] = None,
    ) -> None:
        super().__init__()
        self._injected_factory = session_factory
        self._bunq_client = bunq_client

        env_id = os.environ.get("WORKER_ID", "") or get_settings().WORKER_ID
        self._worker_id = (
            worker_id
            or (env_id if env_id else f"worker-{os.getpid()}-{uuid.uuid4().hex[:8]}")
        )
        logger.info(
            "executor_worker.init",
            extra={"worker_id": self._worker_id},
        )

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _factory(self) -> async_sessionmaker[AsyncSession]:
        """Return the active session factory (injected or global)."""
        return self._injected_factory if self._injected_factory is not None else get_session_factory()

    # ── BaseWorker contract ────────────────────────────────────────────────────

    async def tick(self) -> None:
        await self._process_batch()

    # ── Batch processing ───────────────────────────────────────────────────────

    async def _fetch_pending_run_ids(self) -> list[uuid.UUID]:
        """
        Fetch a batch of claimable job_run IDs and commit immediately.

        The commit releases the FOR UPDATE locks so other workers can proceed
        on the rows that this worker will not process in this batch.  The IDs
        are then re-locked individually in _process_run() to prevent TOCTOU
        races.
        """
        s = get_settings()
        now = datetime.now(timezone.utc)
        batch_size = s.EXECUTOR_BATCH_SIZE

        async with self._factory()() as session:
            try:
                result = await session.execute(
                    select(JobRun.id)
                    .where(
                        and_(
                            JobRun.status == JobRunStatus.PENDING,
                            or_(
                                JobRun.retry_after.is_(None),
                                JobRun.retry_after <= now,
                            ),
                        )
                    )
                    .order_by(JobRun.created_at)
                    .limit(batch_size)
                    .with_for_update(skip_locked=True)
                )
                ids = list(result.scalars().all())
                await session.commit()
                return ids
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    async def _process_batch(self) -> None:
        run_ids = await self._fetch_pending_run_ids()
        if not run_ids:
            return

        logger.info(
            "executor.batch_fetched",
            extra={"count": len(run_ids), "worker_id": self._worker_id},
        )

        batch_start = time.monotonic()
        for run_id in run_ids:
            try:
                await self._process_run(run_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "executor.run_processing_failed",
                    extra={"run_id": str(run_id), "worker_id": self._worker_id},
                )

        elapsed = time.monotonic() - batch_start
        if elapsed > 0:
            EXECUTOR_THROUGHPUT.set(len(run_ids) / elapsed)

    async def _process_run(self, run_id: uuid.UUID) -> None:
        """
        Re-lock, claim, and execute a single job_run.

        Opens a fresh session for each run so that session state from one run
        can never contaminate another.
        """
        start = time.monotonic()
        tracer = get_tracer()

        with tracer.start_as_current_span(
            "executor.process_run",
            attributes={"run_id": str(run_id), "worker_id": self._worker_id},
        ):
            async with self._factory()() as session:
                try:
                    # Re-lock the specific row.  Another worker may have claimed it
                    # in the gap between _fetch_pending_run_ids() and here.
                    result = await session.execute(
                        select(JobRun)
                        .where(
                            and_(
                                JobRun.id == run_id,
                                JobRun.status == JobRunStatus.PENDING,
                            )
                        )
                        .with_for_update(skip_locked=True)
                    )
                    run = result.scalar_one_or_none()
                    if run is None:
                        logger.debug(
                            "executor.run_already_claimed",
                            extra={"run_id": str(run_id)},
                        )
                        await session.commit()
                        return

                    # Load related objects inside the same session.
                    job_result = await session.get(ScheduledJob, run.job_id)
                    if job_result is None:
                        await session.rollback()
                        return
                    sub_result = await session.get(Subscription, job_result.subscription_id)
                    if sub_result is None:
                        await session.rollback()
                        return

                    # Mark as RUNNING and record lock expiry.
                    lock_expires_at = datetime.now(timezone.utc) + timedelta(
                        seconds=get_settings().EXECUTOR_LOCK_TIMEOUT_S
                    )
                    await session.execute(
                        update(JobRun)
                        .where(JobRun.id == run.id)
                        .values(
                            status=JobRunStatus.RUNNING,
                            worker_id=self._worker_id,
                            lock_expires_at=lock_expires_at,
                            started_at=datetime.now(timezone.utc),
                        )
                    )
                    await session.commit()
                except Exception:
                    await session.rollback()
                    raise
                finally:
                    await session.close()

            # Reload fresh objects for the actual execution path (session closed above).
            await self._execute_run(run_id, start)

    async def _execute_run(self, run_id: uuid.UUID, start: float) -> None:
        """Drive the run through payment execution and final state transition."""
        async with self._factory()() as session:
            try:
                run = await session.get(JobRun, run_id)
                if run is None:
                    return
                job = await session.get(ScheduledJob, run.job_id)
                if job is None:
                    return
                subscription = await session.get(Subscription, job.subscription_id)
                if subscription is None:
                    return

                await session.commit()
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

        # All sub-operations open their own short-lived sessions.
        try:
            payment_config = PaymentConfig.from_dict(subscription.payment_config)
        except Exception as exc:
            await self._handle_failure(run, subscription, str(exc), is_retryable=False)
            return

        # DAG dependency check.
        if job.depends_on:
            all_done = await self._check_dag_dependencies(job.depends_on)
            if not all_done:
                await self._mark_dag_cancelled(run)
                return

        # Idempotency pre-check: recover stored result for this (run, attempt).
        existing_key = await self._find_idempotency_key(run.id, run.attempt)
        if existing_key is not None:
            logger.info(
                "executor.idempotency_hit_recovery",
                extra={"run_id": str(run.id), "payment_id": existing_key.bunq_payment_id},
            )
            await self._persist_success(run, subscription, existing_key.bunq_payment_id)
            return

        idempotency_key = IdempotencyKey.for_run(run.id, run.attempt)
        try:
            client = self._bunq_client or BunqClient(get_settings())
            async with client as c:
                response = await c.create_payment(
                    payment_config=payment_config,
                    idempotency_key=str(idempotency_key),
                )
            payment_id = response["Response"][0]["Id"]["id"]
        except BunqTransientError as exc:
            await self._handle_failure(run, subscription, str(exc), is_retryable=True)
            return
        except BunqAmbiguousError as exc:
            await self._handle_failure(run, subscription, str(exc), is_retryable=True)
            return
        except (BunqPaymentError, ConfigurationError) as exc:
            await self._handle_failure(run, subscription, str(exc), is_retryable=False)
            return
        except Exception as exc:
            await self._handle_failure(run, subscription, str(exc), is_retryable=True)
            return

        succeeded = await self._persist_success(run, subscription, payment_id)
        if succeeded:
            latency = time.monotonic() - start
            JOB_LATENCY_SECONDS.observe(latency)
            EXECUTOR_JOBS_TOTAL.labels(outcome="succeeded").inc()
            logger.info(
                "executor.run_succeeded",
                extra={
                    "run_id": str(run.id),
                    "payment_id": payment_id,
                    "latency_s": round(latency, 3),
                },
            )

    async def _check_dag_dependencies(self, depends_on: list[uuid.UUID]) -> bool:
        async with self._factory()() as session:
            try:
                result = await session.execute(
                    select(ScheduledJob.status)
                    .where(ScheduledJob.id.in_(depends_on))
                )
                statuses = list(result.scalars().all())
                await session.commit()
                return all(s == JobStatus.DONE for s in statuses)
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    async def _find_idempotency_key(
        self, run_id: uuid.UUID, attempt: int
    ) -> Optional[IdempotencyKeyRecord]:
        async with self._factory()() as session:
            try:
                result = await session.execute(
                    select(IdempotencyKeyRecord).where(
                        and_(
                            IdempotencyKeyRecord.job_run_id == run_id,
                            IdempotencyKeyRecord.attempt == attempt,
                        )
                    )
                )
                record = result.scalar_one_or_none()
                await session.commit()
                return record
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    async def _persist_success(
        self,
        run: JobRun,
        subscription: Subscription,
        payment_id: str,
    ) -> bool:
        """
        Atomically persist a successful payment.

        INSERT idempotency key ON CONFLICT DO NOTHING acts as the final
        duplicate gate: if this run_id/attempt pair already has a key, another
        worker beat us here and we return False without updating the run.
        """
        now = datetime.now(timezone.utc)

        async with self._factory()() as session:
            try:
                insert_result = await session.execute(
                    pg_insert(IdempotencyKeyRecord)
                    .values(
                        key=str(IdempotencyKey.for_run(run.id, run.attempt)),
                        job_run_id=run.id,
                        attempt=run.attempt,
                        bunq_payment_id=payment_id,
                        created_at=now,
                    )
                    .on_conflict_do_nothing(constraint="uq_idempotency_run_attempt")
                )

                if insert_result.rowcount == 0:
                    # Another worker already persisted a key for this run/attempt.
                    await session.rollback()
                    return False

                await session.execute(
                    update(JobRun)
                    .where(JobRun.id == run.id)
                    .values(
                        status=JobRunStatus.SUCCEEDED,
                        bunq_payment_id=payment_id,
                        finished_at=now,
                        lock_expires_at=None,
                    )
                )
                await session.execute(
                    update(ScheduledJob)
                    .where(ScheduledJob.id == run.job_id)
                    .values(status=JobStatus.DONE)
                )
                await session.execute(
                    update(Subscription)
                    .where(Subscription.id == subscription.id)
                    .values(consecutive_failures=0)
                )
                await session.commit()
                return True
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    async def _mark_dag_cancelled(self, run: JobRun) -> None:
        now = datetime.now(timezone.utc)
        async with self._factory()() as session:
            try:
                await session.execute(
                    update(JobRun)
                    .where(JobRun.id == run.id)
                    .values(
                        status=JobRunStatus.FAILED,
                        error_message="DAG dependency not satisfied",
                        finished_at=now,
                        lock_expires_at=None,
                    )
                )
                await session.execute(
                    update(ScheduledJob)
                    .where(ScheduledJob.id == run.job_id)
                    .values(status=JobStatus.CANCELLED)
                )
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    async def _handle_failure(
        self,
        run: JobRun,
        subscription: Subscription,
        error: str,
        is_retryable: bool,
    ) -> None:
        policy = RetryPolicy.from_dict(subscription.retry_policy or {})
        now = datetime.now(timezone.utc)

        if is_retryable and not policy.is_exhausted(run.attempt):
            delay = policy.next_delay_s(run.attempt)
            retry_after = now + timedelta(seconds=delay)
            next_attempt = run.attempt + 1

            async with self._factory()() as session:
                try:
                    await session.execute(
                        update(JobRun)
                        .where(JobRun.id == run.id)
                        .values(
                            status=JobRunStatus.FAILED,
                            error_message=error[:ERROR_MESSAGE_MAX_LEN],
                            finished_at=now,
                            lock_expires_at=None,
                        )
                    )
                    await session.execute(
                        pg_insert(JobRun)
                        .values(
                            id=uuid.uuid4(),
                            job_id=run.job_id,
                            attempt=next_attempt,
                            status=JobRunStatus.PENDING,
                            retry_after=retry_after,
                        )
                        .on_conflict_do_nothing(constraint="uq_run_per_attempt")
                    )
                    await session.commit()
                except Exception:
                    await session.rollback()
                    raise
                finally:
                    await session.close()

            RETRY_COUNTER.inc()
            EXECUTOR_JOBS_TOTAL.labels(outcome="failed").inc()
            logger.info(
                "executor.retry_scheduled",
                extra={
                    "next_attempt": next_attempt,
                    "delay_s": round(delay, 1),
                    "retry_after": retry_after.isoformat(),
                },
            )
        else:
            await self._move_to_dlq(run, subscription, error)

    async def _move_to_dlq(
        self,
        run: JobRun,
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
                    )
                )
                session.add(
                    DeadLetterEntry(
                        job_run_id=run.id,
                        job_id=run.job_id,
                        subscription_id=subscription.id,
                        failure_reason=truncated_reason,
                        payload_snapshot={
                            "payment_config": subscription.payment_config,
                            "attempt": run.attempt,
                            "worker_id": self._worker_id,
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
                        extra={
                            "subscription_id": str(subscription.id),
                            "consecutive_failures": refreshed.consecutive_failures,
                        },
                    )
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

        EXECUTOR_JOBS_TOTAL.labels(outcome="dead").inc()
        record_dlq_entry()
        logger.error(
            "executor.run_moved_to_dlq",
            extra={"job_run_id": str(run.id), "reason": truncated_reason[:200]},
        )


async def main() -> None:
    import signal
    from app.core.logging import configure_logging
    from app.infrastructure.db.session import init_db, close_db

    configure_logging()
    await init_db()

    worker = ExecutorWorker()
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGTERM, lambda: worker.stop())

    try:
        await worker.run()
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())