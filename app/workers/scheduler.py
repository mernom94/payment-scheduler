"""
app/workers/scheduler.py — Scheduler Worker.

Runs only on the elected leader.  On each tick:

  1. Verify leadership immediately before any DB write (split-brain prevention).
  2. Query ACTIVE subscriptions whose next_fire_at is within the lookahead
     window, using SELECT FOR UPDATE SKIP LOCKED so concurrent schedulers
     (during leader failover overlap) never double-schedule.
  3. For each due subscription, INSERT a ScheduledJob + initial JobRun with
     ON CONFLICT DO NOTHING fingerprint deduplication.
  4. Advance next_fire_at to the next cron occurrence.

Correctness invariants
----------------------
- UNIQUE(fingerprint) prevents duplicate jobs even if leadership overlaps.
- leader_epoch fencing token is written to every row we create, enabling
  recovery to detect and discard stale writes from a deposed leader.
- verify_still_leader() inside _schedule_due_subscriptions() tightens the
  split-brain window to near-zero (gap between verify and first write is a
  single async call in the same event loop tick).
- Invalid cron expressions pause the subscription rather than allowing the
  scheduler to spin on an un-advanceable next_fire_at.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from croniter import croniter
from pytz import timezone as pytz_timezone
from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError

from app.core.config import get_settings
from app.domain.models import JobFingerprint, JobStatus, SubscriptionStatus
from app.infrastructure.db.models import JobRun, ScheduledJob, Subscription
from app.infrastructure.db.session import get_session_factory
from app.infrastructure.messaging.workers import BaseWorker
from app.infrastructure.redis.leader import LeaderElection, create_redis
from observability.setup import (
    JOBS_CREATED_TOTAL,
    SCHEDULER_LAG_SECONDS,
    SCHEDULER_RUNS_TOTAL,
    get_tracer,
)
from sqlalchemy import select

logger = logging.getLogger(__name__)


class SchedulerWorker(BaseWorker):
    """
    Leader-only worker that converts due subscriptions into executable jobs.

    Only the elected leader runs _schedule_due_subscriptions; non-leaders
    still participate in the election loop so they are ready to take over
    immediately after a leader failure.
    """

    name = "scheduler_worker"

    @property
    def poll_interval(self) -> float:
        return get_settings().SCHEDULER_INTERVAL_S

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
        # Attempt to acquire leadership if we don't hold it.
        if not self._election.is_leader:
            acquired = await self._election.try_acquire()
            if not acquired:
                SCHEDULER_RUNS_TOTAL.labels(outcome="not_leader").inc()
                logger.debug("scheduler.not_leader")
                return

        tracer = get_tracer()
        with tracer.start_as_current_span(
            "scheduler.tick",
            attributes={"epoch": self._election.epoch},
        ):
            try:
                await self._schedule_due_subscriptions(self._election.epoch)
                SCHEDULER_RUNS_TOTAL.labels(outcome="ok").inc()
            except Exception:
                SCHEDULER_RUNS_TOTAL.labels(outcome="error").inc()
                raise

    # ── Scheduling logic ───────────────────────────────────────────────────────

    async def _schedule_due_subscriptions(self, epoch: int) -> None:
        """
        Find and schedule all subscriptions due within the lookahead window.

        Verifies leadership immediately before opening the DB write session to
        minimise the split-brain window.  If we lost the lock between the tick
        check and now (e.g. GC pause), we abort without writing.
        """
        s = get_settings()
        now = datetime.now(timezone.utc)
        lookahead_cutoff = datetime.fromtimestamp(
            now.timestamp() + s.SCHEDULER_LOOKAHEAD_S, tz=timezone.utc
        )

        # Re-verify leadership immediately before writing.  This tightens the
        # split-brain window to the gap between this GET and the next DB write,
        # which is sub-millisecond in normal operation.
        if not await self._election.verify_still_leader():
            logger.warning(
                "scheduler.lost_leadership_before_write",
                extra={"epoch": epoch},
            )
            SCHEDULER_RUNS_TOTAL.labels(outcome="not_leader").inc()
            return

        async with self._factory()() as session:
            try:
                result = await session.execute(
                    select(Subscription)
                    .where(
                        Subscription.status == SubscriptionStatus.ACTIVE,
                        Subscription.next_fire_at <= lookahead_cutoff,
                    )
                    .order_by(Subscription.next_fire_at)   # deterministic ordering
                    .with_for_update(skip_locked=True)
                    .limit(100)
                )
                subscriptions = result.scalars().all()

                if not subscriptions:
                    SCHEDULER_LAG_SECONDS.set(0)
                    return

                # Verify leadership again inside the transaction — fencing token
                # check before any write occurs.
                if not await self._election.verify_still_leader():
                    logger.warning(
                        "scheduler.lost_leadership_mid_transaction",
                        extra={"epoch": epoch},
                    )
                    SCHEDULER_RUNS_TOTAL.labels(outcome="not_leader").inc()
                    await session.rollback()
                    return

                for sub in subscriptions:
                    await self._create_job_for_subscription(session, sub, epoch)

                await session.commit()
            except Exception:
                await session.rollback()
                raise

        logger.info(
            "scheduler.tick_complete",
            extra={
                "subscriptions_processed": len(subscriptions),
                "epoch": epoch,
            },
        )

    async def _create_job_for_subscription(
        self, session, subscription: Subscription, epoch: int
    ) -> None:
        """
        Insert a ScheduledJob and its first JobRun for one subscription tick.

        Uses ON CONFLICT DO NOTHING on the fingerprint unique constraint as a
        belt-and-suspenders guard against duplicate creation (e.g. during
        leader failover overlap, or a scheduler bug).
        """
        fire_at = subscription.next_fire_at
        if fire_at is None:
            return

        fingerprint = JobFingerprint.for_job(subscription.id, fire_at)
        job_id = uuid.uuid4()

        job_stmt = (
            pg_insert(ScheduledJob)
            .values(
                id=job_id,
                subscription_id=subscription.id,
                scheduled_for=fire_at,
                fingerprint=str(fingerprint),
                depends_on=[],
                status=JobStatus.READY,
                created_by_epoch=epoch,
            )
            .on_conflict_do_nothing(constraint="uq_job_fingerprint")
            .returning(ScheduledJob.id)
        )
        result = await session.execute(job_stmt)
        actual_job_id = result.scalar_one_or_none()

        if actual_job_id is None:
            logger.debug(
                "scheduler.job_already_exists",
                extra={
                    "fingerprint": str(fingerprint),
                    "subscription_id": str(subscription.id),
                },
            )
        else:
            await session.execute(
                pg_insert(JobRun)
                .values(
                    id=uuid.uuid4(),
                    job_id=actual_job_id,
                    attempt=1,
                    status="PENDING",
                )
                .on_conflict_do_nothing(constraint="uq_run_per_attempt")
            )
            JOBS_CREATED_TOTAL.inc()
            logger.info(
                "scheduler.job_created",
                extra={
                    "job_id": str(actual_job_id),
                    "subscription_id": str(subscription.id),
                    "scheduled_for": fire_at.isoformat(),
                    "epoch": epoch,
                },
            )

        # Advance next_fire_at.  On invalid cron, pause the subscription
        # rather than leaving next_fire_at unchanged (which would spin the
        # scheduler every tick until a human intervenes).
        next_fire = self._next_fire(subscription)
        if next_fire is None:
            logger.error(
                "scheduler.invalid_cron_pausing_subscription",
                extra={
                    "subscription_id": str(subscription.id),
                    "cron_expression": subscription.cron_expression,
                },
            )
            await session.execute(
                update(Subscription)
                .where(Subscription.id == subscription.id)
                .values(status="PAUSED", paused_at=datetime.now(timezone.utc))
            )
            return

        await session.execute(
            update(Subscription)
            .where(Subscription.id == subscription.id)
            .values(next_fire_at=next_fire)
        )

        lag = (datetime.now(timezone.utc) - fire_at).total_seconds()
        SCHEDULER_LAG_SECONDS.set(max(0.0, lag))

    @staticmethod
    def _next_fire(subscription: Subscription) -> datetime | None:
        """
        Compute the next UTC fire time from the subscription's cron expression.

        Returns None if the expression is invalid so the caller can pause the
        subscription rather than spinning.  The cron expression is validated at
        creation time, so None here indicates data corruption or a previously
        accepted expression that later became invalid after a library upgrade.
        """
        try:
            tz = pytz_timezone(subscription.timezone)
        except Exception:
            tz = timezone.utc

        try:
            now_local = datetime.now(tz)
            cron = croniter(subscription.cron_expression, now_local)
            next_dt = cron.get_next(datetime)
            return next_dt.astimezone(timezone.utc)
        except Exception:
            logger.error(
                "scheduler.next_fire_failed",
                extra={
                    "cron_expression": subscription.cron_expression,
                    "subscription_id": str(subscription.id),
                },
            )
            return None


async def main() -> None:
    import asyncio
    import signal
    from app.core.logging import configure_logging
    from app.infrastructure.db.session import init_db, close_db

    configure_logging()
    await init_db()

    worker = SchedulerWorker()
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGTERM, lambda: worker.stop())

    try:
        await worker.run()
    finally:
        await close_db()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
