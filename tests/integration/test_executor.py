"""
tests/integration/test_executor.py — Executor integration tests (refactored).

Strategy
--------
All tests use a real PostgreSQL DB.  The executor receives the test session
factory via constructor injection (make_executor fixture), so its DB writes
are committed to the same schema that the assertion session reads.

Only BunqClient is mocked.

Isolation
---------
clean_db (autouse=True in conftest) TRUNCATEs all mutable tables before each
test.  Tests are therefore independent of each other regardless of ordering.

Replay tests
------------
Tests in TestIdempotency verify that running the executor twice against the
same job_run produces exactly one payment and leaves the run in SUCCEEDED
state — not FAILED or in a duplicated state.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.models import JobRunStatus, JobStatus
from app.infrastructure.db.models import (
    DeadLetterEntry,
    IdempotencyKeyRecord,
    JobRun,
    ScheduledJob,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def bunq_mock_ctx(payment_id: str = "bunq-payment-test-001"):
    """
    Return a (patch_ctx, mock_client) pair for BunqClient.

    Usage::

        ctx, mock_client = bunq_mock_ctx()
        with ctx:
            await worker._process_batch()
    """
    mock_client = AsyncMock()
    mock_client.create_payment = AsyncMock(
        return_value={"Response": [{"Id": {"id": payment_id}}]}
    )
    mock_cls = MagicMock()
    mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
    mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
    return patch("app.workers.executor.BunqClient", mock_cls), mock_client


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    async def test_successful_execution_marks_run_succeeded(
        self,
        pending_job_run: JobRun,
        scheduled_job: ScheduledJob,
        db_session: AsyncSession,
        make_executor,
    ) -> None:
        """Executor transitions a PENDING run to SUCCEEDED and writes a payment ID."""
        ctx, mock_client = bunq_mock_ctx("bunq-payment-001")
        worker = make_executor("test-worker-happy")

        with ctx:
            await worker._process_batch()

        # The worker committed its own session.  Our assertion session can now
        # see the updated state directly (no refresh needed — clean connection).
        result = await db_session.execute(
            select(JobRun).where(JobRun.id == pending_job_run.id)
        )
        refreshed_run = result.scalar_one()

        assert refreshed_run.status == JobRunStatus.SUCCEEDED
        assert refreshed_run.bunq_payment_id == "bunq-payment-001"
        assert refreshed_run.finished_at is not None

    async def test_successful_execution_marks_job_done(
        self,
        pending_job_run: JobRun,
        scheduled_job: ScheduledJob,
        db_session: AsyncSession,
        make_executor,
    ) -> None:
        ctx, _ = bunq_mock_ctx()
        worker = make_executor()

        with ctx:
            await worker._process_batch()

        result = await db_session.execute(
            select(ScheduledJob).where(ScheduledJob.id == scheduled_job.id)
        )
        refreshed_job = result.scalar_one()
        assert refreshed_job.status == JobStatus.DONE

    async def test_successful_execution_writes_idempotency_key(
        self,
        pending_job_run: JobRun,
        db_session: AsyncSession,
        make_executor,
    ) -> None:
        ctx, _ = bunq_mock_ctx("bunq-idem-001")
        worker = make_executor()

        with ctx:
            await worker._process_batch()

        result = await db_session.execute(
            select(IdempotencyKeyRecord).where(
                IdempotencyKeyRecord.job_run_id == pending_job_run.id
            )
        )
        key = result.scalar_one_or_none()
        assert key is not None
        assert key.bunq_payment_id == "bunq-idem-001"
        assert key.attempt == 1


# ---------------------------------------------------------------------------
# Idempotency / replay safety
# ---------------------------------------------------------------------------


class TestIdempotency:
    async def test_second_execution_of_same_run_is_noop(
        self,
        pending_job_run: JobRun,
        db_session: AsyncSession,
        make_executor,
    ) -> None:
        """
        Running the executor twice for the same PENDING run must produce exactly
        one idempotency key and one SUCCEEDED state — not two keys or a failure.

        This simulates a crash-and-replay scenario where the worker crashes
        after calling bunq but before persisting the result, then the run is
        retried.
        """
        ctx, mock_client = bunq_mock_ctx("bunq-idem-replay-001")
        worker = make_executor()

        # First pass: succeeds normally.
        with ctx:
            await worker._process_batch()

        # Manually reset the run back to PENDING to simulate a replay
        # (e.g. recovery worker re-queued it because the lock expired).
        await db_session.execute(
            __import__("sqlalchemy").update(JobRun)
            .where(JobRun.id == pending_job_run.id)
            .values(status=JobRunStatus.PENDING, lock_expires_at=None)
        )
        await db_session.commit()

        # Second pass: idempotency pre-check must detect the existing key
        # and recover the stored payment_id without calling bunq again.
        with ctx:
            await worker._process_batch()

        # Exactly one idempotency key.
        result = await db_session.execute(
            select(IdempotencyKeyRecord).where(
                IdempotencyKeyRecord.job_run_id == pending_job_run.id
            )
        )
        keys = result.scalars().all()
        assert len(keys) == 1, f"Expected 1 idempotency key, got {len(keys)}"

        # Run still SUCCEEDED.
        run_result = await db_session.execute(
            select(JobRun).where(JobRun.id == pending_job_run.id)
        )
        run = run_result.scalar_one()
        assert run.status == JobRunStatus.SUCCEEDED

        # bunq was called exactly ONCE (not on the replay).
        assert mock_client.create_payment.call_count == 1


# ---------------------------------------------------------------------------
# Failure handling
# ---------------------------------------------------------------------------


class TestFailureHandling:
    async def test_transient_failure_schedules_retry(
        self,
        pending_job_run: JobRun,
        db_session: AsyncSession,
        make_executor,
    ) -> None:
        from app.core.exceptions import BunqTransientError

        failing_client = AsyncMock()
        failing_client.create_payment = AsyncMock(
            side_effect=BunqTransientError("bunq 503")
        )
        mock_cls = MagicMock()
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=failing_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        worker = make_executor()
        with patch("app.workers.executor.BunqClient", mock_cls):
            await worker._process_batch()

        result = await db_session.execute(
            select(JobRun).where(JobRun.id == pending_job_run.id)
        )
        original_run = result.scalar_one()
        assert original_run.status == JobRunStatus.FAILED

        # A new PENDING run with attempt=2 should exist.
        retry_result = await db_session.execute(
            select(JobRun).where(
                JobRun.job_id == pending_job_run.job_id,
                JobRun.attempt == 2,
                JobRun.status == JobRunStatus.PENDING,
            )
        )
        retry_run = retry_result.scalar_one_or_none()
        assert retry_run is not None
        assert retry_run.retry_after is not None

    async def test_exhausted_retries_move_to_dlq(
        self,
        db_session: AsyncSession,
        subscription,
        scheduled_job,
        make_executor,
    ) -> None:
        from app.core.exceptions import BunqTransientError

        # Create a run that is already at the max attempt count.
        max_attempt_run = JobRun(
            id=uuid.uuid4(),
            job_id=scheduled_job.id,
            attempt=3,  # MAX_RETRY_ATTEMPTS=3 in integration_settings
            status=JobRunStatus.PENDING,
        )
        db_session.add(max_attempt_run)
        await db_session.flush()

        failing_client = AsyncMock()
        failing_client.create_payment = AsyncMock(
            side_effect=BunqTransientError("bunq 503")
        )
        mock_cls = MagicMock()
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=failing_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        worker = make_executor()
        with patch("app.workers.executor.BunqClient", mock_cls):
            await worker._process_batch()

        run_result = await db_session.execute(
            select(JobRun).where(JobRun.id == max_attempt_run.id)
        )
        run = run_result.scalar_one()
        assert run.status == JobRunStatus.DEAD

        dlq_result = await db_session.execute(
            select(DeadLetterEntry).where(
                DeadLetterEntry.job_run_id == max_attempt_run.id
            )
        )
        dlq_entry = dlq_result.scalar_one_or_none()
        assert dlq_entry is not None


# ---------------------------------------------------------------------------
# Concurrent worker isolation
# ---------------------------------------------------------------------------


class TestConcurrency:
    async def test_two_workers_do_not_double_process_same_run(
        self,
        pending_job_run: JobRun,
        db_session: AsyncSession,
        make_executor,
    ) -> None:
        """
        Two workers racing for the same PENDING run must result in exactly
        one SUCCEEDED state — not two idempotency keys, not two bunq calls.

        The FOR UPDATE SKIP LOCKED claim ensures only one worker claims the row.
        """
        import asyncio

        ctx, mock_client = bunq_mock_ctx("bunq-concurrent-001")

        worker_a = make_executor("worker-a")
        worker_b = make_executor("worker-b")

        with ctx:
            # Run both workers concurrently.
            await asyncio.gather(
                worker_a._process_batch(),
                worker_b._process_batch(),
            )

        result = await db_session.execute(
            select(IdempotencyKeyRecord).where(
                IdempotencyKeyRecord.job_run_id == pending_job_run.id
            )
        )
        keys = result.scalars().all()
        assert len(keys) == 1, f"Expected 1 idempotency key, got {len(keys)}"
        assert mock_client.create_payment.call_count == 1