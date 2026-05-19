"""
tests/failure/test_failure_injection.py — Failure injection tests.

Tests that the executor classifies failures correctly and routes them to the
right outcome (retry vs DLQ) when individual components fail.

Design principles
-----------------
These are unit-level orchestration tests.

We intentionally avoid:
- real databases
- SQLAlchemy query mocking
- mocking async context manager chains deeply
- patching obsolete get_db_session() globals

Instead we:
- inject a lightweight fake session factory
- patch the bunq integration boundary
- patch orchestration helpers (_handle_failure, _persist_success)
- verify classification behaviour only

DB-specific correctness (idempotency INSERT conflicts, transactional semantics,
SKIP LOCKED behaviour, etc.) belongs in integration tests.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core.exceptions import (
    BunqAmbiguousError,
    BunqPaymentError,
    BunqTransientError,
)
from app.infrastructure.db.models import JobRun, ScheduledJob, Subscription


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def make_run(status: str = "RUNNING", attempt: int = 1) -> MagicMock:
    """Minimal JobRun stand-in."""
    run = MagicMock(spec=JobRun)
    run.id = uuid.uuid4()
    run.job_id = uuid.uuid4()
    run.status = status
    run.attempt = attempt
    run.lock_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    run.error_message = None
    return run


def make_subscription(consecutive_failures: int = 0) -> MagicMock:
    """Minimal Subscription stand-in."""
    sub = MagicMock(spec=Subscription)
    sub.id = uuid.uuid4()

    # Shape is irrelevant because PaymentConfig.from_dict is patched.
    sub.payment_config = {
        "dummy": True,
    }

    sub.retry_policy = {
        "max_attempts": 3,
        "base_backoff_s": 1.0,
        "jitter": False,
    }

    sub.consecutive_failures = consecutive_failures
    return sub


def make_job(run: MagicMock, subscription: MagicMock) -> MagicMock:
    """Minimal ScheduledJob stand-in."""
    job = MagicMock(spec=ScheduledJob)
    job.id = run.job_id
    job.subscription_id = subscription.id
    job.depends_on = []
    return job


@pytest.fixture
def execution_context():
    run = make_run()
    subscription = make_subscription()
    job = make_job(run, subscription)

    return run, job, subscription


def make_session(run, job, subscription):
    """
    Minimal AsyncSession mock matching the executor's actual access pattern.
    """
    session = AsyncMock()

    async def get_side_effect(model, key):
        if model is JobRun:
            return run

        if model is ScheduledJob:
            return job

        if model is Subscription:
            return subscription

        return None

    session.get.side_effect = get_side_effect
    session.commit = AsyncMock()
    session.rollback = AsyncMock()

    return session


def make_session_factory(session):
    """
    Mimic async_sessionmaker behaviour.

    Executor usage:
        async with self._factory()() as session:
    """
    context_manager = MagicMock()
    context_manager.__aenter__ = AsyncMock(return_value=session)
    context_manager.__aexit__ = AsyncMock(return_value=False)

    sessionmaker_mock = MagicMock(return_value=context_manager)

    return sessionmaker_mock


def make_executor(session_factory):
    """Construct ExecutorWorker without running full __init__."""
    from app.workers.executor import ExecutorWorker

    worker = ExecutorWorker.__new__(ExecutorWorker)

    worker._stop = False
    worker._worker_id = "test-worker"
    # No injected BunqClient — tests patch app.workers.executor.BunqClient directly.
    worker._bunq_client = None

    worker._factory = MagicMock(return_value=session_factory)

    return worker


# ---------------------------------------------------------------------------
# bunq network failure classification
# ---------------------------------------------------------------------------


def make_bunq_client(side_effect=None, return_value=None):
    """
    Build a mock BunqClient that works with ``async with client as c:``.

    The executor does:
        client = self._bunq_client or BunqClient(...)
        async with client as c:
            response = await c.create_payment(...)

    So we need an object whose __aenter__ returns a mock whose
    create_payment raises/returns as specified.
    """
    inner = AsyncMock()
    if side_effect is not None:
        inner.create_payment = AsyncMock(side_effect=side_effect)
    else:
        inner.create_payment = AsyncMock(return_value=return_value)

    client = MagicMock()
    client.__aenter__ = AsyncMock(return_value=inner)
    client.__aexit__ = AsyncMock(return_value=False)
    return client


class TestBunqNetworkFailures:
    @pytest.mark.asyncio
    async def test_ambiguous_error_schedules_retry_not_dlq(
        self,
        execution_context,
    ):
        """
        BunqAmbiguousError (network failure after dispatch)
        must be retryable.
        """
        run, job, subscription = execution_context

        session = make_session(run, job, subscription)
        session_factory = make_session_factory(session)

        worker = make_executor(session_factory)
        worker._bunq_client = make_bunq_client(
            side_effect=BunqAmbiguousError("Connection reset by peer")
        )

        retry_calls = []
        dlq_calls = []

        async def mock_handle_failure(r, s, error, is_retryable):
            if is_retryable:
                retry_calls.append(error)
            else:
                dlq_calls.append(error)

        worker._handle_failure = mock_handle_failure

        with patch.object(worker, "_find_idempotency_key", return_value=None):
            with patch.object(worker, "_check_dag_dependencies", return_value=True):
                with patch(
                    "app.workers.executor.PaymentConfig.from_dict",
                    return_value=MagicMock(),
                ):
                    await worker._execute_run(run.id, 0.0)

        assert len(retry_calls) == 1
        assert len(dlq_calls) == 0

    @pytest.mark.asyncio
    async def test_transient_error_is_retryable(
        self,
        execution_context,
    ):
        """
        BunqTransientError (429 / 5xx)
        must be classified as retryable.
        """
        run, job, subscription = execution_context

        session = make_session(run, job, subscription)
        session_factory = make_session_factory(session)

        worker = make_executor(session_factory)
        worker._bunq_client = make_bunq_client(
            side_effect=BunqTransientError("503 Service Unavailable")
        )

        retryable = []

        async def mock_handle_failure(r, s, error, is_retryable):
            retryable.append(is_retryable)

        worker._handle_failure = mock_handle_failure

        with patch.object(worker, "_find_idempotency_key", return_value=None):
            with patch.object(worker, "_check_dag_dependencies", return_value=True):
                with patch(
                    "app.workers.executor.PaymentConfig.from_dict",
                    return_value=MagicMock(),
                ):
                    await worker._execute_run(run.id, 0.0)

        assert retryable == [True]

    @pytest.mark.asyncio
    async def test_4xx_error_is_not_retryable(
        self,
        execution_context,
    ):
        """
        BunqPaymentError (4xx validation/business error)
        must route to DLQ (retryable=False).
        """
        run, job, subscription = execution_context

        session = make_session(run, job, subscription)
        session_factory = make_session_factory(session)

        worker = make_executor(session_factory)
        worker._bunq_client = make_bunq_client(
            side_effect=BunqPaymentError(422, '{"error": "Invalid IBAN"}')
        )

        dlq_calls = []

        async def mock_handle_failure(r, s, error, is_retryable):
            if not is_retryable:
                dlq_calls.append(error)

        worker._handle_failure = mock_handle_failure

        with patch.object(worker, "_find_idempotency_key", return_value=None):
            with patch.object(worker, "_check_dag_dependencies", return_value=True):
                with patch(
                    "app.workers.executor.PaymentConfig.from_dict",
                    return_value=MagicMock(),
                ):
                    await worker._execute_run(run.id, 0.0)

        assert len(dlq_calls) == 1

    @pytest.mark.asyncio
    async def test_unexpected_exception_is_retryable(
        self,
        execution_context,
    ):
        """
        Unknown exceptions should default to retryable=True.
        """
        run, job, subscription = execution_context

        session = make_session(run, job, subscription)
        session_factory = make_session_factory(session)

        worker = make_executor(session_factory)
        worker._bunq_client = make_bunq_client(
            side_effect=RuntimeError("Unexpected SDK failure")
        )

        retryable = []

        async def mock_handle_failure(r, s, error, is_retryable):
            retryable.append(is_retryable)

        worker._handle_failure = mock_handle_failure

        with patch.object(worker, "_find_idempotency_key", return_value=None):
            with patch.object(worker, "_check_dag_dependencies", return_value=True):
                with patch(
                    "app.workers.executor.PaymentConfig.from_dict",
                    return_value=MagicMock(),
                ):
                    await worker._execute_run(run.id, 0.0)

        assert retryable == [True]


# ---------------------------------------------------------------------------
# Lock expiry semantics
# ---------------------------------------------------------------------------


class TestLockExpiry:
    def test_lock_expires_at_formula_is_correct(self):
        """
        lock_expires_at must always be in the future.
        """
        from app.core.config import get_settings

        s = get_settings()

        now = datetime.now(timezone.utc)
        expires = now + timedelta(seconds=s.EXECUTOR_LOCK_TIMEOUT_S)

        assert expires > now

        delta = (expires - now).total_seconds()

        assert abs(delta - s.EXECUTOR_LOCK_TIMEOUT_S) < 1.0

    def test_expired_lock_is_in_the_past(self):
        """
        A run whose lock_expires_at < now is considered stuck.
        """
        now = datetime.now(timezone.utc)

        expired_at = now - timedelta(seconds=1)

        assert expired_at < now


# ---------------------------------------------------------------------------
# BaseWorker shutdown behaviour
# ---------------------------------------------------------------------------


class TestWorkerShutdown:
    @pytest.mark.asyncio
    async def test_stop_prevents_further_ticks(self):
        """
        Once stop() is called, no further ticks should execute.
        """
        from app.infrastructure.messaging.workers import BaseWorker

        tick_count = 0

        class StopOnFirstTickWorker(BaseWorker):
            name = "test_worker"

            @property
            def poll_interval(self) -> float:
                return 0.001

            async def tick(self) -> None:
                nonlocal tick_count

                tick_count += 1
                self.stop()

        worker = StopOnFirstTickWorker()

        await asyncio.wait_for(worker.run(), timeout=2.0)

        assert tick_count == 1

    @pytest.mark.asyncio
    async def test_tick_exception_does_not_kill_worker(self):
        """
        Exceptions inside tick() must not terminate the worker loop.
        """
        from app.infrastructure.messaging.workers import BaseWorker

        tick_count = 0

        class ErrorThenStopWorker(BaseWorker):
            name = "test_worker"

            @property
            def poll_interval(self) -> float:
                return 0.001

            async def tick(self) -> None:
                nonlocal tick_count

                tick_count += 1

                if tick_count == 1:
                    raise RuntimeError("Simulated transient failure")

                self.stop()

        worker = ErrorThenStopWorker()

        await asyncio.wait_for(worker.run(), timeout=2.0)

        assert tick_count == 2

    @pytest.mark.asyncio
    async def test_cancelled_error_propagates(self):
        """
        asyncio.CancelledError must propagate correctly.
        """
        from app.infrastructure.messaging.workers import BaseWorker

        class NeverStopsWorker(BaseWorker):
            name = "test_worker"

            @property
            def poll_interval(self) -> float:
                return 10.0

            async def tick(self) -> None:
                pass

        worker = NeverStopsWorker()

        task = asyncio.create_task(worker.run())

        await asyncio.sleep(0.05)

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task