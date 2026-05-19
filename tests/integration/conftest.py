"""
tests/integration/conftest.py — Production-grade integration test fixtures.

Architecture
============

Engine lifecycle
----------------
ONE async engine is created for the entire pytest SESSION (not per-module,
not per-function).  It is disposed exactly once in session teardown.  This
matches the lifecycle of a production process and eliminates:

  - "Event loop is closed" errors from disposing an engine whose pool tasks
    are still pending in a live loop.
  - "Dead connection pool" errors from tests that created and disposed their
    own engine, then tried to use sessions against the disposed pool.

Session lifecycle
-----------------
Each test gets a FRESH, INDEPENDENT session from the shared session factory.
Sessions are function-scoped (short-lived unit of work) and never shared
across tests.  We do NOT use nested transactions / SAVEPOINT rollback here
because:

  1. SAVEPOINT rollback does not work cleanly with asyncpg's connection-level
     transaction state when multiple sessions are involved (e.g. worker
     sessions inside the test and the test-assertion session are different
     connections — the SAVEPOINT on one connection is invisible to the other).
  2. Workers use their own sessions (different connections); rolling back only
     the test session leaves worker-written data in the DB.

Instead, every test gets full table TRUNCATION before it runs.  This is
deterministic, loop-safe, and works regardless of how many sessions or
connections a test's code paths open.

Dependency injection
--------------------
configure_session_factory() is called in the session-scoped
``patch_global_session_factory`` fixture.  Workers (ExecutorWorker) receive
the test factory via constructor injection, not via global state.  HTTP routes
get the factory via FastAPI's dependency override mechanism.

HTTP client
-----------
The ``http_client`` fixture wraps the FastAPI app in LifespanManager (from
asgi-lifespan) so that startup/shutdown hooks run correctly inside the test.
The app's lifespan normally calls init_db() / close_db(), but because the
test session already owns the engine, the app's lifespan is configured NOT to
create its own engine (via dependency override + settings override).

Cleanup
-------
The ``clean_db`` autouse fixture TRUNCATEs all mutable tables before each
test.  RESTART IDENTITY CASCADE resets sequences and cascades to FK-dependent
tables in a single round-trip.  This is placed as autouse=True so it applies
to every integration test without requiring opt-in.
"""

from __future__ import annotations

import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import Settings, get_settings
from app.infrastructure.db.models import Base, JobRun, ScheduledJob, Subscription
from app.infrastructure.db.session import (
    configure_session_factory,
    restore_session_factory,
)
from app.main import create_app

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TEST_POSTGRES_DSN: str = os.environ.get(
    "TEST_POSTGRES_DSN",
    "postgresql+asyncpg://postgres:postgres@localhost:5432/test_scheduler",
)
TEST_REDIS_URL: str = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/15")

# Tables to truncate between tests.  Order matters for FK constraints: child
# tables first.  RESTART IDENTITY CASCADE handles sequences and cascades.
_MUTABLE_TABLES: tuple[str, ...] = (
    "dead_letter_queue",
    "idempotency_keys",
    "job_runs",
    "scheduled_jobs",
    "subscriptions",
)

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def integration_settings() -> Settings:
    """Session-scoped settings pointing at the test database."""
    return Settings(
        POSTGRES_DSN=TEST_POSTGRES_DSN,
        REDIS_URL=TEST_REDIS_URL,
        EXECUTOR_BATCH_SIZE=5,
        EXECUTOR_LOCK_TIMEOUT_S=10,
        MAX_RETRY_ATTEMPTS=3,
        LOG_FORMAT="text",
    )


# ---------------------------------------------------------------------------
# Engine — ONE per session
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="session")
async def async_engine(integration_settings: Settings) -> AsyncGenerator[AsyncEngine, None]:
    """
    Create exactly ONE async engine for the entire test session.

    Creates the schema once at session start and disposes the engine once at
    session end.  Never disposes mid-session, which would invalidate all
    connections checked out by running tests.

    ``scope="session"`` + pytest-asyncio >= 0.23 requires that the event loop
    also lives for the session.  Add ``asyncio_mode = "auto"`` and
    ``asyncio_default_fixture_loop_scope = "session"`` to pyproject.toml (see
    configuration note below).
    """
    engine = create_async_engine(
        integration_settings.POSTGRES_DSN,
        pool_pre_ping=True,
        echo=False,
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    # Single disposal point — runs once when the session ends.
    await engine.dispose()


# ---------------------------------------------------------------------------
# Session factory — shared, session-scoped
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="session")
async def session_factory(
    async_engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    """
    Shared async_sessionmaker bound to the test engine.

    Session-scoped because the factory is stateless — it is safe to share
    across tests.  Individual sessions (opened from this factory) are
    function-scoped and never shared.
    """
    return async_sessionmaker(
        bind=async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )


# ---------------------------------------------------------------------------
# Global session factory override — injects test factory into all DB paths
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="session", autouse=True)
async def patch_global_session_factory(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[None, None]:
    """
    Redirect the module-level session factory to the test factory.

    Workers that call get_session_factory() will receive the test factory and
    therefore write into the same schema that the test-assertion session reads.

    Restored to the canonical factory (None during tests, production engine
    in production) on session teardown.
    """
    configure_session_factory(session_factory)
    yield
    restore_session_factory()


# ---------------------------------------------------------------------------
# Per-test isolation — TRUNCATE before every test
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(autouse=True)
async def clean_db(async_engine: AsyncEngine) -> None:
    """
    TRUNCATE all mutable tables before each test.

    Why TRUNCATE instead of rollback:
      - Tests that open multiple sessions (e.g. a worker session + an
        assertion session) cannot be isolated by rolling back a single outer
        transaction, because each session has its own connection.
      - TRUNCATE ... RESTART IDENTITY CASCADE is a single DDL statement that
        clears rows, resets sequences, and cascades to FK children atomically.
      - Deterministic: the DB starts from a known-empty state regardless of
        what the previous test did or how many sessions it opened.

    Why not expire_all() / session.expunge_all():
      - These are session-level operations that don't affect other sessions or
        connections.  Worker writes from a different connection are invisible
        to them.
    """
    table_list = ", ".join(_MUTABLE_TABLES)
    async with async_engine.begin() as conn:
        await conn.execute(
            text(f"TRUNCATE {table_list} RESTART IDENTITY CASCADE")
        )


# ---------------------------------------------------------------------------
# Per-test session — function-scoped, short-lived
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def db_session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession, None]:
    """
    Provide a per-test AsyncSession.

    Opens a fresh session, yields it, then closes it.  Each test gets its own
    session object; sessions are never shared across tests.  Commits are
    intentionally NOT wrapped in a rollback: after clean_db runs, the DB is
    already empty, so any data inserted by the test is harmlessly left for the
    next clean_db to truncate.

    This means tests are free to commit intermediate state (which workers
    also do), and the assertion session can see committed worker state.
    """
    async with session_factory() as session:
        yield session
        # Close without committing so that any un-committed test setup is
        # not accidentally persisted.  Test fixtures that want committed
        # state should call ``await db_session.flush()`` themselves.
        await session.close()


# ---------------------------------------------------------------------------
# HTTP client — lifespan-aware
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def http_client(
    session_factory: async_sessionmaker[AsyncSession],
    integration_settings: Settings,
) -> AsyncGenerator[AsyncClient, None]:
    """
    Lifespan-aware AsyncClient for integration-testing HTTP routes.

    Uses asgi-lifespan.LifespanManager so that FastAPI's startup/shutdown
    hooks run correctly — including any background task teardown registered
    in the lifespan.

    The app's own init_db() is bypassed via a FastAPI dependency override:
    the ``get_db`` dependency is replaced with one that opens sessions from
    the test factory, so HTTP route DB writes are visible to the assertion
    session in the same test.

    Settings are overridden so that the app does not try to create its own
    engine pointing at a different DSN.
    """
    # Override get_settings so the app uses the test DSN.
    get_settings.cache_clear()
    os.environ["POSTGRES_DSN"] = integration_settings.POSTGRES_DSN
    os.environ["REDIS_URL"] = integration_settings.REDIS_URL

    app = create_app()

    # Override the FastAPI DB dependency so routes use the test factory.
    from app.infrastructure.db.session import get_db

    async def _test_get_db() -> AsyncGenerator[AsyncSession, None]:
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _test_get_db

    async with LifespanManager(app) as manager:
        async with AsyncClient(
            transport=ASGITransport(app=manager.app),
            base_url="http://testserver",
        ) as client:
            yield client

    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Domain fixtures — function-scoped, depend on db_session
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def subscription(
    db_session: AsyncSession,
    valid_payment_config: dict,
    valid_retry_policy: dict,
) -> Subscription:
    """A persisted ACTIVE subscription ready for scheduling."""
    sub = Subscription(
        id=uuid.uuid4(),
        name="Test Subscription",
        cron_expression="0 9 * * 1",
        timezone="Europe/Amsterdam",
        payment_config=valid_payment_config,
        retry_policy=valid_retry_policy,
        status="ACTIVE",
        next_fire_at=datetime.now(timezone.utc),
        consecutive_failures=0,
    )
    db_session.add(sub)
    await db_session.commit()
    return sub


@pytest_asyncio.fixture
async def scheduled_job(
    db_session: AsyncSession,
    subscription: Subscription,
) -> ScheduledJob:
    """A persisted READY ScheduledJob with no DAG dependencies."""
    job = ScheduledJob(
        id=uuid.uuid4(),
        subscription_id=subscription.id,
        scheduled_for=datetime.now(timezone.utc),
        fingerprint=f"test-fingerprint-{uuid.uuid4().hex}",
        depends_on=[],
        status="READY",
        created_by_epoch=1,
    )
    db_session.add(job)
    await db_session.commit()
    return job


@pytest_asyncio.fixture
async def pending_job_run(
    db_session: AsyncSession,
    scheduled_job: ScheduledJob,
) -> JobRun:
    """A persisted PENDING JobRun ready to be claimed by the executor."""
    run = JobRun(
        id=uuid.uuid4(),
        job_id=scheduled_job.id,
        attempt=1,
        status="PENDING",
    )
    db_session.add(run)
    await db_session.commit()
    return run


# ---------------------------------------------------------------------------
# Replay test helpers
# ---------------------------------------------------------------------------


async def reset_consumer_offsets(engine: AsyncEngine) -> None:
    """
    Reset ONLY consumer offset state for replay tests.

    Does NOT truncate rollup tables, so replay tests can verify that
    reprocessing the same events produces exactly the same aggregated result
    (idempotency / exactly-once semantics).

    Replay test fixture example::

        @pytest_asyncio.fixture
        async def replay_state(async_engine):
            # Run the initial ingestion...
            yield
            # Reset offsets so the same events can be replayed.
            await reset_consumer_offsets(async_engine)
    """
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE consumer_offsets RESTART IDENTITY CASCADE"))


@pytest.fixture
def make_executor(session_factory: async_sessionmaker[AsyncSession]):
    """
    Factory fixture that produces ExecutorWorker instances bound to the test
    session factory.

    Usage::

        async def test_something(make_executor):
            worker = make_executor(worker_id="test-worker-1")
            await worker._process_batch()
    """
    from app.workers.executor import ExecutorWorker

    def _make(worker_id: str = "test-worker-1") -> ExecutorWorker:
        worker = ExecutorWorker.__new__(ExecutorWorker)
        worker._stop = False
        worker._worker_id = worker_id
        # Inject the test factory — worker never touches the global factory.
        worker._injected_factory = session_factory
        # No injected BunqClient — tests patch app.workers.executor.BunqClient directly.
        worker._bunq_client = None
        return worker

    return _make