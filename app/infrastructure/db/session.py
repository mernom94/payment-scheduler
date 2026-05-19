"""
app/infrastructure/db/session.py — Async database engine and session factory.

Architecture
------------
This module owns exactly ONE async engine for the lifetime of the process and
exposes an injectable session-factory abstraction so that tests can substitute
a test-controlled factory without touching global state.

Key invariants:
  1.  The engine is created ONCE in init_db() and disposed ONCE in close_db().
  2.  Workers and HTTP routes never create engines themselves; they call
      get_session_factory() to obtain the current factory.
  3.  Tests call configure_session_factory() to inject a test factory before
      any DB code runs, then restore the original in teardown.
  4.  pool_pre_ping=True detects stale connections before handing them to a
      session, eliminating "connection already closed" errors after idle periods.
  5.  expire_on_commit=False keeps ORM objects usable after a commit without
      hitting the DB again — critical for async code that may not have an open
      session at the moment of attribute access.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Callable, Optional

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import get_settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level singletons — None until init_db() is called.
# ---------------------------------------------------------------------------

_engine: Optional[AsyncEngine] = None

# The *canonical* factory created from _engine.
_canonical_factory: Optional[async_sessionmaker[AsyncSession]] = None

# The *active* factory — may be replaced by configure_session_factory() in tests.
_active_factory: Optional[async_sessionmaker[AsyncSession]] = None


# ---------------------------------------------------------------------------
# Lifecycle — called once at process startup / shutdown
# ---------------------------------------------------------------------------


async def init_db() -> None:
    """
    Initialise the database engine and canonical session factory.

    Must be called exactly once at application startup (app/main.py lifespan).
    After this returns, get_session_factory() returns a working factory.

    Safe to call multiple times in tests (idempotent — re-initialises if the
    engine has been disposed).
    """
    global _engine, _canonical_factory, _active_factory

    s = get_settings()

    _engine = create_async_engine(
        s.POSTGRES_DSN,
        pool_size=s.DB_POOL_SIZE,
        max_overflow=s.DB_MAX_OVERFLOW,
        # Detect stale connections before handing them to a session.
        # Adds one lightweight SELECT 1 per idle-connection checkout.
        pool_pre_ping=True,
        echo=s.DEBUG,
        execution_options={"isolation_level": "READ COMMITTED"},  # default isolation level for asyncpg, but set explicitly for clarity
    )

    _canonical_factory = async_sessionmaker(
        bind=_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )

    # The active factory defaults to the canonical one; tests may override.
    _active_factory = _canonical_factory

    db_host = s.POSTGRES_DSN.split("@")[-1] if "@" in s.POSTGRES_DSN else s.POSTGRES_DSN
    logger.info("db.engine_initialised", extra={"db_host": db_host})


async def close_db() -> None:
    """
    Dispose the engine connection pool.

    Called ONCE at application shutdown.  Safe to call if init_db() was never
    called (no-op).  Resets the active factory to None so any subsequent DB
    access fails immediately rather than using a dead pool.
    """
    global _engine, _canonical_factory, _active_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _canonical_factory = None
        _active_factory = None
        logger.info("db.engine_disposed")


# ---------------------------------------------------------------------------
# Injectable session factory — the DI boundary
# ---------------------------------------------------------------------------


def configure_session_factory(
    factory: async_sessionmaker[AsyncSession],
) -> None:
    """
    Replace the active session factory.

    Called by integration test fixtures to inject a test-scoped factory that
    shares the test engine (and therefore the test transaction / isolation).

    The original factory is NOT automatically restored — callers are responsible
    for restoring it in teardown (or use the ``override_session_factory``
    context manager below).

    Example (pytest fixture)::

        @pytest_asyncio.fixture
        async def patch_session_factory(test_session_factory):
            configure_session_factory(test_session_factory)
            yield
            restore_session_factory()
    """
    global _active_factory
    _active_factory = factory
    logger.debug("db.session_factory_overridden")


def restore_session_factory() -> None:
    """
    Restore the active factory to the canonical engine-bound factory.

    Called in test teardown after configure_session_factory().
    """
    global _active_factory
    _active_factory = _canonical_factory
    logger.debug("db.session_factory_restored")


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """
    Return the currently active session factory.

    This is the ONLY function workers and routes should call to obtain a
    session factory.  It honours any test-time override set via
    configure_session_factory().

    Raises RuntimeError if called before init_db() AND before any test-time
    factory has been configured — fails loudly at the call site rather than
    with a cryptic connection error later.
    """
    if _active_factory is not None:
        return _active_factory

    raise RuntimeError(
        "DB session factory used before init_db() was called and no test "
        "factory has been configured via configure_session_factory().  "
        "Ensure init_db() runs at application startup, or call "
        "configure_session_factory() in your test fixture before any DB access."
    )


# ---------------------------------------------------------------------------
# Session context managers
# ---------------------------------------------------------------------------


@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Short-lived unit-of-work session context manager.

    Opens a session from the active factory, commits on clean exit, rolls back
    on any exception, and always closes the session to return the connection to
    the pool promptly.

    Usage::

        async with get_db_session() as session:
            result = await session.execute(select(JobRun).where(...))
    """
    factory = get_session_factory()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency that yields a session for the duration of a request.

    Injected via ``Depends(get_db)`` in route functions.  The session is
    committed when the handler returns normally and rolled back on any
    exception.
    """
    async with get_db_session() as session:
        yield session