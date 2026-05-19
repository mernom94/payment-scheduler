"""
conftest.py — Root pytest configuration and shared fixtures.

All test files can import fixtures defined here without explicit imports —
pytest discovers conftest.py automatically.

Fixture design principles
--------------------------
- Fixtures at this level are intentionally lightweight: no DB, no Redis,
  no network.  Integration fixtures live in tests/integration/conftest.py.
- Shared mocks are factory fixtures (return objects / callables) rather than
  auto-used so each test opts in explicitly.
- Settings are patched via get_settings() cache-clear, not env-var mutation,
  so parallel test runs don't interfere.
"""

import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Ensure the project root is on sys.path regardless of how pytest is invoked.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.core.config import get_settings, Settings


# ---------------------------------------------------------------------------
# Settings helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clear_settings_cache():
    """
    Clear the @lru_cache on get_settings() between tests.

    Prevents a test that patches settings from leaking its configuration into
    subsequent tests.  autouse=True so it applies everywhere without opt-in.
    """
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def test_settings() -> Settings:
    """
    Return a Settings instance pre-configured for tests.

    Overrides only values that differ from defaults.  Tests that need further
    customisation can build on this fixture.
    """
    return Settings(
        APP_NAME="payment-scheduler-test",
        DEBUG=True,
        LOG_FORMAT="text",
        LOG_LEVEL="DEBUG",
        POSTGRES_DSN="postgresql+asyncpg://test:test@localhost:5432/test_scheduler",
        REDIS_URL="redis://localhost:6379/15",
        EXECUTOR_BATCH_SIZE=5,
        EXECUTOR_LOCK_TIMEOUT_S=30,
        MAX_RETRY_ATTEMPTS=3,
    )


# ---------------------------------------------------------------------------
# Domain object factories
# ---------------------------------------------------------------------------


@pytest.fixture
def valid_payment_config() -> dict:
    return {
        "amount": "25.00",
        "currency": "EUR",
        "counterparty_iban": "NL02ABNA0123456789",
        "counterparty_name": "Test Recipient",
        "description": "Monthly subscription payment",
    }


@pytest.fixture
def valid_retry_policy() -> dict:
    return {
        "max_attempts": 3,
        "base_backoff_s": 1.0,
        "max_backoff_s": 10.0,
        "jitter": False,   # deterministic delays in tests
    }


@pytest.fixture
def subscription_id() -> uuid.UUID:
    return uuid.UUID("00000000-0000-0000-0000-000000000001")


@pytest.fixture
def job_id() -> uuid.UUID:
    return uuid.UUID("00000000-0000-0000-0000-000000000002")


@pytest.fixture
def run_id() -> uuid.UUID:
    return uuid.UUID("00000000-0000-0000-0000-000000000003")


# ---------------------------------------------------------------------------
# Mock bunq client
# ---------------------------------------------------------------------------


@pytest.fixture
def bunq_success_response() -> dict:
    """Standard bunq successful payment response."""
    return {"Response": [{"Id": {"id": "bunq-payment-test-001"}}]}


@pytest.fixture
def mock_bunq_client(bunq_success_response):
    """
    Mock BunqClient context manager that returns a successful payment response.

    Usage in a test::

        async def test_something(mock_bunq_client):
            mock_cls, mock_client = mock_bunq_client
            mock_client.create_payment.return_value = {"Response": [...]}

            with patch("app.workers.executor.BunqClient", mock_cls):
                ...
    """
    mock_client = AsyncMock()
    mock_client.create_payment = AsyncMock(return_value=bunq_success_response)
    mock_client.get_payment = AsyncMock(return_value=bunq_success_response)

    mock_cls = MagicMock()
    mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
    mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)

    return mock_cls, mock_client


# ---------------------------------------------------------------------------
# Mock Redis / leader election
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_redis():
    """Mock aioredis.Redis with common commands pre-configured."""
    redis = AsyncMock()
    redis.set = AsyncMock(return_value=True)
    redis.get = AsyncMock(return_value=None)
    redis.incr = AsyncMock(return_value=1)
    redis.eval = AsyncMock(return_value=1)
    redis.aclose = AsyncMock()
    return redis


@pytest.fixture
def mock_leader_election():
    """
    Mock LeaderElection that is always the leader with epoch=1.

    For tests that exercise worker logic that should run only on the leader.
    """
    election = MagicMock()
    election.is_leader = True
    election.epoch = 1
    election.try_acquire = AsyncMock(return_value=True)
    election.verify_still_leader = AsyncMock(return_value=True)
    election.release = AsyncMock()
    return election
