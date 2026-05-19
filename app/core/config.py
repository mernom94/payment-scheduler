"""
app/core/config.py — Application configuration.

All settings are loaded from environment variables (or a .env file via
python-dotenv). No secrets are hard-coded. The Settings object is a singleton
produced by get_settings(), which is cached with @lru_cache so tests can patch
the factory rather than mutating module-level state.

Naming convention: UPPER_CASE fields mirror the environment variable names they
are populated from.  pydantic-settings maps DATABASE_URL → DATABASE_URL
case-insensitively, so existing deployments need no changes.
"""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        # Case-insensitive: DATABASE_URL, database_url, and Database_Url all
        # resolve to DATABASE_URL.  Prevents silent misses on mixed-case envs.
        case_sensitive=False,
    )

    # ── App ───────────────────────────────────────────────────────────────────
    APP_NAME: str = "payment-scheduler"
    DEBUG: bool = False
    # Unique worker identity.  Set via env (e.g. k8s pod name) for traceability
    # in multi-instance deployments.  Falls back to pid+uuid if empty.
    WORKER_ID: str = Field(default="", description="Unique worker identity")

    # ── Database ──────────────────────────────────────────────────────────────
    POSTGRES_DSN: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/payment_scheduler",
    )
    DB_POOL_SIZE: int = 10
    DB_MAX_OVERFLOW: int = 20

    # ── Redis ─────────────────────────────────────────────────────────────────
    REDIS_URL: str = Field(default="redis://localhost:6379/0")
    LEADER_LOCK_TTL_MS: int = 15_000        # 15 s
    LEADER_HEARTBEAT_INTERVAL_S: float = 5.0

    # ── Scheduler ─────────────────────────────────────────────────────────────
    SCHEDULER_INTERVAL_S: float = 10.0
    # How far ahead to fire jobs early (avoids missed fires on slow ticks).
    SCHEDULER_LOOKAHEAD_S: float = 60.0

    # ── Executor ──────────────────────────────────────────────────────────────
    EXECUTOR_POLL_INTERVAL_S: float = 2.0
    EXECUTOR_BATCH_SIZE: int = 10
    # A RUNNING job_run whose lock_expires_at is in the past is considered stuck
    # and will be reset by the recovery worker.  Must exceed the maximum expected
    # time for a bunq API call + DB write to complete.
    EXECUTOR_LOCK_TIMEOUT_S: int = 300      # 5 min

    # ── Recovery ──────────────────────────────────────────────────────────────
    RECOVERY_INTERVAL_S: float = 30.0
    MAX_RETRY_ATTEMPTS: int = 5

    # ── bunq ──────────────────────────────────────────────────────────────────
    BUNQ_BASE_URL: str = "https://public-api.sandbox.bunq.com/v1"
    BUNQ_API_KEY: str = Field(default="", description="bunq sandbox API key")
    BUNQ_MONETARY_ACCOUNT_ID: int = 0

    # ── Observability ─────────────────────────────────────────────────────────
    OTLP_ENDPOINT: str = "http://localhost:4317"
    LOG_LEVEL: str = "INFO"
    # json  → structured JSON lines (production, log aggregators)
    # text  → human-readable coloured output (local development)
    LOG_FORMAT: str = "json"


@lru_cache
def get_settings() -> Settings:
    """
    Return the singleton Settings instance.

    @lru_cache means the Settings object is constructed once and reused.
    In tests, patch this function rather than module-level state:

        with patch("app.core.config.get_settings", return_value=Settings(LOG_LEVEL="DEBUG")):
            ...

    Or clear the cache between tests:

        get_settings.cache_clear()
    """
    return Settings()
