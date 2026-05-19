"""
app/core/constants.py — Shared constants and Redis key definitions.

Using named constants rather than magic strings scattered through the codebase
means typos are caught at import time, IDEs can navigate to the definition,
and changing a key prefix is a single-location edit.
"""

# ── Retry limits ──────────────────────────────────────────────────────────────

# Default maximum retries for a job_run before it is moved to the DLQ.
# Individual subscriptions may override this via their retry_policy JSONB.
MAX_JOB_RETRIES: int = 5

# ── Worker timing ─────────────────────────────────────────────────────────────

# A RUNNING job_run whose lock_expires_at has passed is considered stuck.
# This value is the default; the config EXECUTOR_LOCK_TIMEOUT_S overrides it.
EXECUTOR_LOCK_TIMEOUT_S: int = 300  # 5 min

# ── Redis keys ────────────────────────────────────────────────────────────────

# SET NX key used for leader election.
REDIS_LEADER_KEY: str = "payment_scheduler:leader"

# INCR key that holds the monotonically-increasing fencing epoch.
REDIS_LEADER_EPOCH_KEY: str = "payment_scheduler:leader_epoch"

# ── Subscription auto-pause threshold ────────────────────────────────────────

# Pause a subscription after this many consecutive DLQ entries.
# Prevents a broken subscription from continuously filling the DLQ.
CONSECUTIVE_FAILURE_PAUSE_THRESHOLD: int = 3

# ── Executor / persistence ────────────────────────────────────────────────────

# Maximum length stored in error_message / failure_reason columns.
ERROR_MESSAGE_MAX_LEN: int = 2000
