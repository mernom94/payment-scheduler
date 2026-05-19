"""
app/core/exceptions.py — Domain exception hierarchy.

All exceptions raised inside the application extend SchedulerError.
FastAPI exception handlers in app/main.py translate these into HTTP responses
so that HTTP status codes are never chosen inside domain, infrastructure, or
worker code — only at the API boundary.

Failure classification
----------------------
Exceptions are grouped into two behavioural categories that the executor and
recovery workers use to decide retry vs DLQ routing:

  Retryable (transient):   BunqTransientError, LeaderElectionError
  Non-retryable (permanent): ConfigurationError, BunqPaymentError,
                              BunqAmbiguousError (→ reconciliation path)

Workers catch each group explicitly rather than catching the base class so that
a new exception type is never silently swallowed or incorrectly retried.
"""

from typing import Optional


class SchedulerError(Exception):
    """Base class for all application errors."""

    def __init__(self, message: str, *, detail: Optional[str] = None) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.message!r})"


# ── Subscription errors ───────────────────────────────────────────────────────


class SubscriptionNotFoundError(SchedulerError):
    """No subscription with the given ID exists."""


class SubscriptionValidationError(SchedulerError):
    """Input validation failed (bad cron expression, invalid payment config, etc.)."""


class InvalidSubscriptionStateError(SchedulerError):
    """A subscription state transition was attempted that is not permitted."""

    def __init__(self, current: str, attempted: str) -> None:
        super().__init__(
            f"Cannot transition subscription from {current!r} to {attempted!r}."
        )
        self.current = current
        self.attempted = attempted


# ── Job / job-run errors ──────────────────────────────────────────────────────


class JobNotFoundError(SchedulerError):
    """No scheduled_job with the given ID exists."""


class InvalidJobStateError(SchedulerError):
    """
    A job_run state transition was attempted that is not permitted by the
    state machine.

    Raised by JobRunStateMachine.transition_to() when the target state is not
    in VALID_RUN_TRANSITIONS[current_state].
    """

    def __init__(self, current: str, attempted: str) -> None:
        super().__init__(
            f"Cannot transition job_run from {current!r} to {attempted!r}."
        )
        self.current = current
        self.attempted = attempted


# ── Configuration errors ──────────────────────────────────────────────────────


class ConfigurationError(SchedulerError):
    """
    Non-retryable: subscription payment_config is semantically invalid.

    This represents a permanent failure — retrying will not fix a bad config.
    The executor routes directly to DLQ when this is raised, and increments
    consecutive_failures on the subscription.

    Distinct from pydantic ValidationError (which covers structural/type errors
    caught at the API boundary).  ConfigurationError is raised after the row
    is already in the DB, when the worker attempts to parse the stored JSONB.
    """


# ── bunq / external API errors ────────────────────────────────────────────────


class BunqPaymentError(SchedulerError):
    """
    Non-retryable bunq API error (HTTP 4xx, excluding 429).

    A 4xx response means the request itself was invalid — the same request will
    always fail.  Retrying it would waste API quota and time.

    Carries the HTTP status code and raw response body for logging.
    """

    def __init__(self, status_code: int, body: str) -> None:
        super().__init__(
            f"bunq API error {status_code}: {body}",
            detail=body,
        )
        self.status_code = status_code


class BunqTransientError(SchedulerError):
    """
    Retryable bunq API error.

    Raised on: HTTP 429, 500, 502, 503, 504, network timeouts, DNS failures.
    The executor will schedule a retry with exponential backoff.
    """


class BunqAmbiguousError(SchedulerError):
    """
    The payment was dispatched to bunq but no response was received.

    The payment MAY OR MAY NOT have been created.  This is distinct from
    BunqTransientError because retrying without an idempotency check could
    double-charge.  The idempotency_key system handles safe retry for this
    case: re-submitting with the same key returns the existing result if bunq
    already processed it.
    """


# ── Infrastructure errors ─────────────────────────────────────────────────────


class LeaderElectionError(SchedulerError):
    """
    Leader election failed or leadership was lost unexpectedly.

    Raised by LeaderElection when the heartbeat renewal fails, indicating
    another instance took the leader key.  The scheduler and recovery workers
    stop writing when this is raised.
    """


class IdempotencyConflictError(SchedulerError):
    """
    An idempotency key conflict was detected during persist.

    Raised by the executor when INSERT idempotency_key ON CONFLICT returns
    no rows (another worker already inserted this key).  This is not an error
    condition — it means the concurrent worker successfully persisted the result
    and state is already correct.
    """
