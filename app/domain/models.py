"""
app/domain/models.py — Pure domain models.

No ORM, no framework dependencies.  Importable anywhere without side effects.
All classes here are either value objects (immutable, equality by value) or
thin behavioural entities (no DB access).

Subdomain packages
------------------
app/domain/jobs/state_machine.py — JobRun state transition machine.
"""

from __future__ import annotations

import hashlib
import random
import uuid
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any

# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class SubscriptionStatus(StrEnum):
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    CANCELLED = "CANCELLED"


class JobStatus(StrEnum):
    PENDING = "PENDING"
    BLOCKED = "BLOCKED"      # waiting on a DAG dependency
    READY = "READY"
    DONE = "DONE"            # at least one run SUCCEEDED
    CANCELLED = "CANCELLED"  # upstream DAG permanently failed


class JobRunStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    DEAD = "DEAD"            # exhausted retries → DLQ


# ---------------------------------------------------------------------------
# Value objects
# ---------------------------------------------------------------------------


class IdempotencyKey(str):
    """
    Deterministic payment idempotency key: sha256(job_run_id:attempt).

    Because the key is derived from (run_id, attempt) it is:
      - Stable across retries of the same attempt (process restarts, timeouts).
      - Unique per attempt so a retry of attempt N never collides with attempt N+1.
      - Safe to store in the DB as a plain string.
    """

    @classmethod
    def for_run(cls, job_run_id: uuid.UUID, attempt: int) -> "IdempotencyKey":
        raw = f"{job_run_id}:{attempt}"
        return cls(hashlib.sha256(raw.encode()).hexdigest())


class JobFingerprint(str):
    """
    Unique fingerprint per (subscription_id, scheduled_for).

    Used as a UNIQUE constraint guard so the scheduler never creates duplicate
    jobs for the same subscription window, even during leader failover overlap.
    """

    @classmethod
    def for_job(
        cls, subscription_id: uuid.UUID, scheduled_for: datetime
    ) -> "JobFingerprint":
        # scheduled_for must be UTC-normalised before calling so that the same
        # fire time expressed in different offsets produces the same fingerprint.
        raw = f"{subscription_id}:{scheduled_for.isoformat()}"
        return cls(hashlib.sha256(raw.encode()).hexdigest())


# ---------------------------------------------------------------------------
# Payment configuration value object
# ---------------------------------------------------------------------------


class PaymentConfig:
    """
    Validated payment configuration, constructed from a JSONB dict.

    Raises ValueError with a descriptive message on any invalid field so the
    executor can classify the failure as non-retryable (ConfigurationError).
    Validation is intentionally strict: a bad config will never succeed on
    retry, so failing fast is the correct behaviour.
    """

    REQUIRED_FIELDS = ("amount", "counterparty_iban", "counterparty_name")

    def __init__(
        self,
        amount: str,
        counterparty_iban: str,
        counterparty_name: str,
        currency: str = "EUR",
        description: str = "",
        monetary_account_id: int | None = None,
    ) -> None:
        try:
            parsed = Decimal(str(amount))
        except InvalidOperation:
            raise ValueError(
                f"payment_config.amount is not a valid decimal: {amount!r}"
            )
        if parsed <= 0:
            raise ValueError(
                f"payment_config.amount must be positive, got {amount!r}"
            )
        if not counterparty_iban or not counterparty_iban.strip():
            raise ValueError("payment_config.counterparty_iban is required")
        if not counterparty_name or not counterparty_name.strip():
            raise ValueError("payment_config.counterparty_name is required")
        if len(currency) != 3:
            raise ValueError(
                f"payment_config.currency must be a 3-char ISO code, got {currency!r}"
            )

        self.amount = parsed
        self.currency = currency.upper()
        self.counterparty_iban = counterparty_iban.strip()
        self.counterparty_name = counterparty_name.strip()
        self.description = description
        self.monetary_account_id = monetary_account_id

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "PaymentConfig":
        missing = [f for f in cls.REQUIRED_FIELDS if f not in d]
        if missing:
            raise ValueError(
                f"payment_config missing required fields: {missing}"
            )
        return cls(
            amount=d["amount"],
            counterparty_iban=d["counterparty_iban"],
            counterparty_name=d["counterparty_name"],
            currency=d.get("currency", "EUR"),
            description=d.get("description", ""),
            monetary_account_id=d.get("monetary_account_id"),
        )


# ---------------------------------------------------------------------------
# Retry policy
# ---------------------------------------------------------------------------

_RETRY_POLICY_KNOWN_KEYS = frozenset(
    {"max_attempts", "base_backoff_s", "max_backoff_s", "jitter"}
)


class RetryPolicy:
    """
    Exponential backoff retry policy with optional full jitter.

    Semantics of is_exhausted(attempt)
    ------------------------------------
    ``attempt`` is the 1-based index of the run that *just failed*.
    ``is_exhausted(N)`` returns True when N >= max_attempts, meaning the Nth
    attempt was the last allowed and no (N+1)th attempt should be created.

    Examples with max_attempts=3:
      is_exhausted(1) → False  (2nd attempt will be scheduled)
      is_exhausted(2) → False  (3rd attempt will be scheduled)
      is_exhausted(3) → True   (no 4th attempt; route to DLQ)
    """

    def __init__(
        self,
        max_attempts: int = 5,
        base_backoff_s: float = 60.0,
        max_backoff_s: float = 3600.0,
        jitter: bool = True,
    ) -> None:
        self.max_attempts = max_attempts
        self.base_backoff_s = base_backoff_s
        self.max_backoff_s = max_backoff_s
        self.jitter = jitter

    def next_delay_s(self, attempt: int) -> float:
        """
        Compute the backoff delay for the upcoming retry.

        ``attempt`` is the 1-based index of the attempt that *just failed*.
        The delay is calculated for the attempt that is about to be scheduled
        (attempt + 1), using exponential backoff with optional full jitter.

        Full jitter (when enabled) produces a delay in [0.5 * computed,
        1.0 * computed], reducing thundering-herd on shared infrastructure.
        """
        delay = min(
            self.base_backoff_s * (2 ** (attempt - 1)),
            self.max_backoff_s,
        )
        if self.jitter:
            delay *= 0.5 + random.random() * 0.5
        return delay

    def is_exhausted(self, attempt: int) -> bool:
        """
        Return True if no further retry should be scheduled.

        ``attempt`` is the 1-based number of the attempt that just failed.
        See class docstring for examples.
        """
        return attempt >= self.max_attempts

    def to_dict(self) -> dict[str, Any]:
        return {
            "max_attempts": self.max_attempts,
            "base_backoff_s": self.base_backoff_s,
            "max_backoff_s": self.max_backoff_s,
            "jitter": self.jitter,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "RetryPolicy":
        # Silently drop unknown keys so policy documents written by a newer
        # version of the code don't crash older workers after a rolling deploy.
        known = {k: v for k, v in d.items() if k in _RETRY_POLICY_KNOWN_KEYS}
        return cls(**known)
