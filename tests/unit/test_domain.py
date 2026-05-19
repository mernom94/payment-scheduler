"""
tests/unit/test_domain.py — Domain model unit tests.

Tests for PaymentConfig, RetryPolicy, IdempotencyKey, JobFingerprint.
No DB, Redis, or network dependencies.
"""

import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from app.domain.models import (
    IdempotencyKey,
    JobFingerprint,
    PaymentConfig,
    RetryPolicy,
)


class TestPaymentConfig:
    def test_valid_config_parses_successfully(self, valid_payment_config):
        cfg = PaymentConfig.from_dict(valid_payment_config)
        assert cfg.amount == Decimal("25.00")
        assert cfg.currency == "EUR"
        assert cfg.counterparty_iban == "NL02ABNA0123456789"
        assert cfg.counterparty_name == "Test Recipient"

    def test_missing_required_field_raises(self, valid_payment_config):
        del valid_payment_config["amount"]
        with pytest.raises(ValueError, match="missing required fields"):
            PaymentConfig.from_dict(valid_payment_config)

    def test_non_numeric_amount_raises(self, valid_payment_config):
        valid_payment_config["amount"] = "not-a-number"
        with pytest.raises(ValueError, match="not a valid decimal"):
            PaymentConfig.from_dict(valid_payment_config)

    def test_zero_amount_raises(self, valid_payment_config):
        valid_payment_config["amount"] = "0"
        with pytest.raises(ValueError, match="must be positive"):
            PaymentConfig.from_dict(valid_payment_config)

    def test_negative_amount_raises(self, valid_payment_config):
        valid_payment_config["amount"] = "-5.00"
        with pytest.raises(ValueError, match="must be positive"):
            PaymentConfig.from_dict(valid_payment_config)

    def test_blank_iban_raises(self, valid_payment_config):
        valid_payment_config["counterparty_iban"] = "   "
        with pytest.raises(ValueError, match="counterparty_iban is required"):
            PaymentConfig.from_dict(valid_payment_config)

    def test_invalid_currency_code_raises(self, valid_payment_config):
        valid_payment_config["currency"] = "EURO"
        with pytest.raises(ValueError, match="3-char ISO code"):
            PaymentConfig.from_dict(valid_payment_config)

    def test_currency_uppercased(self, valid_payment_config):
        valid_payment_config["currency"] = "eur"
        cfg = PaymentConfig.from_dict(valid_payment_config)
        assert cfg.currency == "EUR"

    def test_iban_whitespace_stripped(self, valid_payment_config):
        valid_payment_config["counterparty_iban"] = "  NL02ABNA0123456789  "
        cfg = PaymentConfig.from_dict(valid_payment_config)
        assert cfg.counterparty_iban == "NL02ABNA0123456789"

    def test_optional_fields_default(self, valid_payment_config):
        cfg = PaymentConfig.from_dict(valid_payment_config)
        assert cfg.monetary_account_id is None
        assert cfg.description == "Monthly subscription payment"


class TestRetryPolicy:
    def test_default_policy(self):
        policy = RetryPolicy()
        assert policy.max_attempts == 5
        assert policy.jitter is True

    def test_from_dict_round_trip(self, valid_retry_policy):
        policy = RetryPolicy.from_dict(valid_retry_policy)
        assert policy.max_attempts == 3
        assert policy.base_backoff_s == 1.0
        assert policy.jitter is False

    def test_from_dict_ignores_unknown_keys(self):
        """Forward-compatible: unknown keys must not crash older workers."""
        policy = RetryPolicy.from_dict(
            {"max_attempts": 2, "unknown_future_key": "value"}
        )
        assert policy.max_attempts == 2

    @pytest.mark.parametrize(
        "attempt, expected_exhausted",
        [
            (1, False),
            (2, False),
            (3, True),   # 3 >= max_attempts=3
            (4, True),   # already over
        ],
    )
    def test_is_exhausted_semantics(self, attempt: int, expected_exhausted: bool):
        """
        is_exhausted(attempt) where attempt is the 1-based index of the run
        that just failed.  max_attempts=3 means the 3rd attempt is the last.
        """
        policy = RetryPolicy(max_attempts=3, jitter=False)
        assert policy.is_exhausted(attempt) is expected_exhausted

    def test_next_delay_s_increases_with_attempt(self):
        policy = RetryPolicy(base_backoff_s=10.0, max_backoff_s=1000.0, jitter=False)
        delays = [policy.next_delay_s(a) for a in range(1, 6)]
        for i in range(len(delays) - 1):
            assert delays[i] <= delays[i + 1]

    def test_next_delay_s_capped_at_max(self):
        policy = RetryPolicy(
            base_backoff_s=60.0, max_backoff_s=120.0, jitter=False
        )
        # With large attempt, delay would exceed max without the cap
        delay = policy.next_delay_s(100)
        assert delay == 120.0

    def test_jitter_stays_within_range(self):
        policy = RetryPolicy(base_backoff_s=100.0, max_backoff_s=1000.0, jitter=True)
        for _ in range(50):
            delay = policy.next_delay_s(1)
            assert 50.0 <= delay <= 100.0, f"Jitter out of range: {delay}"


class TestIdempotencyKey:
    def test_same_inputs_produce_same_key(self, run_id):
        k1 = IdempotencyKey.for_run(run_id, 1)
        k2 = IdempotencyKey.for_run(run_id, 1)
        assert k1 == k2

    def test_different_attempt_produces_different_key(self, run_id):
        k1 = IdempotencyKey.for_run(run_id, 1)
        k2 = IdempotencyKey.for_run(run_id, 2)
        assert k1 != k2

    def test_different_run_id_produces_different_key(self):
        id1, id2 = uuid.uuid4(), uuid.uuid4()
        k1 = IdempotencyKey.for_run(id1, 1)
        k2 = IdempotencyKey.for_run(id2, 1)
        assert k1 != k2

    def test_key_is_hex_string(self, run_id):
        key = IdempotencyKey.for_run(run_id, 1)
        assert len(key) == 64
        int(key, 16)  # raises ValueError if not valid hex


class TestJobFingerprint:
    def test_same_inputs_produce_same_fingerprint(self, subscription_id):
        dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        f1 = JobFingerprint.for_job(subscription_id, dt)
        f2 = JobFingerprint.for_job(subscription_id, dt)
        assert f1 == f2

    def test_different_subscription_produces_different_fingerprint(self):
        dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        f1 = JobFingerprint.for_job(uuid.uuid4(), dt)
        f2 = JobFingerprint.for_job(uuid.uuid4(), dt)
        assert f1 != f2

    def test_different_fire_time_produces_different_fingerprint(
        self, subscription_id
    ):
        dt1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        dt2 = datetime(2024, 1, 16, 12, 0, 0, tzinfo=timezone.utc)
        f1 = JobFingerprint.for_job(subscription_id, dt1)
        f2 = JobFingerprint.for_job(subscription_id, dt2)
        assert f1 != f2
