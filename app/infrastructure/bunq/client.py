"""
app/infrastructure/bunq/client.py — bunq Sandbox API integration.

Key guarantee: every payment call is wrapped with an idempotency key so that
retrying the same call never double-charges.  bunq honours the
X-Bunq-Client-Request-Id header for idempotency.

Exception taxonomy (defined in app.core.exceptions, imported here):
  BunqPaymentError   — Non-retryable 4xx.  The request itself is wrong.
  BunqTransientError — Retryable 5xx / 429 / network failure.
  BunqAmbiguousError — Request sent, no response received.  May or may not
                       have been processed; idempotency key enables safe retry.

References:
  https://doc.bunq.com/#/payment
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

import httpx

from app.core.config import get_settings
from app.core.exceptions import BunqAmbiguousError, BunqPaymentError, BunqTransientError

logger = logging.getLogger(__name__)

_PAYMENT_ENDPOINT = "/monetary-account/{account_id}/payment"

# HTTP status codes that indicate a transient failure worth retrying.
_TRANSIENT_STATUS_CODES: frozenset[int] = frozenset({429, 500, 502, 503, 504})


class BunqClient:
    """
    Async HTTP client for the bunq API.

    Designed to be used as an async context manager::

        async with BunqClient() as client:
            response = await client.create_payment(...)

    A new httpx.AsyncClient is created per context-manager entry and closed on
    exit.  Do not share a single BunqClient instance across concurrent tasks —
    each task should open its own context.
    """

    def __init__(self) -> None:
        s = get_settings()
        self._client = httpx.AsyncClient(
            base_url=s.BUNQ_BASE_URL,
            headers={
                "Content-Type": "application/json",
                "X-Bunq-Client-Authentication": s.BUNQ_API_KEY,
                "User-Agent": f"{s.APP_NAME}/1.0",
            },
            timeout=30.0,
        )

    async def __aenter__(self) -> "BunqClient":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self._client.aclose()

    async def create_payment(
        self,
        *,
        idempotency_key: str,
        amount: Decimal,
        currency: str,
        counterparty_iban: str,
        counterparty_name: str,
        description: str,
        monetary_account_id: int | None = None,
    ) -> dict[str, Any]:
        """
        Execute a payment via bunq.

        The idempotency_key (X-Bunq-Client-Request-Id) ensures that if bunq
        already processed this request, it returns the existing result rather
        than creating a duplicate payment.  The executor generates this key
        deterministically as sha256(job_run_id:attempt) so it survives restarts
        and is consistent across retries of the same attempt.

        Returns:
            The full bunq response dict on success.

        Raises:
            BunqPaymentError: Non-retryable 4xx response.
            BunqTransientError: Retryable 5xx / 429 / rate-limit.
            BunqAmbiguousError: Network error after the request was sent —
                the payment may or may not have been created.
        """
        s = get_settings()
        account_id = monetary_account_id or s.BUNQ_MONETARY_ACCOUNT_ID
        endpoint = _PAYMENT_ENDPOINT.format(account_id=account_id)

        payload = {
            "amount": {"value": str(amount), "currency": currency},
            "counterparty_alias": {
                "type": "IBAN",
                "value": counterparty_iban,
                "name": counterparty_name,
            },
            "description": description,
        }

        logger.info(
            "bunq.payment_request",
            extra={
                "idempotency_key": idempotency_key,
                "amount": str(amount),
                "currency": currency,
                # Log partial IBAN only — avoid logging full account numbers.
                "counterparty_iban_prefix": counterparty_iban[:8],
            },
        )

        try:
            response = await self._client.post(
                endpoint,
                json=payload,
                headers={"X-Bunq-Client-Request-Id": idempotency_key},
            )
        except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as exc:
            # The request may have been received by bunq before the network
            # error occurred.  Raise BunqAmbiguousError so the caller knows
            # it must not assume failure.
            raise BunqAmbiguousError(
                f"Network error calling bunq (payment outcome unknown): {exc}"
            ) from exc

        if response.status_code in (200, 201):
            data = response.json()
            payment_id = str(
                data.get("Response", [{}])[0].get("Id", {}).get("id", "unknown")
            )
            logger.info(
                "bunq.payment_success",
                extra={
                    "idempotency_key": idempotency_key,
                    "payment_id": payment_id,
                },
            )
            return data

        if response.status_code in _TRANSIENT_STATUS_CODES:
            raise BunqTransientError(
                f"Transient bunq error {response.status_code}: "
                f"{response.text[:200]}"
            )

        # 4xx (excluding 429 handled above): non-retryable.
        raise BunqPaymentError(response.status_code, response.text[:500])

    async def get_payment(
        self,
        payment_id: str,
        monetary_account_id: int | None = None,
    ) -> dict[str, Any]:
        """Fetch an existing payment by ID (used by reconciliation paths)."""
        s = get_settings()
        account_id = monetary_account_id or s.BUNQ_MONETARY_ACCOUNT_ID
        endpoint = f"/monetary-account/{account_id}/payment/{payment_id}"

        response = await self._client.get(endpoint)
        if response.status_code == 200:
            return response.json()

        if response.status_code in _TRANSIENT_STATUS_CODES:
            raise BunqTransientError(
                f"Transient error fetching payment {payment_id}: {response.status_code}"
            )
        raise BunqPaymentError(response.status_code, response.text[:500])
