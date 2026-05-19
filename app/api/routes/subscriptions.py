"""
Subscription CRUD routes.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from croniter import croniter
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, field_validator
from pytz import timezone as pytz_timezone, all_timezones
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.models import RetryPolicy
from app.infrastructure.db.models import Subscription
from app.infrastructure.db.session import get_db

router = APIRouter()


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class RetryPolicySchema(BaseModel):
    max_attempts: int = Field(default=5, ge=1, le=20)
    base_backoff_s: float = Field(default=60.0, ge=1.0)
    max_backoff_s: float = Field(default=3600.0, ge=60.0)
    jitter: bool = True


class PaymentConfigSchema(BaseModel):
    amount: str = Field(..., description="Decimal string e.g. '12.50'")
    currency: str = Field(default="EUR", min_length=3, max_length=3)
    counterparty_iban: str
    counterparty_name: str
    description: str = ""
    monetary_account_id: int | None = None


class SubscriptionCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    cron_expression: str
    timezone: str = "UTC"
    payment_config: PaymentConfigSchema
    retry_policy: RetryPolicySchema = Field(default_factory=RetryPolicySchema)

    @field_validator("cron_expression")
    @classmethod
    def validate_cron(cls, v: str) -> str:
        if not croniter.is_valid(v):
            raise ValueError(f"Invalid cron expression: {v!r}")
        return v

    @field_validator("timezone")
    @classmethod
    def validate_tz(cls, v: str) -> str:
        if v not in all_timezones:
            raise ValueError(f"Unknown timezone: {v!r}")
        return v


class SubscriptionUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    cron_expression: str | None = None
    timezone: str | None = None
    payment_config: PaymentConfigSchema | None = None
    retry_policy: RetryPolicySchema | None = None
    status: str | None = None

    @field_validator("cron_expression")
    @classmethod
    def validate_cron(cls, v: str | None) -> str | None:
        if v is not None and not croniter.is_valid(v):
            raise ValueError(f"Invalid cron expression: {v!r}")
        return v


class SubscriptionResponse(BaseModel):
    id: uuid.UUID
    name: str
    description: str | None
    cron_expression: str
    timezone: str
    next_fire_at: datetime | None
    payment_config: dict[str, Any]
    retry_policy: dict[str, Any]
    status: str
    consecutive_failures: int
    paused_at: datetime | None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def compute_next_fire(cron_expr: str, tz_name: str) -> datetime:
    tz = pytz_timezone(tz_name)
    now_local = datetime.now(tz)
    cron = croniter(cron_expr, now_local)
    return cron.get_next(datetime).astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post("/", response_model=SubscriptionResponse, status_code=status.HTTP_201_CREATED)
async def create_subscription(
    body: SubscriptionCreate,
    db: AsyncSession = Depends(get_db),
):
    next_fire = compute_next_fire(body.cron_expression, body.timezone)
    sub = Subscription(
        name=body.name,
        description=body.description,
        cron_expression=body.cron_expression,
        timezone=body.timezone,
        next_fire_at=next_fire,
        payment_config=body.payment_config.model_dump(),
        retry_policy=body.retry_policy.model_dump(),
        status="ACTIVE",
    )
    db.add(sub)
    await db.flush()
    await db.refresh(sub)
    return sub


@router.get("/", response_model=list[SubscriptionResponse])
async def list_subscriptions(
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
):
    q = select(Subscription).limit(limit).offset(offset).order_by(Subscription.created_at.desc())
    if status:
        q = q.where(Subscription.status == status.upper())
    result = await db.execute(q)
    return result.scalars().all()


@router.get("/{subscription_id}", response_model=SubscriptionResponse)
async def get_subscription(
    subscription_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    sub = await db.get(Subscription, subscription_id)
    if not sub:
        raise HTTPException(status_code=404, detail="Subscription not found")
    return sub


@router.patch("/{subscription_id}", response_model=SubscriptionResponse)
async def update_subscription(
    subscription_id: uuid.UUID,
    body: SubscriptionUpdate,
    db: AsyncSession = Depends(get_db),
):
    sub = await db.get(Subscription, subscription_id)
    if not sub:
        raise HTTPException(status_code=404, detail="Subscription not found")

    changes: dict[str, Any] = {}
    if body.name is not None:
        changes["name"] = body.name
    if body.description is not None:
        changes["description"] = body.description
    if body.cron_expression is not None:
        changes["cron_expression"] = body.cron_expression
        tz = body.timezone or sub.timezone
        changes["next_fire_at"] = compute_next_fire(body.cron_expression, tz)
    if body.timezone is not None:
        changes["timezone"] = body.timezone
    if body.payment_config is not None:
        changes["payment_config"] = body.payment_config.model_dump()
    if body.retry_policy is not None:
        changes["retry_policy"] = body.retry_policy.model_dump()
    if body.status is not None:
        allowed = {"ACTIVE", "PAUSED", "CANCELLED"}
        if body.status.upper() not in allowed:
            raise HTTPException(status_code=422, detail=f"status must be one of {allowed}")
        changes["status"] = body.status.upper()
        if body.status.upper() == "PAUSED":
            changes["paused_at"] = datetime.now(timezone.utc)

    if changes:
        await db.execute(
            update(Subscription)
            .where(Subscription.id == subscription_id)
            .values(**changes)
        )
        await db.flush()
        await db.refresh(sub)
    return sub


@router.delete("/{subscription_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_subscription(
    subscription_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    sub = await db.get(Subscription, subscription_id)
    if not sub:
        raise HTTPException(status_code=404, detail="Subscription not found")
    await db.execute(
        update(Subscription)
        .where(Subscription.id == subscription_id)
        .values(status="CANCELLED")
    )


@router.post("/{subscription_id}/resume", response_model=SubscriptionResponse)
async def resume_subscription(
    subscription_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    sub = await db.get(Subscription, subscription_id)
    if not sub:
        raise HTTPException(status_code=404, detail="Subscription not found")
    next_fire = compute_next_fire(sub.cron_expression, sub.timezone)
    await db.execute(
        update(Subscription)
        .where(Subscription.id == subscription_id)
        .values(
            status="ACTIVE",
            paused_at=None,
            consecutive_failures=0,
            next_fire_at=next_fire,
        )
    )
    await db.flush()
    await db.refresh(sub)
    return sub
