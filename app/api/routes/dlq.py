"""
Dead Letter Queue routes.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.db.models import DeadLetterEntry, Subscription
from app.infrastructure.db.session import get_db
from observability.setup import record_dlq_resolved

router = APIRouter()


class DLQEntryResponse(BaseModel):
    id: uuid.UUID
    job_run_id: uuid.UUID
    job_id: uuid.UUID
    subscription_id: uuid.UUID
    failure_reason: str
    error_class: str | None
    payload_snapshot: dict
    created_at: datetime
    resolved_at: datetime | None
    resolved_by: str | None

    class Config:
        from_attributes = True


@router.get("/", response_model=list[DLQEntryResponse])
async def list_dlq(
    subscription_id: uuid.UUID | None = None,
    resolved: bool = False,
    limit: int = 50,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
):
    q = select(DeadLetterEntry).limit(limit).offset(offset).order_by(DeadLetterEntry.created_at.desc())
    if subscription_id:
        q = q.where(DeadLetterEntry.subscription_id == subscription_id)
    if not resolved:
        q = q.where(DeadLetterEntry.resolved_at.is_(None))
    result = await db.execute(q)
    return result.scalars().all()


@router.get("/count")
async def dlq_count(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(func.count()).select_from(DeadLetterEntry).where(DeadLetterEntry.resolved_at.is_(None))
    )
    return {"unresolved_count": result.scalar_one()}


@router.post("/{entry_id}/resolve", response_model=DLQEntryResponse)
async def resolve_dlq_entry(
    entry_id: uuid.UUID,
    resolved_by: str = "manual",
    db: AsyncSession = Depends(get_db),
):
    entry = await db.get(DeadLetterEntry, entry_id)
    if not entry:
        raise HTTPException(status_code=404, detail="DLQ entry not found")
    if entry.resolved_at:
        raise HTTPException(status_code=409, detail="Already resolved")

    await db.execute(
        update(DeadLetterEntry)
        .where(DeadLetterEntry.id == entry_id)
        .values(resolved_at=datetime.now(timezone.utc), resolved_by=resolved_by)
    )
    await db.flush()
    await db.refresh(entry)
    record_dlq_resolved()
    return entry
