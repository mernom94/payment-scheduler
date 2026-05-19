"""
Job and job_run query routes.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.db.models import JobRun, ScheduledJob
from app.infrastructure.db.session import get_db

router = APIRouter()


class JobResponse(BaseModel):
    id: uuid.UUID
    subscription_id: uuid.UUID
    scheduled_for: datetime
    fingerprint: str
    depends_on: list[str]
    status: str
    created_at: datetime

    class Config:
        from_attributes = True


class JobRunResponse(BaseModel):
    id: uuid.UUID
    job_id: uuid.UUID
    attempt: int
    worker_id: str | None
    status: str
    bunq_payment_id: str | None
    error_message: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None

    class Config:
        from_attributes = True


@router.get("/", response_model=list[JobResponse])
async def list_jobs(
    subscription_id: uuid.UUID | None = None,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
):
    q = (
        select(ScheduledJob)
        .limit(limit)
        .offset(offset)
        .order_by(ScheduledJob.scheduled_for.desc())
    )
    if subscription_id:
        q = q.where(ScheduledJob.subscription_id == subscription_id)
    if status:
        q = q.where(ScheduledJob.status == status.upper())
    result = await db.execute(q)
    return result.scalars().all()


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    job = await db.get(ScheduledJob, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.get("/{job_id}/runs", response_model=list[JobRunResponse])
async def list_job_runs(job_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(JobRun)
        .where(JobRun.job_id == job_id)
        .order_by(JobRun.attempt)
    )
    return result.scalars().all()


@router.get("/runs/{run_id}", response_model=JobRunResponse)
async def get_job_run(run_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    run = await db.get(JobRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Job run not found")
    return run
