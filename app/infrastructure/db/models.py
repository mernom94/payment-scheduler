"""
SQLAlchemy 2.0 async ORM models.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger, Boolean, DateTime, ForeignKey, Index, Integer,
    String, Text, UniqueConstraint, func, text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# subscriptions
# ---------------------------------------------------------------------------

class Subscription(Base):
    __tablename__ = "subscriptions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)

    # Scheduling
    cron_expression: Mapped[str] = mapped_column(String(100), nullable=False)
    timezone: Mapped[str] = mapped_column(String(64), nullable=False, default="UTC")
    next_fire_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # Payment details (stored as JSONB for flexibility)
    payment_config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)

    # Retry policy
    retry_policy: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)

    # State
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="ACTIVE")
    consecutive_failures: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    paused_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # Audit
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    jobs: Mapped[list["ScheduledJob"]] = relationship("ScheduledJob", back_populates="subscription")

    __table_args__ = (
        Index("ix_subscriptions_next_fire_at_status", "next_fire_at", "status"),
    )


# ---------------------------------------------------------------------------
# scheduled_jobs
# ---------------------------------------------------------------------------

class ScheduledJob(Base):
    __tablename__ = "scheduled_jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    subscription_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("subscriptions.id", ondelete="CASCADE"), nullable=False
    )
    scheduled_for: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # Deduplication fingerprint
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)

    # DAG
    depends_on: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)

    # State  — valid values defined by JobStatus enum
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="PENDING")

    # Leader epoch that created this job (fencing)
    created_by_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    subscription: Mapped["Subscription"] = relationship("Subscription", back_populates="jobs")
    job_runs: Mapped[list["JobRun"]] = relationship("JobRun", back_populates="job")

    __table_args__ = (
        UniqueConstraint("subscription_id", "scheduled_for", name="uq_job_per_window"),
        UniqueConstraint("fingerprint", name="uq_job_fingerprint"),
        Index("ix_scheduled_jobs_status_scheduled_for", "status", "scheduled_for"),
        Index("ix_scheduled_jobs_subscription_id", "subscription_id"),
    )


# ---------------------------------------------------------------------------
# job_runs
# ---------------------------------------------------------------------------

class JobRun(Base):
    __tablename__ = "job_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("scheduled_jobs.id", ondelete="CASCADE"), nullable=False
    )

    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    # Worker ownership
    worker_id: Mapped[str | None] = mapped_column(String(255))
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lock_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # Status — valid values defined by JobRunStatus enum
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="PENDING")

    # Retry scheduling: executor skips this run until retry_after has passed.
    # NULL means "eligible immediately".
    retry_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # External execution result
    bunq_payment_id: Mapped[str | None] = mapped_column(String(255))
    error_message: Mapped[str | None] = mapped_column(Text)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # Relationships
    job: Mapped["ScheduledJob"] = relationship("ScheduledJob", back_populates="job_runs")

    __table_args__ = (
        UniqueConstraint("job_id", "attempt", name="uq_run_per_attempt"),
        Index("ix_job_runs_status", "status"),
        Index("ix_job_runs_lock_expires_at", "lock_expires_at"),
        # Partial index: only index PENDING rows that are due — keeps the
        # executor claim query fast even with a large history of terminal rows.
        Index(
            "ix_job_runs_pending_claimable",
            "retry_after",
            postgresql_where=text("status = 'PENDING'"),
        ),
    )


# ---------------------------------------------------------------------------
# idempotency_keys
# ---------------------------------------------------------------------------

class IdempotencyKeyRecord(Base):
    __tablename__ = "idempotency_keys"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    job_run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("job_runs.id", ondelete="CASCADE"), nullable=False
    )
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    bunq_payment_id: Mapped[str | None] = mapped_column(String(255))
    external_provider: Mapped[str] = mapped_column(String(50), nullable=False, default="bunq")
    response_snapshot: Mapped[dict | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("job_run_id", "attempt", name="uq_idempotency_run_attempt"),
    )


# ---------------------------------------------------------------------------
# dead_letter_queue
# ---------------------------------------------------------------------------

class DeadLetterEntry(Base):
    __tablename__ = "dead_letter_queue"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("job_runs.id", ondelete="CASCADE"), nullable=False
    )
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    subscription_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)

    failure_reason: Mapped[str] = mapped_column(Text, nullable=False)
    error_class: Mapped[str | None] = mapped_column(String(255))
    payload_snapshot: Mapped[dict] = mapped_column(JSONB, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    resolved_by: Mapped[str | None] = mapped_column(String(255))

    __table_args__ = (
        Index("ix_dlq_job_id", "job_id"),
        Index("ix_dlq_subscription_id", "subscription_id"),
        Index("ix_dlq_created_at", "created_at"),
    )
