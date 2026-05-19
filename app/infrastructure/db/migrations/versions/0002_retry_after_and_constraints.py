"""Add retry_after to job_runs; CHECK constraints on status columns; partial PENDING index.

Revision ID: 0002_retry_after_and_constraints
Revises: 0001_initial
Create Date: 2026-05-12
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0002_retry_after_and_constraints"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # job_runs: add retry_after column
    # ------------------------------------------------------------------
    op.add_column(
        "job_runs",
        sa.Column("retry_after", sa.DateTime(timezone=True), nullable=True),
    )

    # Partial index: only PENDING rows that are claimable now or in the future.
    # Keeps executor claim query fast even with a large table of terminal rows.
    op.create_index(
        "ix_job_runs_pending_claimable",
        "job_runs",
        ["retry_after"],
        postgresql_where=sa.text("status = 'PENDING'"),
    )

    # ------------------------------------------------------------------
    # CHECK constraints: enforce enum values at the DB level
    # These catch any code path that uses a raw string instead of the enum.
    # ------------------------------------------------------------------
    op.create_check_constraint(
        "ck_job_runs_status",
        "job_runs",
        "status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'DEAD')",
    )

    op.create_check_constraint(
        "ck_scheduled_jobs_status",
        "scheduled_jobs",
        "status IN ('PENDING', 'BLOCKED', 'READY', 'DONE', 'CANCELLED')",
    )

    op.create_check_constraint(
        "ck_subscriptions_status",
        "subscriptions",
        "status IN ('ACTIVE', 'PAUSED', 'CANCELLED')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_subscriptions_status", "subscriptions", type_="check")
    op.drop_constraint("ck_scheduled_jobs_status", "scheduled_jobs", type_="check")
    op.drop_constraint("ck_job_runs_status", "job_runs", type_="check")
    op.drop_index("ix_job_runs_pending_claimable", table_name="job_runs")
    op.drop_column("job_runs", "retry_after")
