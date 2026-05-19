"""initial schema

Revision ID: 0001_initial
Revises:
Create Date: 2024-01-01 00:00:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # subscriptions
    # ------------------------------------------------------------------
    op.create_table(
        "subscriptions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text),
        sa.Column("cron_expression", sa.String(100), nullable=False),
        sa.Column("timezone", sa.String(64), nullable=False, server_default="UTC"),
        sa.Column("next_fire_at", sa.DateTime(timezone=True)),
        sa.Column("payment_config", postgresql.JSONB, nullable=False),
        sa.Column("retry_policy", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("status", sa.String(20), nullable=False, server_default="ACTIVE"),
        sa.Column("consecutive_failures", sa.Integer, nullable=False, server_default="0"),
        sa.Column("paused_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_subscriptions_next_fire_at_status", "subscriptions", ["next_fire_at", "status"])

    # ------------------------------------------------------------------
    # scheduled_jobs
    # ------------------------------------------------------------------
    op.create_table(
        "scheduled_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("subscription_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("subscriptions.id", ondelete="CASCADE"), nullable=False),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("depends_on", postgresql.JSONB, nullable=False, server_default="[]"),
        sa.Column("status", sa.String(20), nullable=False, server_default="PENDING"),
        sa.Column("created_by_epoch", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("subscription_id", "scheduled_for", name="uq_job_per_window"),
        sa.UniqueConstraint("fingerprint", name="uq_job_fingerprint"),
    )
    op.create_index("ix_scheduled_jobs_status_scheduled_for", "scheduled_jobs", ["status", "scheduled_for"])
    op.create_index("ix_scheduled_jobs_subscription_id", "scheduled_jobs", ["subscription_id"])

    # ------------------------------------------------------------------
    # job_runs
    # ------------------------------------------------------------------
    op.create_table(
        "job_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("scheduled_jobs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("attempt", sa.Integer, nullable=False, server_default="1"),
        sa.Column("worker_id", sa.String(255)),
        sa.Column("claimed_at", sa.DateTime(timezone=True)),
        sa.Column("lock_expires_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(20), nullable=False, server_default="PENDING"),
        sa.Column("bunq_payment_id", sa.String(255)),
        sa.Column("error_message", sa.Text),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("job_id", "attempt", name="uq_run_per_attempt"),
    )
    op.create_index("ix_job_runs_status", "job_runs", ["status"])
    op.create_index("ix_job_runs_lock_expires_at", "job_runs", ["lock_expires_at"])

    # ------------------------------------------------------------------
    # idempotency_keys
    # ------------------------------------------------------------------
    op.create_table(
        "idempotency_keys",
        sa.Column("key", sa.String(64), primary_key=True),
        sa.Column("job_run_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("job_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("external_provider", sa.String(50), nullable=False, server_default="bunq"),
        sa.Column("response_snapshot", postgresql.JSONB),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    # ------------------------------------------------------------------
    # dead_letter_queue
    # ------------------------------------------------------------------
    op.create_table(
        "dead_letter_queue",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("job_run_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("job_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("subscription_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("failure_reason", sa.Text, nullable=False),
        sa.Column("error_class", sa.String(255)),
        sa.Column("payload_snapshot", postgresql.JSONB, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("resolved_at", sa.DateTime(timezone=True)),
        sa.Column("resolved_by", sa.String(255)),
    )
    op.create_index("ix_dlq_job_id", "dead_letter_queue", ["job_id"])
    op.create_index("ix_dlq_subscription_id", "dead_letter_queue", ["subscription_id"])
    op.create_index("ix_dlq_created_at", "dead_letter_queue", ["created_at"])

    # ------------------------------------------------------------------
    # Trigger: auto-update updated_at
    # ------------------------------------------------------------------
    op.execute("""
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ language 'plpgsql';
    """)
    for table in ("subscriptions", "scheduled_jobs"):
        op.execute(f"""
            CREATE TRIGGER update_{table}_updated_at
            BEFORE UPDATE ON {table}
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
        """)


def downgrade() -> None:
    for table in ("subscriptions", "scheduled_jobs"):
        op.execute(f"DROP TRIGGER IF EXISTS update_{table}_updated_at ON {table}")
    op.execute("DROP FUNCTION IF EXISTS update_updated_at_column()")

    op.drop_table("dead_letter_queue")
    op.drop_table("idempotency_keys")
    op.drop_table("job_runs")
    op.drop_table("scheduled_jobs")
    op.drop_table("subscriptions")
