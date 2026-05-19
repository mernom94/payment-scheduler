"""
0004_idempotency_key_attempt — Add `attempt` and `bunq_payment_id` columns to
idempotency_keys, and enforce UNIQUE(job_run_id, attempt).

Background
----------
The executor's _persist_success() uses ON CONFLICT DO NOTHING on the constraint
``uq_idempotency_run_attempt`` to implement the final idempotency gate: only one
worker can win the INSERT for a given (job_run_id, attempt) pair.  The constraint
was referenced in application code but never existed in the schema.

Additionally, the idempotency lookup in _find_idempotency_key() filters by
(job_run_id, attempt), so the `attempt` column is required for that query to work.

`bunq_payment_id` is stored on the idempotency record so that a recovered
executor can replay the success path using the stored payment ID without making
a duplicate API call.

Revision ID: 0004
Revises: 0003_updated_at_trigger
"""

from alembic import op
import sqlalchemy as sa

revision = "0004"
down_revision = "0003_updated_at_trigger"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add attempt column (NOT NULL with default 1 so existing rows are valid).
    op.add_column(
        "idempotency_keys",
        sa.Column("attempt", sa.Integer(), nullable=False, server_default="1"),
    )

    # Remove the server default now that existing rows are populated.
    op.alter_column("idempotency_keys", "attempt", server_default=None)

    # Add bunq_payment_id column for idempotency recovery replay.
    op.add_column(
        "idempotency_keys",
        sa.Column("bunq_payment_id", sa.String(255), nullable=True),
    )

    # Add the unique constraint that application code depends on.
    op.create_unique_constraint(
        "uq_idempotency_run_attempt",
        "idempotency_keys",
        ["job_run_id", "attempt"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_idempotency_run_attempt", "idempotency_keys", type_="unique")
    op.drop_column("idempotency_keys", "bunq_payment_id")
    op.drop_column("idempotency_keys", "attempt")
