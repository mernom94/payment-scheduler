"""
0003_updated_at_trigger — DB-level updated_at refresh trigger.

SQLAlchemy's onupdate=func.now() only fires for ORM-style UPDATE statements,
not for bulk update() calls.  Since the executor, scheduler, and recovery
workers exclusively use bulk updates for performance, updated_at was never
being refreshed.

Fix: a BEFORE UPDATE trigger on each table calls set_updated_at(), which sets
NEW.updated_at = NOW() before the row is written.  This is DB-enforced and
works regardless of how the update was issued (ORM, bulk, raw SQL, migration).

Revision ID: 0003
Revises: 0002_retry_after_and_constraints
"""

from alembic import op

revision = "0003"
down_revision = "0002_retry_after_and_constraints"
branch_labels = None
depends_on = None

# Tables with an updated_at column that must be kept current.
_TABLES = ("subscriptions", "scheduled_jobs")


def upgrade() -> None:
    # Shared trigger function — created once, reused by all tables.
    op.execute("""
        CREATE OR REPLACE FUNCTION trg_set_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)

    for table in _TABLES:
        op.execute(f"""
            DROP TRIGGER IF EXISTS trg_{table}_updated_at ON {table};
        """)
        op.execute(f"""
            CREATE TRIGGER trg_{table}_updated_at
            BEFORE UPDATE ON {table}
            FOR EACH ROW EXECUTE FUNCTION trg_set_updated_at();
        """)


def downgrade() -> None:
    for table in _TABLES:
        op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_updated_at ON {table};")
    op.execute("DROP FUNCTION IF EXISTS trg_set_updated_at();")
