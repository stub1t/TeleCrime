"""Drop ix_pc_email_domain_trgm (22 scans ever, 7 GB).

pg_stat_user_indexes shows only 22 lifetime scans vs 969 for domain_trgm.
The email_domain ILIKE filter in credential search falls back to a seq-scan
when the index is absent; given the low query rate this is acceptable and
saves ~7 GB of disk and ongoing write amplification on every credential insert.

Revision ID: y5z6a7b8c9d0
Revises: x4y5z6a7b8c9
Create Date: 2026-05-17 00:00:00.000000
"""

import sqlalchemy as sa
from alembic import op


revision = "y5z6a7b8c9d0"
down_revision = "x4y5z6a7b8c9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        bind.execute(sa.text("COMMIT"))
        bind.execute(sa.text("SET lock_timeout = 0"))
        bind.execute(sa.text("SET statement_timeout = 0"))
        bind.execute(
            sa.text(
                "DROP INDEX CONCURRENTLY IF EXISTS ix_pc_email_domain_trgm"
            )
        )
        bind.execute(sa.text("BEGIN"))
    else:
        op.drop_index(
            "ix_pc_email_domain_trgm",
            table_name="parsed_credentials",
            if_exists=True,
        )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    bind.execute(sa.text("COMMIT"))
    bind.execute(sa.text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
    bind.execute(sa.text("SET lock_timeout = 0"))
    bind.execute(sa.text("SET statement_timeout = 0"))
    bind.execute(
        sa.text(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_pc_email_domain_trgm "
            "ON parsed_credentials USING GIN (email_domain gin_trgm_ops)"
        )
    )
    bind.execute(sa.text("BEGIN"))
