"""Drop URL trigram index from broad credential search.

The broad PostgreSQL credential search no longer scans url for every token.
On the current dataset this GIN index is the largest credential index and still
causes slow rare-term searches under ingest load.

Revision ID: v2w3x4y5z6a7
Revises: u1v2w3x4y5z6
Create Date: 2026-05-11 00:10:00.000000
"""

import sqlalchemy as sa
from alembic import op


revision = "v2w3x4y5z6a7"
down_revision = "u1v2w3x4y5z6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        bind.execute(sa.text("COMMIT"))
        bind.execute(sa.text("SET lock_timeout = 0"))
        bind.execute(sa.text("SET statement_timeout = 0"))
        bind.execute(sa.text("DROP INDEX CONCURRENTLY IF EXISTS ix_pc_url_trgm"))
        bind.execute(sa.text("BEGIN"))
    else:
        op.drop_index("ix_pc_url_trgm", table_name="parsed_credentials", if_exists=True)


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
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_pc_url_trgm "
            "ON parsed_credentials USING GIN (url gin_trgm_ops)"
        )
    )
    bind.execute(sa.text("BEGIN"))
