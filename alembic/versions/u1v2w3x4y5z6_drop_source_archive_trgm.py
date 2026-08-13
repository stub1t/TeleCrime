"""Drop source_archive trigram index.

The broad credential search path no longer scans source_archive for every token.
On large datasets this index costs substantial disk and insert maintenance while
providing little value compared with domain, username, URL, and email-domain
search.

Revision ID: u1v2w3x4y5z6
Revises: t0u1v2w3x4y5
Create Date: 2026-05-11 00:00:00.000000
"""

import sqlalchemy as sa
from alembic import op


revision = "u1v2w3x4y5z6"
down_revision = "t0u1v2w3x4y5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        bind.execute(sa.text("COMMIT"))
        bind.execute(sa.text("SET lock_timeout = 0"))
        bind.execute(sa.text("SET statement_timeout = 0"))
        bind.execute(sa.text("DROP INDEX CONCURRENTLY IF EXISTS ix_pc_source_archive_trgm"))
        bind.execute(sa.text("BEGIN"))
    else:
        op.drop_index("ix_pc_source_archive_trgm", table_name="parsed_credentials", if_exists=True)


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
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_pc_source_archive_trgm "
            "ON parsed_credentials USING GIN (source_archive gin_trgm_ops)"
        )
    )
    bind.execute(sa.text("BEGIN"))
