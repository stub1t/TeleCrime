"""Drop ix_parsed_credentials_source_conversation_id (zero scans, 1.5 GB).

pg_stat_user_indexes shows this index has never been accessed since stats were
last reset.  The column is retained for provenance; only the B-tree index is
removed.  Saves ~1.5 GB of disk and ~1.5 GB of ongoing write amplification.

Revision ID: x4y5z6a7b8c9
Revises: w3x4y5z6a7b8
Create Date: 2026-05-17 00:00:00.000000
"""

import sqlalchemy as sa
from alembic import op


revision = "x4y5z6a7b8c9"
down_revision = "w3x4y5z6a7b8"
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
                "DROP INDEX CONCURRENTLY IF EXISTS "
                "ix_parsed_credentials_source_conversation_id"
            )
        )
        bind.execute(sa.text("BEGIN"))
    else:
        op.drop_index(
            "ix_parsed_credentials_source_conversation_id",
            table_name="parsed_credentials",
            if_exists=True,
        )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    bind.execute(sa.text("COMMIT"))
    bind.execute(sa.text("SET lock_timeout = 0"))
    bind.execute(sa.text("SET statement_timeout = 0"))
    bind.execute(
        sa.text(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
            "ix_parsed_credentials_source_conversation_id "
            "ON parsed_credentials (source_conversation_id)"
        )
    )
    bind.execute(sa.text("BEGIN"))
