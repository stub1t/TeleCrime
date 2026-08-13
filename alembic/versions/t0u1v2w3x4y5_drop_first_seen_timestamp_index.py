"""Drop ix_first_seen_index_first_seen_timestamp — 176 MB B-tree, 0 scans ever.

All first_seen_index lookups use content_hash (unique index). The timestamp
column is updated in-place on already-fetched rows; nobody queries by timestamp.
Dropping this frees 176 MB and eliminates per-update index maintenance
(including dead-tuple churn on the frequent timestamp-update path).

Revision ID: t0u1v2w3x4y5
Revises: s9t0u1v2w3x4
Create Date: 2026-05-09 00:00:00.000000
"""

import sqlalchemy as sa
from alembic import op


revision = "t0u1v2w3x4y5"
down_revision = "s9t0u1v2w3x4"
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
                "DROP INDEX CONCURRENTLY IF EXISTS ix_first_seen_index_first_seen_timestamp"
            )
        )
        bind.execute(sa.text("BEGIN"))
    else:
        op.drop_index(
            "ix_first_seen_index_first_seen_timestamp",
            table_name="first_seen_index",
            if_exists=True,
        )


def downgrade() -> None:
    op.create_index(
        "ix_first_seen_index_first_seen_timestamp",
        "first_seen_index",
        ["first_seen_timestamp"],
        unique=False,
    )
