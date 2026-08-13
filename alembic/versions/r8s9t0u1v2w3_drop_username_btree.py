"""Drop ix_parsed_credentials_username B-tree index (redundant with trigram).

ix_parsed_credentials_username had only 4 scans on a 146M-row table across its
entire lifetime — all username searches are ILIKE queries covered by the GIN
trigram index ix_pc_username_trgm.  The 4 GB B-tree is pure INSERT overhead.

Use CONCURRENTLY so it doesn't block ongoing ingestion.

Alembic note: DROP INDEX CONCURRENTLY cannot run inside a transaction.
This migration disables Alembic's implicit transaction wrapper for Postgres.

Revision ID: r8s9t0u1v2w3
Revises: q7r8s9t0u1v2
Create Date: 2026-05-08 12:00:00.000000
"""

import sqlalchemy as sa
from alembic import op


revision = "r8s9t0u1v2w3"
down_revision = "q7r8s9t0u1v2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        bind.execute(sa.text("COMMIT"))
        bind.execute(
            sa.text(
                "DROP INDEX CONCURRENTLY IF EXISTS ix_parsed_credentials_username"
            )
        )
        bind.execute(sa.text("BEGIN"))
    else:
        op.drop_index("ix_parsed_credentials_username", table_name="parsed_credentials", if_exists=True)


def downgrade() -> None:
    op.create_index(
        "ix_parsed_credentials_username",
        "parsed_credentials",
        ["username"],
        unique=False,
    )
