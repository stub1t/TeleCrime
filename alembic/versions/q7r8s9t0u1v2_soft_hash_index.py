"""Add soft_credential_hash index using CONCURRENTLY.

The soft_credential_hash column was added in m3n4o5p6q7r8 but its index
went missing from the live DB. This migration restores it using
CONCURRENTLY so it doesn't block ongoing ingestion.

Alembic note: CREATE INDEX CONCURRENTLY cannot run inside a transaction.
This migration disables Alembic's implicit transaction wrapper for Postgres.

Revision ID: q7r8s9t0u1v2
Revises: p6q7r8s9t0u1
Create Date: 2026-05-08 11:30:00.000000
"""

import sqlalchemy as sa
from alembic import op


revision = "q7r8s9t0u1v2"
down_revision = "p6q7r8s9t0u1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        # CONCURRENTLY can't run in a transaction; commit first.
        bind.execute(sa.text("COMMIT"))
        bind.execute(
            sa.text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS"
                " ix_parsed_credentials_soft_credential_hash"
                " ON parsed_credentials (soft_credential_hash)"
            )
        )
        # Alembic needs an open transaction to write alembic_version.
        bind.execute(sa.text("BEGIN"))
    else:
        # SQLite: index already exists from m3n4o5p6q7r8; no-op via IF NOT EXISTS
        op.create_index(
            "ix_parsed_credentials_soft_credential_hash",
            "parsed_credentials",
            ["soft_credential_hash"],
            unique=False,
            if_not_exists=True,
        )


def downgrade() -> None:
    op.drop_index(
        "ix_parsed_credentials_soft_credential_hash",
        table_name="parsed_credentials",
        if_exists=True,
    )
