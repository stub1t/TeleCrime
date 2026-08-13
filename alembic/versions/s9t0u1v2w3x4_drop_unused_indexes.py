"""Drop ix_parsed_credentials_soft_credential_hash and ix_parsed_credentials_domain.

ix_parsed_credentials_soft_credential_hash: 13 GB B-tree, 0 index scans ever —
completely unused; the soft hash is only used for analytics grouping, not lookups.

ix_parsed_credentials_domain: 1.7 GB B-tree, 1 scan across its lifetime — all
domain searches are ILIKE queries covered by the GIN trigram index ix_pc_domain_trgm.

Dropping both frees ~14.7 GB and eliminates their INSERT maintenance overhead.

Use CONCURRENTLY so drops don't block ongoing ingestion.

Alembic note: DROP INDEX CONCURRENTLY cannot run inside a transaction.
This migration disables Alembic's implicit transaction wrapper for Postgres.

Revision ID: s9t0u1v2w3x4
Revises: r8s9t0u1v2w3
Create Date: 2026-05-09 00:00:00.000000
"""

import sqlalchemy as sa
from alembic import op


revision = "s9t0u1v2w3x4"
down_revision = "r8s9t0u1v2w3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        bind.execute(sa.text("COMMIT"))
        # CONCURRENTLY needs ShareUpdateExclusiveLock and can take many minutes on large indexes
        bind.execute(sa.text("SET lock_timeout = 0"))
        bind.execute(sa.text("SET statement_timeout = 0"))
        bind.execute(
            sa.text(
                "DROP INDEX CONCURRENTLY IF EXISTS ix_parsed_credentials_soft_credential_hash"
            )
        )
        bind.execute(
            sa.text(
                "DROP INDEX CONCURRENTLY IF EXISTS ix_parsed_credentials_domain"
            )
        )
        bind.execute(sa.text("BEGIN"))
    else:
        op.drop_index("ix_parsed_credentials_soft_credential_hash", table_name="parsed_credentials", if_exists=True)
        op.drop_index("ix_parsed_credentials_domain", table_name="parsed_credentials", if_exists=True)


def downgrade() -> None:
    op.create_index(
        "ix_parsed_credentials_soft_credential_hash",
        "parsed_credentials",
        ["soft_credential_hash"],
        unique=False,
    )
    op.create_index(
        "ix_parsed_credentials_domain",
        "parsed_credentials",
        ["domain"],
        unique=False,
    )
