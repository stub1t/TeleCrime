"""Drop unused B-tree indexes; soft_credential_hash created separately CONCURRENTLY.

ix_parsed_credentials_url and ix_parsed_credentials_email_domain had 0 scans
on a 146M-row table — every INSERT was updating 4+ GB of dead index weight.
Trigram GIN indexes (ix_pc_url_trgm, ix_pc_email_domain_trgm) cover ILIKE
searches on those columns, making these B-tree indexes redundant.

These two indexes were dropped CONCURRENTLY on the live DB before this
migration ran. The DROP INDEX IF EXISTS statements here are no-ops on that
DB and the correct operation on fresh installs.

The soft_credential_hash index is created in q7r8s9t0u1v2 using CONCURRENTLY
(can't run CONCURRENTLY inside a transaction block).

Revision ID: p6q7r8s9t0u1
Revises: o5p6q7r8s9t0
Create Date: 2026-05-08 11:00:00.000000
"""

from alembic import op


revision = "p6q7r8s9t0u1"
down_revision = "o5p6q7r8s9t0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("ix_parsed_credentials_url", table_name="parsed_credentials", if_exists=True)
    op.drop_index(
        "ix_parsed_credentials_email_domain", table_name="parsed_credentials", if_exists=True
    )


def downgrade() -> None:
    op.create_index(
        "ix_parsed_credentials_url", "parsed_credentials", ["url"], unique=False
    )
    op.create_index(
        "ix_parsed_credentials_email_domain",
        "parsed_credentials",
        ["email_domain"],
        unique=False,
    )
