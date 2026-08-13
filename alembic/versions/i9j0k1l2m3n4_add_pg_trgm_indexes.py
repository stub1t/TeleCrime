"""Add pg_trgm extension and GIN indexes for PostgreSQL full-text search.

For SQLite deployments this migration is a no-op.

Revision ID: i9j0k1l2m3n4
Revises: h8c9d0e1f2a3
Create Date: 2026-03-23

"""

from typing import Union

from alembic import op
import sqlalchemy as sa

revision: str = "i9j0k1l2m3n4"
down_revision: Union[str, None] = "h8c9d0e1f2a3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    if op.get_bind().dialect.name != "postgresql":
        return

    # CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
    with op.get_context().autocommit_block():
        op.execute(sa.text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        op.execute(
            sa.text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_pc_domain_trgm "
                "ON parsed_credentials USING GIN (domain gin_trgm_ops)"
            )
        )
        op.execute(
            sa.text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_pc_username_trgm "
                "ON parsed_credentials USING GIN (username gin_trgm_ops)"
            )
        )
        op.execute(
            sa.text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_pc_url_trgm "
                "ON parsed_credentials USING GIN (url gin_trgm_ops)"
            )
        )
        op.execute(
            sa.text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_pc_email_domain_trgm "
                "ON parsed_credentials USING GIN (email_domain gin_trgm_ops)"
            )
        )
        op.execute(
            sa.text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_pc_source_archive_trgm "
                "ON parsed_credentials USING GIN (source_archive gin_trgm_ops)"
            )
        )


def downgrade() -> None:
    if op.get_bind().dialect.name != "postgresql":
        return

    with op.get_context().autocommit_block():
        op.execute(sa.text("DROP INDEX IF EXISTS ix_pc_domain_trgm"))
        op.execute(sa.text("DROP INDEX IF EXISTS ix_pc_username_trgm"))
        op.execute(sa.text("DROP INDEX IF EXISTS ix_pc_url_trgm"))
        op.execute(sa.text("DROP INDEX IF EXISTS ix_pc_email_domain_trgm"))
        op.execute(sa.text("DROP INDEX IF EXISTS ix_pc_source_archive_trgm"))
