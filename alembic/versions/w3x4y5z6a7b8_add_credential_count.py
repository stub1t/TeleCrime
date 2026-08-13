"""Add credential_count to archive_groups for fast web UI lookups.

Replaces the slow COUNT JOIN (parsed_credentials JOIN extraction_jobs) in the
ops fragment with a denormalized counter updated at finalize time.

Revision ID: w3x4y5z6a7b8
Revises: v2w3x4y5z6a7
Create Date: 2026-05-14 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "w3x4y5z6a7b8"
down_revision = "v2w3x4y5z6a7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "archive_groups",
        sa.Column("credential_count", sa.Integer(), nullable=False, server_default="0"),
    )


def downgrade() -> None:
    op.drop_column("archive_groups", "credential_count")
