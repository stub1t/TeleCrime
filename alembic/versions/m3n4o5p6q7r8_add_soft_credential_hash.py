"""add soft credential hash

Revision ID: m3n4o5p6q7r8
Revises: l2m3n4o5p6q7
Create Date: 2026-04-20 20:15:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op


revision: str = "m3n4o5p6q7r8"
down_revision: str | None = "l2m3n4o5p6q7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "parsed_credentials",
        sa.Column("soft_credential_hash", sa.String(length=64), nullable=True),
    )
    op.create_index(
        op.f("ix_parsed_credentials_soft_credential_hash"),
        "parsed_credentials",
        ["soft_credential_hash"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_parsed_credentials_soft_credential_hash"),
        table_name="parsed_credentials",
    )
    op.drop_column("parsed_credentials", "soft_credential_hash")
