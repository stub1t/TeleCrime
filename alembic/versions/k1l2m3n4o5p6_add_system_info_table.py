"""Add system_info table — persist machine metadata from SystemInfo.txt per extraction job.

Revision ID: k1l2m3n4o5p6
Revises: j0k1l2m3n4o5
Create Date: 2026-03-23

"""

from typing import Union

from alembic import op
import sqlalchemy as sa

revision: str = "k1l2m3n4o5p6"
down_revision: Union[str, None] = "j0k1l2m3n4o5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "system_info",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "extraction_job_id",
            sa.Integer,
            sa.ForeignKey("extraction_jobs.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        sa.Column("hostname", sa.String(255), nullable=True),
        sa.Column("username", sa.String(255), nullable=True),
        sa.Column("ip_address", sa.String(50), nullable=True),
        sa.Column("country", sa.String(100), nullable=True),
        sa.Column("hwid", sa.String(255), nullable=True),
        sa.Column("os", sa.String(255), nullable=True),
        sa.Column("cpu", sa.String(255), nullable=True),
        sa.Column("gpu", sa.String(255), nullable=True),
        sa.Column("ram", sa.String(50), nullable=True),
        sa.Column("timezone", sa.String(100), nullable=True),
        sa.Column("language", sa.String(50), nullable=True),
        sa.Column("screen_size", sa.String(50), nullable=True),
        sa.Column("log_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("stealer_name", sa.String(100), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    # The unique=True on extraction_job_id generates the constraint automatically.
    # Only add non-unique indexes for frequently-queried columns.
    op.create_index("ix_system_info_country", "system_info", ["country"])
    op.create_index("ix_system_info_stealer_name", "system_info", ["stealer_name"])


def downgrade() -> None:
    op.drop_index("ix_system_info_stealer_name", "system_info")
    op.drop_index("ix_system_info_country", "system_info")
    op.drop_table("system_info")
