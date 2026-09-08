"""Drop write-only extractor_name/extractor_version columns from extraction_jobs.

The columns were removed from the ORM model (dead code cleanup): they were
never read or written by any code. The DB columns stayed NOT NULL, so every
new extraction_jobs INSERT now violates the constraint. Dropping them matches
the model.

Revision ID: f0e1d2c3b4a5
Revises: z6a7b8c9d0e1
Create Date: 2026-09-08 23:10:00.000000
"""

import sqlalchemy as sa

from alembic import op

revision = "f0e1d2c3b4a5"
down_revision = "z6a7b8c9d0e1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    cols = {
        row[0]
        for row in bind.execute(
            sa.text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'extraction_jobs'"
            )
        )
    }
    for col in ("extractor_name", "extractor_version"):
        if col in cols:
            op.drop_column("extraction_jobs", col)


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    op.add_column(
        "extraction_jobs",
        sa.Column("extractor_name", sa.String(length=50), nullable=True),
    )
    op.add_column(
        "extraction_jobs",
        sa.Column("extractor_version", sa.String(length=50), nullable=True),
    )