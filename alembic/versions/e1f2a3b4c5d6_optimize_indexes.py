"""Optimize indexes: drop low-selectivity, add compound index for parse-check.

Revision ID: e1f2a3b4c5d6
Revises: c3d7a8b9e1f2
Create Date: 2026-03-09

Changes:
- Drop ix_parsed_credentials_stealer_type (6 unique values in 15M+ rows — useless for filtering)
- Drop ix_parsed_credentials_application (mostly NULL, ~50 unique values — low selectivity)
- Add ix_parsed_credentials_job_file compound index on (extraction_job_id, source_file)
  for O(1) "already parsed this file?" check in ParseStage
"""
from alembic import op


revision = "e1f2a3b4c5d6"
down_revision = "b2c3d4e5f6a7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop low-selectivity single-column indexes that waste space and slow writes.
    # stealer_type: only 6 unique values in 15M+ rows — useless for selectivity.
    # application: mostly NULL, ~50 unique values — not worth the write overhead.
    op.drop_index("ix_parsed_credentials_stealer_type", table_name="parsed_credentials", if_exists=True)
    op.drop_index("ix_parsed_credentials_application", table_name="parsed_credentials", if_exists=True)

    # Compound index for the "already parsed this file?" check in ParseStage.
    # SELECT ... WHERE extraction_job_id = ? AND source_file = ?  → O(1) vs full scan.
    op.create_index(
        "ix_parsed_credentials_job_file",
        "parsed_credentials",
        ["extraction_job_id", "source_file"],
        if_not_exists=True,
    )


def downgrade() -> None:
    op.drop_index("ix_parsed_credentials_job_file", table_name="parsed_credentials", if_exists=True)
    op.create_index("ix_parsed_credentials_stealer_type", "parsed_credentials", ["stealer_type"])
    op.create_index("ix_parsed_credentials_application", "parsed_credentials", ["application"])
