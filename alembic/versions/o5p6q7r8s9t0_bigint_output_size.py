"""Change extracted_outputs.output_size to BigInteger.

Files larger than 2 GB (e.g. 4 GB ULP dumps) caused NumericValueOutOfRange
on INSERT because the column was Integer (INT4).

Revision ID: o5p6q7r8s9t0
Revises: n4o5p6q7r8s9
Create Date: 2026-05-08 10:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "o5p6q7r8s9t0"
down_revision = "n4o5p6q7r8s9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("extracted_outputs") as batch_op:
        batch_op.alter_column(
            "output_size",
            existing_type=sa.Integer(),
            type_=sa.BigInteger(),
            existing_nullable=True,
        )


def downgrade() -> None:
    with op.batch_alter_table("extracted_outputs") as batch_op:
        batch_op.alter_column(
            "output_size",
            existing_type=sa.BigInteger(),
            type_=sa.Integer(),
            existing_nullable=True,
        )
