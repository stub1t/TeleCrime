"""Add indexes for credential query performance

Revision ID: d4e5f6a7b8c9
Revises: c3d7a8b9e1f2
Create Date: 2026-02-10 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'd4e5f6a7b8c9'
down_revision: Union[str, None] = 'c3d7a8b9e1f2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        'ix_parsed_credentials_username',
        'parsed_credentials',
        ['username'],
    )
    op.create_index(
        'ix_parsed_credentials_stealer_type',
        'parsed_credentials',
        ['stealer_type'],
    )
    op.create_index(
        'ix_parsed_credentials_application',
        'parsed_credentials',
        ['application'],
    )
    op.create_index(
        'ix_parsed_credentials_source_archive',
        'parsed_credentials',
        ['source_archive'],
    )
    op.create_index(
        'ix_parsed_credentials_source_message_id',
        'parsed_credentials',
        ['source_message_id'],
    )


def downgrade() -> None:
    op.drop_index('ix_parsed_credentials_source_message_id', table_name='parsed_credentials')
    op.drop_index('ix_parsed_credentials_source_archive', table_name='parsed_credentials')
    op.drop_index('ix_parsed_credentials_application', table_name='parsed_credentials')
    op.drop_index('ix_parsed_credentials_stealer_type', table_name='parsed_credentials')
    op.drop_index('ix_parsed_credentials_username', table_name='parsed_credentials')
