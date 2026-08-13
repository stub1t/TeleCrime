"""Add telegram_channel table

Revision ID: c3d7a8b9e1f2
Revises: 05fc44e0c2ea
Create Date: 2025-01-23 15:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c3d7a8b9e1f2'
down_revision: Union[str, None] = '05fc44e0c2ea'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'telegram_channels',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('platform_id', sa.BigInteger(), nullable=True),
        sa.Column('username', sa.String(255), nullable=True),
        sa.Column('title', sa.String(500), nullable=True),
        sa.Column('invite_link', sa.String(500), nullable=True),
        sa.Column('source', sa.String(50), nullable=False),
        sa.Column('discovered_at', sa.DateTime(), nullable=True),
        sa.Column('discovered_from', sa.String(500), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True, default=True),
        sa.Column('is_accessible', sa.Boolean(), nullable=True, default=True),
        sa.Column('is_subscribed', sa.Boolean(), nullable=True, default=False),
        sa.Column('last_checked', sa.DateTime(), nullable=True),
        sa.Column('check_error', sa.String(500), nullable=True),
        sa.Column('member_count', sa.Integer(), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('messages_seen', sa.Integer(), nullable=True, default=0),
        sa.Column('archives_seen', sa.Integer(), nullable=True, default=0),
        sa.Column('credentials_extracted', sa.Integer(), nullable=True, default=0),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_telegram_channels_platform_id', 'telegram_channels', ['platform_id'], unique=True)
    op.create_index('ix_telegram_channels_username', 'telegram_channels', ['username'], unique=False)
    op.create_index('ix_telegram_channels_username_lower', 'telegram_channels', ['username'], unique=False)


def downgrade() -> None:
    op.drop_index('ix_telegram_channels_username_lower', table_name='telegram_channels')
    op.drop_index('ix_telegram_channels_username', table_name='telegram_channels')
    op.drop_index('ix_telegram_channels_platform_id', table_name='telegram_channels')
    op.drop_table('telegram_channels')
