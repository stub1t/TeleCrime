"""Add indexes for recency ordering

Revision ID: fe9a7c2b1d3e
Revises: d4e5f6a7b8c9
Create Date: 2026-02-12 10:30:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "fe9a7c2b1d3e"
down_revision: Union[str, None] = "d4e5f6a7b8c9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    is_sqlite = bind.dialect.name == "sqlite"

    def create_index(name: str, table: str, columns: list[str]) -> None:
        if is_sqlite:
            cols = ", ".join(columns)
            op.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {table} ({cols})")
        else:
            op.create_index(name, table, columns)

    create_index("ix_parsed_credentials_created_at", "parsed_credentials", ["created_at"])
    create_index("ix_file_attachments_created_at", "file_attachments", ["created_at"])
    create_index("ix_download_artifacts_created_at", "download_artifacts", ["created_at"])
    create_index("ix_extracted_outputs_created_at", "extracted_outputs", ["created_at"])
    create_index("ix_conversations_created_at", "conversations", ["created_at"])
    create_index("ix_telegram_channels_discovered_at", "telegram_channels", ["discovered_at"])


def downgrade() -> None:
    bind = op.get_bind()
    is_sqlite = bind.dialect.name == "sqlite"

    def drop_index(name: str, table: str) -> None:
        if is_sqlite:
            op.execute(f"DROP INDEX IF EXISTS {name}")
        else:
            op.drop_index(name, table_name=table)

    drop_index("ix_telegram_channels_discovered_at", "telegram_channels")
    drop_index("ix_conversations_created_at", "conversations")
    drop_index("ix_extracted_outputs_created_at", "extracted_outputs")
    drop_index("ix_download_artifacts_created_at", "download_artifacts")
    drop_index("ix_file_attachments_created_at", "file_attachments")
    drop_index("ix_parsed_credentials_created_at", "parsed_credentials")
