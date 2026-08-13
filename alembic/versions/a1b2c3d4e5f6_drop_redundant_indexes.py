"""Drop redundant duplicate indexes

Revision ID: a1b2c3d4e5f6
Revises: fe9a7c2b1d3e
Create Date: 2026-02-12 11:05:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, None] = "fe9a7c2b1d3e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    is_sqlite = bind.dialect.name == "sqlite"

    def drop_index(name: str, table: str) -> None:
        if is_sqlite:
            op.execute(f"DROP INDEX IF EXISTS {name}")
        else:
            op.drop_index(name, table_name=table, if_exists=True)

    # Keep the *_id / is_archive_candidate / *_discovered_at variants.
    drop_index("ix_download_artifacts_attachment", "download_artifacts")
    drop_index("ix_file_attachments_message", "file_attachments")
    drop_index("ix_file_attachments_archive_candidate", "file_attachments")
    drop_index("ix_parsed_credentials_source_conversation", "parsed_credentials")
    drop_index("ix_telegram_channels_discovered", "telegram_channels")


def downgrade() -> None:
    bind = op.get_bind()
    is_sqlite = bind.dialect.name == "sqlite"

    def create_index(name: str, table: str, columns: list[str]) -> None:
        if is_sqlite:
            cols = ", ".join(columns)
            op.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {table} ({cols})")
        else:
            op.create_index(name, table, columns)

    create_index("ix_download_artifacts_attachment", "download_artifacts", ["attachment_id"])
    create_index("ix_file_attachments_message", "file_attachments", ["message_id"])
    create_index("ix_file_attachments_archive_candidate", "file_attachments", ["is_archive_candidate"])
    create_index("ix_parsed_credentials_source_conversation", "parsed_credentials", ["source_conversation_id"])
    create_index("ix_telegram_channels_discovered", "telegram_channels", ["discovered_at"])
