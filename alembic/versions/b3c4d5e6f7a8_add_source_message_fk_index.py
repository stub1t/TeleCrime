"""Restore ix_parsed_credentials_source_message_id (FK ON DELETE SET NULL).

parsed_credentials.source_message_id references messages(id) ON DELETE SET
NULL. PostgreSQL implements that FK with a delete trigger that runs
``UPDATE parsed_credentials SET source_message_id = NULL WHERE
source_message_id = $1`` for every deleted parent row; without an index on the
child column each deleted message full-scans the ~353M-row table. The same
applies transitively to ``DELETE FROM conversations`` because messages cascade
from conversations.

migration d4e5f6a7b8c9 created ix_parsed_credentials_source_message_id and no
migration ever drops it, so fresh migration-built databases already have it.
The 2026-09 disaster-recovery rebuild recreated parsed_credentials from the
heap scan/COPY without that index, so the live database has the FK but not its
index. The model still declares index=True (see models/credential.py), meaning
create_all-based databases have it too.

Only this column is indexed: parsed_credentials.source_conversation_id's index
was deliberately dropped by x4y5z6a7b8c9 (zero scans, 1.5GB) and
web/app.py._ensure_stats_indexes explicitly refuses to recreate it. No
production code path deletes conversations or messages today, so no other FK
index is warranted.

CREATE INDEX CONCURRENTLY cannot run inside a transaction; use Alembic's
autocommit_block like i9j0k1l2m3n4 does. IF NOT EXISTS keeps this a no-op on
databases that still have the index.

Revision ID: b3c4d5e6f7a8
Revises: f0e1d2c3b4a5
Create Date: 2026-09-14 00:00:00.000000
"""

import sqlalchemy as sa

from alembic import op

revision = "b3c4d5e6f7a8"
down_revision = "f0e1d2c3b4a5"
branch_labels = None
depends_on = None

_INDEX_NAME = "ix_parsed_credentials_source_message_id"


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        with op.get_context().autocommit_block():
            # 353M rows: never let a lock wait or the 5-minute server
            # statement_timeout abort the build (it just retries from scratch
            # and leaves an invalid index behind).
            op.execute(sa.text("SET lock_timeout = 0"))
            op.execute(sa.text("SET statement_timeout = 0"))
            op.execute(
                sa.text(
                    f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {_INDEX_NAME} "
                    "ON parsed_credentials (source_message_id)"
                )
            )
    else:
        op.create_index(
            _INDEX_NAME,
            "parsed_credentials",
            ["source_message_id"],
            unique=False,
            if_not_exists=True,
        )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        with op.get_context().autocommit_block():
            op.execute(sa.text("SET lock_timeout = 0"))
            op.execute(sa.text("SET statement_timeout = 0"))
            op.execute(
                sa.text(f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX_NAME}")
            )
    else:
        op.drop_index(
            _INDEX_NAME, table_name="parsed_credentials", if_exists=True
        )
