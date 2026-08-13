"""Add credential_hash column and deduplicate

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-02-13 12:00:00.000000

"""
from typing import Sequence, Union
import hashlib

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b2c3d4e5f6a7"
down_revision: Union[str, None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

BATCH_SIZE = 10000


def _compute_hash(url: str, username: str, password: str) -> str:
    raw = f"{url}|{username}|{password}"
    return hashlib.sha256(raw.encode("utf-8", errors="replace")).hexdigest()


def upgrade() -> None:
    # 1. Add nullable credential_hash column
    op.add_column(
        "parsed_credentials",
        sa.Column("credential_hash", sa.String(64), nullable=True),
    )

    # 2. Backfill hashes in batches (SQLite has no native SHA256)
    conn = op.get_bind()

    while True:
        rows = conn.execute(
            sa.text(
                "SELECT id, url, username, password FROM parsed_credentials "
                "WHERE credential_hash IS NULL LIMIT :batch"
            ),
            {"batch": BATCH_SIZE},
        ).fetchall()

        if not rows:
            break

        for row_id, url, username, password in rows:
            h = _compute_hash(url or "", username or "", password or "")
            conn.execute(
                sa.text(
                    "UPDATE parsed_credentials SET credential_hash = :h WHERE id = :id"
                ),
                {"h": h, "id": row_id},
            )

    # 3. Delete duplicate rows (keep lowest id per hash)
    conn.execute(
        sa.text(
            "DELETE FROM parsed_credentials WHERE id NOT IN ("
            "  SELECT MIN(id) FROM parsed_credentials GROUP BY credential_hash"
            ")"
        )
    )

    # 4. Create unique index
    op.create_index(
        "ix_parsed_credentials_credential_hash",
        "parsed_credentials",
        ["credential_hash"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_parsed_credentials_credential_hash", table_name="parsed_credentials")
    op.drop_column("parsed_credentials", "credential_hash")
