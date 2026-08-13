"""Clean markdown formatting from password candidates.

Revision ID: l2m3n4o5p6q7
Revises: k1l2m3n4o5p6
Create Date: 2026-03-27 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "l2m3n4o5p6q7"
down_revision: Union[str, None] = "k1l2m3n4o5p6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    import re
    conn = op.get_bind()

    # Fetch rows with leading/trailing backticks
    rows = conn.execute(sa.text(
        "SELECT id, value FROM password_candidates "
        "WHERE value LIKE '`%' OR value LIKE '%`'"
    )).fetchall()
    for row in rows:
        cleaned = row[1].strip("`")
        conn.execute(sa.text(
            "UPDATE password_candidates SET value = :v WHERE id = :id"
        ), {"v": cleaned, "id": row[0]})

    # Delete rows that are pure separator lines (3+ repeated =, -, _, or spaces)
    sep_pattern = re.compile(r'^[=\-_ ]{3,}$')
    junk_rows = conn.execute(sa.text(
        "SELECT id, value FROM password_candidates"
    )).fetchall()
    junk_ids = [r[0] for r in junk_rows if sep_pattern.match(r[1])]
    if junk_ids:
        conn.execute(sa.text(
            "DELETE FROM password_candidates WHERE id IN :ids"
        ).bindparams(sa.bindparam("ids", expanding=True)), {"ids": junk_ids})


def downgrade() -> None:
    pass
