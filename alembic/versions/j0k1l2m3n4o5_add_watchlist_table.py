"""Add watchlist_items table — migrate watchlist from separate SQLite file to main DB.

Revision ID: j0k1l2m3n4o5
Revises: i9j0k1l2m3n4
Create Date: 2026-03-23

"""

from typing import Union

from alembic import op
import sqlalchemy as sa

revision: str = "j0k1l2m3n4o5"
down_revision: Union[str, None] = "i9j0k1l2m3n4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_tables = inspector.get_table_names()

    if "watchlist_items" not in existing_tables:
        op.create_table(
            "watchlist_items",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("label", sa.String(500), nullable=False),
            sa.Column("query", sa.String(1000), nullable=False),
            sa.Column("match_type", sa.String(20), nullable=False, server_default="any"),
            sa.Column("enabled", sa.Boolean, nullable=False, server_default="1"),
            sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_known_count", sa.Integer, nullable=False, server_default="0"),
            sa.Column("new_count", sa.Integer, nullable=False, server_default="0"),
            sa.Column("last_viewed_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
        )
        op.create_index("ix_watchlist_items_enabled", "watchlist_items", ["enabled"])
    else:
        # Table already exists — ensure updated_at column is present (backfill schema)
        existing_cols = {c["name"] for c in inspector.get_columns("watchlist_items")}
        if "updated_at" not in existing_cols:
            # SQLite ADD COLUMN only allows constant literals as defaults.
            # Use nullable=True and backfill with created_at value.
            op.add_column(
                "watchlist_items",
                sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
            )
            op.execute(
                sa.text("UPDATE watchlist_items SET updated_at = created_at WHERE updated_at IS NULL")
            )
        existing_indexes = {ix["name"] for ix in inspector.get_indexes("watchlist_items")}
        if "ix_watchlist_items_enabled" not in existing_indexes:
            op.create_index("ix_watchlist_items_enabled", "watchlist_items", ["enabled"])


def downgrade() -> None:
    op.drop_index("ix_watchlist_items_enabled", "watchlist_items")
    op.drop_table("watchlist_items")
