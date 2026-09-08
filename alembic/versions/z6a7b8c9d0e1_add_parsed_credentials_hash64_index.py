"""Create ix_pc_hash64 compact dedup index on parsed_credentials.

The two-stage dedup in the parse pipeline (anti-join prefilter + ON CONFLICT
backstop) references the ix_pc_hash64 expression index, but no migration or
runtime ensure ever created it — every parse INSERT chunk silently fell back
to the left(credential_hash, 32) prefilter, which seq-scans parsed_credentials
per chunk and times out on 200M+ row tables.

Revision ID: z6a7b8c9d0e1
Revises: y5z6a7b8c9d0
Create Date: 2026-09-08 00:00:00.000000
"""

import sqlalchemy as sa

from alembic import op

revision = "z6a7b8c9d0e1"
down_revision = "y5z6a7b8c9d0"
branch_labels = None
depends_on = None


_HASH64_EXPR = (
    "(CAST((CAST((chr(120) || substring(credential_hash, 1, 16)) AS bit(64))) AS bigint))"
)


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        bind.execute(sa.text("COMMIT"))
        bind.execute(sa.text("SET lock_timeout = 0"))
        bind.execute(sa.text("SET statement_timeout = 0"))
        bind.execute(
            sa.text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_pc_hash64 "
                f"ON parsed_credentials ({_HASH64_EXPR})"
            )
        )
        bind.execute(sa.text("BEGIN"))
    else:
        # PostgreSQL-only expression index; the parse pipeline's
        # _has_hash64_index check reports False on SQLite and uses the fallback.
        return


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    bind.execute(sa.text("COMMIT"))
    bind.execute(sa.text("SET lock_timeout = 0"))
    bind.execute(sa.text("SET statement_timeout = 0"))
    bind.execute(sa.text("DROP INDEX CONCURRENTLY IF EXISTS ix_pc_hash64"))
    bind.execute(sa.text("BEGIN"))
