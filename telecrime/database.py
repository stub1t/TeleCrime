"""Database session and engine management (PostgreSQL)."""

import weakref
from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.dialects.postgresql import insert as _pg_insert
from sqlalchemy.orm import Session, sessionmaker

from telecrime.models.base import Base

_session_factories: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()


def get_dialect_insert(session):
    """Return the PostgreSQL INSERT constructor (supports on_conflict_*)."""
    del session
    return _pg_insert


def get_engine(database_url: str | None = None):
    """Create the database engine.

    Production: always PostgreSQL (the only dialect the SQL in this codebase
    targets). SQLite URLs are accepted as a backdoor for test fixtures that
    need an isolated in-memory DB; production code paths emit PG-only SQL and
    will fail on SQLite.
    """
    if not database_url:
        raise RuntimeError(
            "database_url is required. Set TELECRIME_DATABASE_URL "
            "(postgresql://...)."
        )
    if database_url.startswith("sqlite:"):
        return create_engine(
            database_url,
            echo=False,
            connect_args={"check_same_thread": False},
        )
    return create_engine(
        database_url,
        echo=False,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
    )


def get_session_factory(engine) -> sessionmaker[Session]:
    """Create session factory bound to engine."""
    return sessionmaker(bind=engine, expire_on_commit=False)


@contextmanager
def get_session(engine) -> Generator[Session, None, None]:
    """Context manager for database sessions with automatic commit/rollback."""
    factory = _session_factories.get(engine)
    if factory is None:
        factory = get_session_factory(engine)
        _session_factories[engine] = factory
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        try:
            session.rollback()
        except Exception:
            pass
        raise
    finally:
        session.close()


def init_db(engine) -> None:
    """Create all tables. Use Alembic for production migrations."""
    from telecrime import models as _models  # noqa: F401

    for model_name in _models.__all__:
        getattr(_models, model_name)
    Base.metadata.create_all(bind=engine)


def ensure_runtime_schema(engine) -> list[str]:
    """Apply additive, forward-compatible schema repairs.

    Limited to nullable/defaulted additions safe to run when Alembic is
    unavailable.
    """
    inspector = inspect(engine)
    changes: list[str] = []

    def has_table(name: str) -> bool:
        return name in inspector.get_table_names()

    def columns(table: str) -> set[str]:
        return {col["name"] for col in inspector.get_columns(table)}

    with engine.begin() as conn:
        soft_hash_ready = False
        watchlist_alert_ready = False
        if has_table("parsed_credentials"):
            parsed_cols = columns("parsed_credentials")
            if "soft_credential_hash" not in parsed_cols:
                conn.execute(
                    text("ALTER TABLE parsed_credentials ADD COLUMN soft_credential_hash VARCHAR(64)")
                )
                changes.append("added parsed_credentials.soft_credential_hash")
                parsed_cols.add("soft_credential_hash")
            soft_hash_ready = "soft_credential_hash" in parsed_cols
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS "
                    "ix_parsed_credentials_soft_credential_hash "
                    "ON parsed_credentials (soft_credential_hash)"
                )
            )

        if has_table("watchlist_items"):
            watchlist_cols = columns("watchlist_items")
            if "last_alerted_at" not in watchlist_cols:
                conn.execute(
                    text("ALTER TABLE watchlist_items ADD COLUMN last_alerted_at TIMESTAMP")
                )
                changes.append("added watchlist_items.last_alerted_at")
                watchlist_cols.add("last_alerted_at")
            if "last_alerted_count" not in watchlist_cols:
                conn.execute(
                    text(
                        "ALTER TABLE watchlist_items "
                        "ADD COLUMN last_alerted_count INTEGER NOT NULL DEFAULT 0"
                    )
                )
                changes.append("added watchlist_items.last_alerted_count")
                watchlist_cols.add("last_alerted_count")
            watchlist_alert_ready = {
                "last_alerted_at",
                "last_alerted_count",
            } <= watchlist_cols

        if has_table("alembic_version") and soft_hash_ready and watchlist_alert_ready:
            current = conn.execute(text("SELECT version_num FROM alembic_version")).scalar()
            if current in {"l2m3n4o5p6q7", "m3n4o5p6q7r8"}:
                conn.execute(
                    text("UPDATE alembic_version SET version_num = 'n4o5p6q7r8s9'")
                )
                changes.append("advanced alembic_version to n4o5p6q7r8s9")

    return changes
