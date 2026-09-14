"""Database session and engine management (PostgreSQL)."""

import os
import weakref
from collections.abc import Generator
from contextlib import contextmanager
from functools import lru_cache

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.dialects.postgresql import insert as _pg_insert
from sqlalchemy.orm import Session, sessionmaker

from telecrime.models.base import Base

_session_factories: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()


def _pg_connect_args() -> dict[str, str]:
    """libpq connect args shared by every PostgreSQL engine.

    ``application_name`` mirrors the watchdog contract: the scheduler tags the
    pipeline subprocess with ``PGAPPNAME=telecrime-pipeline`` and
    ``scripts/unattended-watchdog.sh`` scopes its ``pg_stat_activity`` hang
    probe to that name. Passing it through ``connect_args`` keeps the tag even
    if a URL/launcher strips the environment, and gives every other process a
    non-empty name (``telecrime``) instead of libpq's blank default.

    ``statement_timeout`` is deliberately not set here: the compose Postgres
    sets the 5-minute server default, and per-session ``SET``/``RESET`` in
    ``web/app.py`` / ``pipeline/parse.py`` must remain the single source of
    truth for bounded vs unbounded statements.
    """
    return {"application_name": os.environ.get("PGAPPNAME") or "telecrime"}


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
        # Per-engine budget 5 + 10 overflow. The web process runs an app engine
        # plus one shared cached engine for its three background workers
        # (get_cached_engine); the worker runs one engine and the pipeline
        # subprocess one more. The pipeline's peak is its main session plus up
        # to 3 prefetch downloads plus TELECRIME_READY_GROUP_CONCURRENCY group
        # tasks (~5-7), so 15/engine leaves headroom while keeping the
        # cross-process total (<= ~60) below PG's default max_connections=100.
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        connect_args=_pg_connect_args(),
    )


@lru_cache(maxsize=8)
def get_cached_engine(database_url: str):
    """URL-keyed engine with pool reuse.

    The web process creates 4 engines (app + 3 background workers); without
    caching that is up to 60 pooled connections against PG's default
    max_connections=100.
    """
    return get_engine(database_url)


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
    except BaseException:
        # BaseException (not Exception) so KeyboardInterrupt / asyncio
        # CancelledError also roll back explicitly before the connection is
        # returned to the pool; close() in finally would roll back anyway, but
        # this guarantees the pool never sees a checked out transaction even if
        # close() itself fails.
        try:
            session.rollback()
        except Exception:
            pass
        raise
    finally:
        session.close()


def init_db(engine) -> None:
    """Create all tables. Use Alembic for production migrations."""
    from telecrime import models as _models

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
            # NOTE: no CREATE INDEX for soft_credential_hash here — migration
            # s9t0u1v2w3x4 deliberately dropped the 13 GB B-tree (0 index scans
            # ever; INSERT maintenance cost). Re-creating it (non-CONCURRENTLY,
            # blocking writes on 300M+ rows) would undo that on every
            # init/repair.

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
