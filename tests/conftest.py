"""Pytest configuration and fixtures.

Test fixtures use an isolated in-memory SQLite database for speed. Production
code paths are PostgreSQL-only; SQLite is allowed by `get_engine` solely so
these fixtures can construct ephemeral test engines. See test_database.py for
PG-backed coverage of the database module proper.

PG-dependent tests (FTS search, VACUUM, ...) use the `pg_engine` fixture,
which connects to a real PostgreSQL instance when `TELECRIME_TEST_DATABASE_URL`
is set (the GitHub Actions `tests.yml` workflow starts a postgres service and
exports it). Without it, those tests skip.
"""

import fcntl
import hashlib
import os
import tempfile
import time
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from telecrime.config import Config
from telecrime.models import Base

PG_URL = os.environ.get("TELECRIME_TEST_DATABASE_URL", "")
_PG_TEST_LOCK_WAIT_SECONDS = 900.0


def _acquire_pg_test_lock():
    """Serialize destructive PG fixtures across pytest processes.

    Two pytest runs sharing one ``telecrime_test`` database (e.g. a developer
    and CI, or concurrent agents) otherwise terminate each other's backends
    with ``pg_terminate_backend`` and DROP/CREATE the schema under a live test,
    producing "server closed the connection unexpectedly". The lock is held
    for the duration of one PG test; a bounded wait turns a wedged holder into
    a clear error instead of a hang.
    """
    digest = hashlib.sha256(PG_URL.encode()).hexdigest()[:16]
    lock_path = Path(tempfile.gettempdir()) / f"telecrime-pytest-pg-{digest}.lock"
    handle = lock_path.open("w")
    deadline = time.monotonic() + _PG_TEST_LOCK_WAIT_SECONDS
    while True:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return handle
        except BlockingIOError:
            if time.monotonic() >= deadline:
                handle.close()
                raise RuntimeError(
                    "Another pytest run has held the telecrime_test lock for "
                    f"{_PG_TEST_LOCK_WAIT_SECONDS:.0f}s ({lock_path}); refusing "
                    "to reset the schema under it."
                ) from None
            time.sleep(0.5)


def _assert_safe_test_database(url: str) -> None:
    """Refuse to run destructive fixtures against a non-test database.

    ``pg_engine`` executes ``DROP SCHEMA public CASCADE``. On 2026-09-10 a
    pytest run with ``TELECRIME_TEST_DATABASE_URL`` pointed at the production
    database wiped the live dataset. Only databases whose name contains
    "test" (e.g. CI's ``telecrime_test``) are accepted; set
    ``TELECRIME_ALLOW_DESTRUCTIVE_TESTS=1`` to override deliberately.
    """
    from sqlalchemy.engine import make_url

    db_name = (make_url(url).database or "").lower()
    if "test" in db_name:
        return
    if os.environ.get("TELECRIME_ALLOW_DESTRUCTIVE_TESTS") == "1":
        return
    raise RuntimeError(
        f"Refusing to DROP SCHEMA public in database {db_name!r}. "
        "TELECRIME_TEST_DATABASE_URL must point at a dedicated test database "
        "whose name contains 'test' (e.g. telecrime_test)."
    )


@pytest.fixture()
def pg_engine():
    """PostgreSQL engine, reset before each test (skipped if no PG URL set).

    Drops and recreates the schema on a dedicated autocommit connection so
    every test starts empty and the trgm extension is present. Leftover idle
    connections from `create_app` are terminated first to avoid lock blocks.
    A cross-process file lock serializes the destructive reset so two pytest
    runs sharing the test database do not kill each other's connections.
    """
    if not PG_URL:
        pytest.skip("TELECRIME_TEST_DATABASE_URL not set — PG-only test skipped")
    _assert_safe_test_database(PG_URL)
    lock_handle = _acquire_pg_test_lock()
    engine = create_engine(PG_URL, pool_pre_ping=True)
    try:
        admin = create_engine(PG_URL, isolation_level="AUTOCOMMIT")
        try:
            with admin.connect() as conn:
                conn.execute(
                    text(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        "WHERE datname = current_database() AND pid <> pg_backend_pid()"
                    )
                )
                conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
                conn.execute(text("CREATE SCHEMA public"))
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        finally:
            admin.dispose()
        Base.metadata.create_all(bind=engine)
        # The trigram GIN indexes live in migration i9j0k1l2m3n4, not in the
        # models, so create_all leaves them out and fts_available() (correctly)
        # reports FTS as unavailable. Recreate them on the empty schema so the
        # PG-backed FTS tests exercise the production search path.
        from telecrime.fts import _PG_TRGM_INDEXES

        with engine.begin() as conn:
            for name, column in _PG_TRGM_INDEXES.items():
                conn.execute(
                    text(
                        f"CREATE INDEX IF NOT EXISTS {name} ON parsed_credentials "
                        f"USING GIN ({column} gin_trgm_ops)"
                    )
                )
        yield engine
    finally:
        engine.dispose()
        lock_handle.close()


@pytest.fixture
def pg_session(pg_engine) -> Session:
    """A session against the PostgreSQL engine (requires pg_engine)."""
    SessionLocal = sessionmaker(bind=pg_engine, expire_on_commit=False)
    session = SessionLocal()
    yield session
    session.close()


@pytest.fixture(autouse=True)
def _isolate_runtime_state_files(tmp_path, monkeypatch):
    """Point runtime state files at per-test temp paths.

    Without this, scheduler tests read the developer's real
    ``data/pipeline_shutdown_request.json``; ``_pipeline_lock_is_held()`` then
    probes a MagicMock ``data_dir`` and ``Path`` coercion creates
    ``MagicMock/mock.data_dir/<id>/`` junk dirs in the repo root. The progress
    file is isolated symmetrically: a live host pipeline can leave
    ``data/pipeline_progress.json`` with ``running: true``, which would make
    tests that read it depend on host state instead of their own fixtures.

    The scheduler status / pipeline PID / data-dir variables are isolated for
    the same reason: the host worker keeps ``data/scheduler_status.json`` and
    ``data/pipeline.pid`` current, so a test that forgets an explicit override
    would otherwise read (or overwrite) live host state in the repo root.
    Tests that assert the ``TELECRIME_DATA_DIR`` fallback delete these
    explicitly via ``monkeypatch``.
    """
    monkeypatch.setenv("TELECRIME_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv(
        "TELECRIME_SHUTDOWN_REQUEST_FILE",
        str(tmp_path / "pipeline_shutdown_request.json"),
    )
    monkeypatch.setenv(
        "TELECRIME_PROGRESS_FILE",
        str(tmp_path / "pipeline_progress.json"),
    )
    monkeypatch.setenv(
        "TELECRIME_STATUS_FILE",
        str(tmp_path / "scheduler_status.json"),
    )
    monkeypatch.setenv(
        "TELECRIME_PIPELINE_PID_FILE",
        str(tmp_path / "pipeline.pid"),
    )


def _reset_module_caches() -> None:
    """Clear process-global module caches so tests cannot leak state.

    Telecrime keeps a few path/URL-keyed caches at module scope (parser
    classification, schema introspection, password files). In one pytest
    process those survive across tests, so whether a test sees a cache hit
    depends on which tests ran before it. This is only cleared for modules
    that are already imported: a module imported for the first time by the
    current test starts with empty caches by construction.
    """
    import sys

    parser = sys.modules.get("telecrime.stealer.parser")
    if parser is not None:
        parser._COMBO_CLASS_CACHE.clear()

    parse_mod = sys.modules.get("telecrime.pipeline.parse")
    if parse_mod is not None:
        # Tri-state hash64 probe; a stale True/False from an earlier test's
        # engine must not decide the dedup path for this test's engine.
        parse_mod._HAS_HASH64 = None
        parse_mod._HAS_HASH64_RETRY_AT = 0.0
        # Same class of cache for the trigram-index probe: a result cached
        # against one test's schema must not decide gin_clean_pending_list()
        # behavior for the next test's schema.
        parse_mod._HAS_TRGM_INDEXES = None

    database = sys.modules.get("telecrime.database")
    if database is not None:
        # URL-keyed engine cache: tests share the PG URL, so without a reset a
        # later test can reuse an engine created before the schema was dropped
        # and recreated by the pg_engine fixture.
        database.get_cached_engine.cache_clear()

    extractor = sys.modules.get("telecrime.passwords.extractor")
    if extractor is not None:
        extractor._password_file_cache.clear()

    fts = sys.modules.get("telecrime.fts")
    if fts is not None:
        fts._column_cache.clear()

    scheduler = sys.modules.get("telecrime.scheduler")
    if scheduler is not None:
        scheduler._soft_hash_col_cache.clear()

    web_app = sys.modules.get("telecrime.web.app")
    if web_app is not None:
        web_app._db_column_cache.clear()
        web_app._cred_agg_allowed_cache.clear()

    progress = sys.modules.get("telecrime.pipeline.progress")
    if progress is not None:
        progress._NOTE_OVERRIDES.clear()
        progress._last_progress_write_warn.clear()


@pytest.fixture(autouse=True)
def _isolate_module_caches():
    """Reset process-global caches before each test (no ordering coupling)."""
    _reset_module_caches()
    yield


@pytest.fixture
def in_memory_engine():
    """In-memory SQLite engine with schema created (test fixture only)."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(bind=engine)
    yield engine
    engine.dispose()


@pytest.fixture
def session(in_memory_engine) -> Session:
    """Create a database session for testing."""
    SessionLocal = sessionmaker(bind=in_memory_engine, expire_on_commit=False)
    session = SessionLocal()
    yield session
    session.close()


@pytest.fixture
def test_config(tmp_path) -> Config:
    """Test configuration with temporary directories."""
    config = Config(
        database_url=f"sqlite:///{tmp_path / 'test.db'}",
        data_dir=tmp_path / "data",
        downloads_dir=tmp_path / "downloads",
        extracted_dir=tmp_path / "extracted",
    )
    config.ensure_directories()
    return config
