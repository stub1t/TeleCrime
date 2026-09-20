"""Tests for FTS helpers."""


from telecrime.fts import ensure_fts, fts_available, fts_count, fts_search
from telecrime.models import ParsedCredential


def _add_credential(
    session, *, domain: str, username: str, password: str, stealer: str
) -> ParsedCredential:
    cred = ParsedCredential(
        url=f"https://{domain}/login",
        domain=domain,
        username=username,
        password=password,
        stealer_type=stealer,
        credential_hash=ParsedCredential.compute_hash(domain, username, password),
    )
    session.add(cred)
    session.flush()
    return cred


def test_fts_search_applies_structured_filters(pg_session):
    """FTS filtering happens in SQL before limiting results."""
    expected = _add_credential(
        pg_session,
        domain="accounts.google.com",
        username="alice",
        password="secret",
        stealer="redline",
    )
    _add_credential(
        pg_session,
        domain="accounts.google.com",
        username="bob",
        password="secret",
        stealer="vidar",
    )
    pg_session.commit()

    assert ensure_fts(pg_session.bind) is True
    assert fts_available(pg_session.bind) is True

    ids = fts_search(
        pg_session,
        "google",
        columns=["domain"],
        limit=5,
        filters={"stealer": "redline"},
    )
    total = fts_count(
        pg_session,
        "google",
        columns=["domain"],
        filters={"stealer": "redline"},
    )

    assert ids == [expected.id]
    assert total == 1


def test_ensure_fts_rebuild_creates_valid_indexes(pg_engine):
    """rebuild must leave valid trigram indexes usable by the planner.

    On PostgreSQL the rebuild runs CREATE INDEX CONCURRENTLY (autocommit):
    the old single-transaction path held a SHARE lock on parsed_credentials
    for the whole build, blocking every pipeline INSERT for hours.
    """
    from sqlalchemy import text

    from telecrime.fts import _PG_TRGM_INDEXES

    assert ensure_fts(pg_engine, rebuild=True) is True
    assert fts_available(pg_engine) is True

    with pg_engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT c.relname FROM pg_class c "
                "JOIN pg_index i ON i.indexrelid = c.oid "
                "WHERE c.relname = ANY(:names) AND i.indisvalid"
            ),
            {"names": list(_PG_TRGM_INDEXES)},
        ).fetchall()
    assert {row[0] for row in rows} == set(_PG_TRGM_INDEXES)


def test_fts_available_ignores_invalid_index(pg_engine):
    """An interrupted CONCURRENTLY build leaves an invalid index that the
    planner ignores; fts_available must not claim search is usable."""
    from sqlalchemy import text

    assert ensure_fts(pg_engine, rebuild=True) is True
    with pg_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE pg_index SET indisvalid = false "
                "WHERE indexrelid = 'ix_pc_username_trgm'::regclass"
            )
        )
    try:
        assert fts_available(pg_engine) is False
    finally:
        with pg_engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE pg_index SET indisvalid = true "
                    "WHERE indexrelid = 'ix_pc_username_trgm'::regclass"
                )
            )
