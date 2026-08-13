"""Full-text search for parsed_credentials (PostgreSQL pg_trgm)."""

from sqlalchemy import inspect, text

_PG_SEARCH_COLUMNS = ["domain", "username", "email_domain"]

# Schema introspection cache: (engine_url, table, column) -> bool.
_column_cache: dict[tuple[str, str, str], bool] = {}


def _has_column(session, table: str, column: str) -> bool:
    engine = session.get_bind()
    key = (str(engine.url), table, column)
    if key in _column_cache:
        return _column_cache[key]
    try:
        result = column in {
            col["name"] for col in inspect(engine).get_columns(table)
        }
    except Exception:
        result = False
    _column_cache[key] = result
    return result


def _soft_count_expr(alias: str = "pc", *, has_soft_hash: bool = True) -> str:
    """Count distinct credentials using the softer analytics grouping key."""
    if not has_soft_hash:
        return f"COUNT(DISTINCT COALESCE({alias}.credential_hash, CAST({alias}.id AS TEXT)))"
    return (
        f"COUNT(DISTINCT COALESCE({alias}.soft_credential_hash, "
        f"{alias}.credential_hash, CAST({alias}.id AS TEXT)))"
    )


def fts_available(engine) -> bool:
    """Return True when pg_trgm is installed."""
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm'")
            ).fetchone()
        return row is not None
    except Exception:
        return False


def ensure_fts(engine, rebuild: bool = False) -> bool:
    """Enable pg_trgm extension (GIN indexes created by Alembic migration)."""
    del rebuild  # PG: GIN indexes are managed by Alembic, no per-call rebuild
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        return True
    except Exception:
        return False


def fts_search(
    session,
    query: str,
    columns: list[str] | None = None,
    limit: int = 50,
    filters: dict[str, str] | None = None,
) -> list[int]:
    """Search credentials, returning matching row IDs.

    PostgreSQL ILIKE accelerated by GIN trigram indexes, ordered by id DESC.
    """
    tokens = query.split()
    if not tokens:
        return []
    _disable_pg_parallel_search(session)
    params: dict[str, object] = {"limit": limit, "branch_limit": max(500, limit)}
    if columns:
        where_parts, params = _pg_token_where(tokens, columns, filters)
        params["limit"] = limit
        sql = (
            "SELECT pc.id FROM parsed_credentials pc "
            f"WHERE {' AND '.join(where_parts)} "
            "ORDER BY pc.id DESC LIMIT :limit"
        )
    else:
        cte = _pg_candidate_cte(tokens, params)
        sql = (
            cte
            + "SELECT pc.id FROM matched_ids mi "
            + "JOIN parsed_credentials pc ON pc.id = mi.id "
            + "ORDER BY pc.id DESC LIMIT :limit"
        )
    params["limit"] = limit
    rows = session.execute(text(sql), params).fetchall()
    return [r[0] for r in rows]


def fts_count(
    session,
    query: str,
    columns: list[str] | None = None,
    filters: dict[str, str] | None = None,
) -> int:
    """Count credentials matching a query and optional structured filters."""
    tokens = query.split()
    if not tokens:
        return 0
    _disable_pg_parallel_search(session)
    has_soft_hash = _has_column(session, "parsed_credentials", "soft_credential_hash")
    select_cols = "pc.credential_hash, pc.id"
    if has_soft_hash:
        select_cols = "pc.soft_credential_hash, " + select_cols
    params: dict[str, object] = {"cap": 10_001, "branch_limit": 500}
    if columns:
        where_parts, params = _pg_token_where(tokens, columns, filters)
        params["cap"] = 10_001
        sql = (
            f"SELECT {_soft_count_expr(has_soft_hash=has_soft_hash)} FROM ("
            f"SELECT {select_cols} "
            "FROM parsed_credentials pc "
            f"WHERE {' AND '.join(where_parts)} "
            "LIMIT :cap"
            ") pc"
        )
    else:
        cte = _pg_candidate_cte(tokens, params)
        sql = (
            cte
            + f"SELECT {_soft_count_expr(has_soft_hash=has_soft_hash)} FROM ("
            + f"SELECT {select_cols} "
            + "FROM matched_ids mi "
            + "JOIN parsed_credentials pc ON pc.id = mi.id "
            + "ORDER BY pc.id DESC LIMIT :cap"
            + ") pc"
        )
    return session.execute(text(sql), params).scalar_one()


def _pg_token_where(
    tokens: list[str],
    search_cols: list[str],
    filters: dict[str, str] | None,
) -> tuple[list[str], dict[str, object]]:
    """Build WHERE parts and params for a PostgreSQL ILIKE token search."""
    where_parts: list[str] = []
    params: dict[str, object] = {}
    for i, token in enumerate(tokens):
        k = f"tok_{i}"
        params[k] = f"%{token}%"
        col_conds = " OR ".join(f"pc.{col} ILIKE :{k}" for col in search_cols)
        where_parts.append(f"({col_conds})")
    for filter_sql, filter_params in _fts_filter_sql(filters).values():
        where_parts.append(filter_sql)
        params.update(filter_params)
    return where_parts, params


def _disable_pg_parallel_search(session) -> None:
    session.execute(text("SET LOCAL max_parallel_workers_per_gather = 0"))
    # Cap search queries at 30s so a slow rare-term search never holds the
    # connection open indefinitely or starves bulk INSERT I/O.
    session.execute(text("SET LOCAL statement_timeout = '30s'"))


def _pg_single_token_candidates(token: str, params: dict[str, object]) -> str:
    params["tok_0"] = f"%{token}%"
    params.setdefault("branch_limit", 500)
    branches = [
        f"(SELECT id FROM parsed_credentials WHERE {col} ILIKE :tok_0 LIMIT :branch_limit)"
        for col in _PG_SEARCH_COLUMNS
    ]
    return "WITH matched_ids AS (" + " UNION ".join(branches) + ") "


def _pg_multi_token_candidates(tokens: list[str], params: dict[str, object]) -> str:
    branches: list[str] = []
    for token_idx, token in enumerate(tokens):
        param_name = f"tok_{token_idx}"
        params[param_name] = f"%{token}%"
        for col in _PG_SEARCH_COLUMNS:
            branches.append(
                "SELECT id, "
                f"{token_idx} AS token_idx "
                "FROM parsed_credentials "
                f"WHERE {col} ILIKE :{param_name}"
            )
    return (
        "WITH token_matches AS ("
        + " UNION ALL ".join(branches)
        + "), matched_ids AS ("
        "SELECT id FROM token_matches "
        "GROUP BY id "
        f"HAVING COUNT(DISTINCT token_idx) = {len(tokens)}"
        ") "
    )


def _pg_candidate_cte(tokens: list[str], params: dict[str, object]) -> str:
    if len(tokens) == 1:
        return _pg_single_token_candidates(tokens[0], params)
    return _pg_multi_token_candidates(tokens, params)


def _fts_filter_sql(
    filters: dict[str, str] | None,
) -> dict[str, tuple[str, dict[str, str]]]:
    """Translate supported structured filters into SQL snippets (PostgreSQL)."""
    if not filters:
        return {}

    sql_filters: dict[str, tuple[str, dict[str, str]]] = {}
    if stealer := filters.get("stealer"):
        sql_filters["stealer"] = ("pc.stealer_type = :stealer", {"stealer": stealer})
    if app := filters.get("app"):
        sql_filters["app"] = ("pc.application ILIKE :app", {"app": f"%{app}%"})
    if email_domain := filters.get("email_domain"):
        sql_filters["email_domain"] = (
            "pc.email_domain ILIKE :email_domain",
            {"email_domain": f"%{email_domain}%"},
        )
    if source := filters.get("source"):
        sql_filters["source"] = ("pc.source_archive ILIKE :source", {"source": f"%{source}%"})
    return sql_filters
