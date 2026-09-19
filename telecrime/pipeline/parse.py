"""Stage: Parse - parse extracted stealer log files for credentials."""

import asyncio
import io
import itertools
import json
import logging
import multiprocessing
import os
import re
import threading
import time
from collections import Counter
from collections.abc import AsyncGenerator, Iterator
from concurrent.futures import Future, ProcessPoolExecutor
from pathlib import Path
from typing import cast

from sqlalchemy import delete, inspect, select, text
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import selectinload

from telecrime.database import get_dialect_insert
from telecrime.models import ArchiveGroup, ExtractionJob, ParsedCredential
from telecrime.models.pipeline_state import PipelineState
from telecrime.models.system_info import SystemInfoRecord
from telecrime.pipeline.orchestrator import PipelineContext, PipelineStage
from telecrime.states import ExtractionStatus, GroupStatus
from telecrime.stealer.parser import (
    _COMBO_CLASSIFY_LINES,
    _classify_combo_head,
    _open_credential_file,
    iter_credentials_file,
    parse_credential_lines,
    parse_system_info,
    truncate_field,
)
from telecrime.stealer.patterns import detect_stealer_type, is_credential_file, is_system_info_file

logger = logging.getLogger(__name__)

# Files at least this big are parsed in parallel worker processes (the pure-Python
# regex parser is CPU-bound, so chunked parallelism gives near-linear speedup).
_PARALLEL_PARSE_MIN_BYTES = 20 * 1024 * 1024  # 20 MB
# Chunk size for the parallel path. Each chunk is a list of lines handed to a
# worker process; results stream back in order and feed the same batch/insert
# pipeline as the sequential path.
_PARALLEL_CHUNK_LINES = 100_000
# Labeled-block credentials are separated by blank/separator lines. Chunks are
# cut at those boundaries so a block is never split across two workers.
_CHUNK_BOUNDARY_RE = re.compile(r"^(?:---+|===+|_{3,})\s*$")
# Back-to-back labeled records (no blank line between them) can straddle a hard
# cut. Carrying the trailing lines into the next chunk would re-parse complete
# records (harmless: ON CONFLICT dedups) but never loses a straddling one.
_HARD_CUT_CARRY_LINES = 5
# Fallback sleep for the event-driven parallel consumer. Real work wakes it
# immediately (chunk enqueue / future completion); this only bounds how long a
# lost/edge-missed signal or the no-progress watchdog check can sit idle.
_PARALLEL_PARSE_IDLE_WAIT_SECONDS = 1.0
# Persisted on the job when a file was early-skipped so the next run forces a
# full parse of that file instead of early-skipping at the same point forever
# (the duplicate-heavy prefix is deterministic).
_EARLY_SKIP_CODE = "EARLY_SKIP"

# Job marker for a file whose parse was interrupted. Unlike EARLY_SKIP (which
# deletes the file's rows and forces a full re-parse), RESUME keeps the
# committed rows and tells the next run to continue the deterministic
# credential sequence from the persisted count.
_RESUME_CODE = "RESUME"

# pipeline_state key prefix for the file currently being parsed. A hard kill
# (SIGKILL / OOM / container restart mid-file) leaves rows from the file's
# already-flushed batches committed. The next run's per-file pre-skip
# (parsed_source_files) skips any file with rows, so without this marker the
# file's unparsed tail would be lost forever. The marker stores the exact
# number of credentials already committed for the file's deterministic yield
# sequence plus the parse mode that produced that sequence (the parallel path
# can yield carry-over duplicates at hard chunk cuts, so counts are only
# comparable within the same mode). On startup the marker is validated and
# kept; the parse stage resumes from the count instead of re-probing millions
# of already-committed rows.
_PARSE_MARKER_PREFIX = "parse_in_progress:"


def _marker_payload(row: PipelineState | None) -> dict | None:
    """Decode a parse marker row into its payload dict (or None)."""
    if row is None or not row.value_text:
        return None
    text = row.value_text
    if text.startswith("{"):
        try:
            data = json.loads(text)
        except ValueError:
            return None
        if isinstance(data, dict) and data.get("file"):
            return data
        return None
    # Legacy marker (pre-resume): value_text was the bare file path.
    return {"file": text, "resume": 0, "mode": None, "hash": None}


def _set_parse_marker(
    session,
    job_id: int,
    source_file: str,
    *,
    resume: int = 0,
    mode: str | None = None,
    output_hash: str | None = None,
) -> None:
    """Durably record the file about to be parsed (pre-parse commit)."""
    key = f"{_PARSE_MARKER_PREFIX}{job_id}"
    row = session.get(PipelineState, key)
    if row is None:
        row = PipelineState(key=key)
        session.add(row)
    row.value_int = job_id
    row.value_text = json.dumps(
        {
            "file": source_file,
            "resume": int(resume),
            "mode": mode,
            "hash": output_hash,
        }
    )
    session.commit()


def _update_parse_marker(
    session,
    job_id: int,
    *,
    resume: int,
    mode: str | None,
    output_hash: str | None,
) -> None:
    """Advance the resume count AFTER the rows it describes were committed.

    Called post-commit (not inside the batch transaction) so the marker can
    never claim more committed credentials than the database actually holds:
    a crash between the row commit and this marker commit leaves the marker
    under-counting, and an under-count only re-processes rows that ON CONFLICT
    already dedups.
    """
    key = f"{_PARSE_MARKER_PREFIX}{job_id}"
    row = session.get(PipelineState, key)
    if row is None:
        return
    data = _marker_payload(row) or {}
    data["resume"] = int(resume)
    data["mode"] = mode
    data["hash"] = output_hash
    row.value_text = json.dumps(data)
    session.commit()


def _clear_parse_marker(session, job_id: int) -> None:
    """Drop the in-progress marker after a file finished (or was cleaned)."""
    session.execute(
        delete(PipelineState).where(PipelineState.key == f"{_PARSE_MARKER_PREFIX}{job_id}")
    )
    session.commit()


def _recover_partial_parses(session) -> int:
    """Validate parse markers left by a crashed run.

    Returns the number of files that will be resumed. Committed rows are kept
    (resuming skips their credentials instead of re-probing them). Markers are
    dropped when the job/group is gone, the group is no longer EXTRACTED, or
    the marked file has zero committed rows (the failure path deletes all of a
    job's rows; a zero-row file must be re-parsed from scratch, and resuming
    past a deleted prefix would lose data).
    """
    markers = (
        session.execute(
            select(PipelineState).where(PipelineState.key.like(f"{_PARSE_MARKER_PREFIX}%"))
        )
        .scalars()
        .all()
    )
    resumed = 0
    changed = False
    for marker in markers:
        payload = _marker_payload(marker)
        job_id = marker.value_int
        if payload is None or not job_id:
            session.delete(marker)
            changed = True
            continue
        job = session.get(ExtractionJob, job_id)
        group = session.get(ArchiveGroup, job.group_id) if job and job.group_id else None
        if job is None or group is None or group.status != GroupStatus.EXTRACTED:
            session.delete(marker)
            changed = True
            continue
        has_rows = (
            session.execute(
                select(ParsedCredential.id)
                .where(
                    ParsedCredential.extraction_job_id == job_id,
                    ParsedCredential.source_file == payload["file"],
                )
                .limit(1)
            ).first()
            is not None
        )
        if not has_rows:
            session.delete(marker)
            changed = True
            continue
        resumed += 1
        logger.warning(
            "Startup recovery: will resume %s (job %s) from credential %s "
            "(mode %s)",
            payload["file"],
            job_id,
            payload.get("resume"),
            payload.get("mode"),
        )
    if changed:
        session.commit()
    return resumed


def _iter_line_chunks(
    fh,
    chunk_lines: int = _PARALLEL_CHUNK_LINES,
) -> Iterator[list[str]]:
    """Yield lists of lines, cut only at blank/separator block boundaries.

    Splitting mid-block would let a labeled credential fall across two workers
    and be lost. We accumulate lines and only cut once we have at least
    `chunk_lines` buffered AND the current line is a blank or
    ``---``/``===``/``___`` separator. If no boundary appears before 4×
    `chunk_lines` the file is mostly line-independent, but back-to-back labeled
    records still exist: the trailing ``_HARD_CUT_CARRY_LINES`` lines are
    carried into the next chunk so a record straddling the cut is completed by
    the following worker instead of being lost. Complete carried records are
    re-parsed and deduplicated by credential_hash, which is harmless.
    """
    buf: list[str] = []
    for raw in fh:
        line = raw.rstrip("\n").rstrip("\r")
        buf.append(line)
        s = line.strip()
        if len(buf) >= chunk_lines and (not s or _CHUNK_BOUNDARY_RE.match(s)):
            yield buf
            buf = []
        elif len(buf) >= chunk_lines * 4:
            # Overlap the trailing lines into the next chunk: a record whose
            # head is at the end of this chunk is then complete in the next one
            # (the previous non-overlapping carry split it and lost it). Any
            # complete record re-parsed by the overlap is deduplicated by
            # credential_hash.
            yield buf
            buf = list(buf[-_HARD_CUT_CARRY_LINES:])
    if buf:
        yield buf


def _read_combo_probe(fh) -> tuple[list[str], bool]:
    """Read the true file head (at most _COMBO_CLASSIFY_LINES lines) and
    classify it.

    Classification must happen once per file, from the real head, in the
    submitting process: chunks of the same file are parsed by independent
    workers, and a later combo-dominant chunk would otherwise classify itself
    as a pure combo file and silently drop the labeled credentials it also
    contains. The consumed lines are returned so the caller can re-chain them
    into the chunk reader.
    """
    head = list(itertools.islice(fh, _COMBO_CLASSIFY_LINES))
    return head, _classify_combo_head(head)


def _parse_lines_chunk_worker(args: tuple[list[str], str, bool | None]) -> list:
    """Worker entry point: parse a chunk of lines into pre-processed tuples.

    Kept at module level so the ProcessPoolExecutor can pickle it. The worker
    does the NUL-strip/truncation AND both SHA-256 hashes (the CPU-bound half
    of the row pipeline) so the main process only assembles COPY rows.
    Tuple shape mirrors what the sequential path builds in _flush_batch:
    (url, domain_row, username, password, email_domain, application, profile,
     credential_hash, soft_credential_hash). soft_credential_hash input is
    the domain-or-url (untruncated) exactly like the sequential path.
    """
    lines, source_file, combo_decision = args
    out = []
    for c in parse_credential_lines(
        iter(lines), source_file, combo_decision=combo_decision
    ):
        url = c.url or ""
        if "\x00" in url:
            url = url.replace("\x00", "")
        url_val = url[:1024]
        d = c.domain
        if d:
            if "\x00" in d:
                d = d.replace("\x00", "")
            domain_row_val = d[:255]
        else:
            domain_row_val = None
        un = c.username or ""
        if "\x00" in un:
            un = un.replace("\x00", "")
        user_val = un[:255]
        pw = c.password or ""
        if "\x00" in pw:
            pw = pw.replace("\x00", "")
        pass_val = pw[:255]
        # `d` is the NUL-stripped domain (or its falsy original) and `url` the
        # NUL-stripped url, so reusing them is identical to re-reading the
        # attributes and re-scanning for NULs on every line.
        domain_or_url = d or url
        hash_input = domain_or_url[:255]
        _ed = c.email_domain
        if _ed:
            if "\x00" in _ed:
                _ed = _ed.replace("\x00", "")
            email_domain_val = _ed[:255]
        else:
            email_domain_val = None
        _app = c.application
        if _app:
            if "\x00" in _app:
                _app = _app.replace("\x00", "")
            app_val = _app[:100]
        else:
            app_val = None
        _prof = c.profile
        if _prof:
            if "\x00" in _prof:
                _prof = _prof.replace("\x00", "")
            prof_val = _prof[:100]
        else:
            prof_val = None
        out.append((
            url_val,
            domain_row_val,
            user_val,
            pass_val,
            email_domain_val,
            app_val,
            prof_val,
            ParsedCredential.compute_hash(hash_input, user_val, pass_val),
            ParsedCredential.compute_soft_hash(domain_or_url, user_val, pass_val),
        ))
    return out


def _save_system_info(session, job_id: int, sysinfo) -> None:
    """Persist a parsed SystemInfo record to the DB (idempotent)."""
    exists = session.execute(
        select(SystemInfoRecord.id).where(SystemInfoRecord.extraction_job_id == job_id)
    ).first()
    if exists:
        return
    def _trunc(val, n):
        return val[:n] if val and len(val) > n else val

    session.add(
        SystemInfoRecord(
            extraction_job_id=job_id,
            hostname=_trunc(sysinfo.hostname, 255),
            username=_trunc(sysinfo.username, 255),
            ip_address=_trunc(sysinfo.ip_address, 50),
            country=_trunc(sysinfo.country, 100),
            hwid=_trunc(sysinfo.hwid, 255),
            os=_trunc(sysinfo.os, 255),
            cpu=_trunc(sysinfo.cpu, 255),
            gpu=_trunc(sysinfo.gpu, 255),
            ram=_trunc(sysinfo.ram, 50),
            timezone=_trunc(sysinfo.timezone, 100),
            language=_trunc(sysinfo.language, 50),
            screen_size=_trunc(sysinfo.screen_size, 50),
            log_date=sysinfo.log_date,
            stealer_name=_trunc(sysinfo.stealer_name, 100),
        )
    )


_BATCH_SIZE = 20_000
# COPY+INSERT chunk size. Larger chunks amortize the staging-table DDL and
# COPY round-trip over more rows; the two-stage dedup then filters the bulk
# of duplicates in one pass before the exact unique-index check.
# PostgreSQL handles 50K rows easily; SQLite (test fixtures) is capped by its
# 999 SQL-variable limit, so the values path keeps the smaller size.
_INSERT_CHUNK_SIZE = 50_000
_INSERT_CHUNK_SIZE_SQLITE = 10_000


def _apply_pg_bulk_settings(session) -> None:
    session.execute(text("SET synchronous_commit = off"))
    session.execute(text("SET statement_timeout = 0"))
    # With url_trgm, source_archive_trgm, and email_domain_trgm dropped we are
    # down to 2 GIN indexes (domain_trgm + username_trgm, ~38 GB total).  128 MB
    # halves flush frequency vs 64 MB; each flush still fits in 1 GB maintenance_work_mem.
    session.execute(text("SET gin_pending_list_limit = 134217728"))
    # GIN pending-list flushes sort entries in memory; a larger budget means
    # faster sorts and shorter stall windows during bulk inserts.
    session.execute(text("SET maintenance_work_mem = '1GB'"))
    # The compose default work_mem is 4MB; the staging-table sort/anti-join
    # paths spill to disk per chunk without a raise.
    session.execute(text("SET work_mem = '64MB'"))
    # Keep parse temp spills off the internal SSD: `intts` holds the growing
    # credential indexes and WAL already shares that filesystem, so temp files
    # there are the fastest way to fill it. The external drive has room.
    session.execute(text("SET temp_tablespaces = 'pg_default'"))


def _reset_pg_bulk_settings(session) -> None:
    try:
        session.execute(text("SET synchronous_commit = on"))
        session.execute(text("SET statement_timeout = DEFAULT"))
        session.execute(text("SET gin_pending_list_limit = DEFAULT"))
        session.execute(text("SET maintenance_work_mem = DEFAULT"))
        session.execute(text("SET work_mem = DEFAULT"))
        session.execute(text("SET temp_tablespaces = DEFAULT"))
    except Exception:
        pass


def _is_dup_batch(new_count: int, dup_count: int, batch_size: int) -> bool:
    """True when a batch is dominated by duplicates (early-skip signal)."""
    total = new_count + dup_count
    return total >= batch_size // 2 and (dup_count / total) >= 0.95


# The trigram GIN indexes are absent on databases rebuilt without them; calling
# gin_clean_pending_list() on a missing relation raises on every chunk (caught
# best-effort, but it floods the PostgreSQL log and opens a savepoint per
# chunk). Probe once per process.
_HAS_TRGM_INDEXES: bool | None = None


def _trigram_indexes_present(session) -> bool:
    global _HAS_TRGM_INDEXES
    if _HAS_TRGM_INDEXES is not None:
        return _HAS_TRGM_INDEXES
    try:
        rows = (
            session.execute(
                text(
                    "SELECT indexname FROM pg_indexes "
                    "WHERE tablename = 'parsed_credentials' "
                    "AND indexname IN "
                    "('ix_pc_username_trgm', 'ix_pc_domain_trgm')"
                )
            )
            .scalars()
            .all()
        )
    except Exception:
        return False
    _HAS_TRGM_INDEXES = len(set(rows)) == 2
    return _HAS_TRGM_INDEXES


# PostgreSQL rejects statements binding more than 65,535 parameters, so an
# unbatched `column.in_(...)` over a large job's outputs raises
# OperationalError and aborts the file mid-parse.
_IN_QUERY_BATCH_SIZE = 1000


def _iter_in_batches(
    values: list[str], batch_size: int = _IN_QUERY_BATCH_SIZE
) -> Iterator[list[str]]:
    """Split ``values`` into bind-parameter-safe IN-list batches."""
    for i in range(0, len(values), batch_size):
        yield values[i : i + batch_size]


def _hash64_expr(alias: str) -> str:
    """SQL expression for the compact 64-bit credential-hash fingerprint.

    First 16 hex chars of the SHA256 credential_hash cast to bigint. Matches
    the ix_pc_hash64 expression index exactly.
    """
    return f"CAST((CAST((chr(120) || substring({alias}.credential_hash, 1, 16)) AS bit(64))) AS bigint)"


_HAS_HASH64: bool | None = None  # resolved lazily against the live schema
# On a transient probe failure (connection reset/timeout) the probe result is
# left unresolved and retried after this short backoff instead of caching
# False for the process lifetime — a single blip used to force the slow
# left(hash,32) dedup fallback until the next restart.
_HAS_HASH64_RETRY_AT: float = 0.0
_HAS_HASH64_PROBE_BACKOFF_SECONDS = 60.0


def _has_hash64_index(engine) -> bool | None:
    """True when a *valid* ix_pc_hash64 (compact dedup index) exists.

    pg_indexes lists indexes left invalid by an interrupted CREATE INDEX
    CONCURRENTLY, but the planner ignores them. Treating an invalid index as
    present made every dedup INSERT fall back to a per-row full index scan of
    parsed_credentials (minutes per 50K chunk instead of milliseconds).

    Tri-state: True/False are cached probe results. ``None`` means the probe
    itself could not run (transient connection error) or is inside its backoff
    window — callers must treat that as "unknown", never as "absent", or they
    would drop and rebuild a perfectly valid index.
    """
    global _HAS_HASH64, _HAS_HASH64_RETRY_AT
    if _HAS_HASH64 is not None:
        return _HAS_HASH64
    now = time.monotonic()
    if now < _HAS_HASH64_RETRY_AT:
        return None
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT 1 FROM pg_index i "
                    "JOIN pg_class c ON c.oid = i.indexrelid "
                    "WHERE c.relname = 'ix_pc_hash64' AND i.indisvalid"
                )
            ).fetchone()
        _HAS_HASH64 = row is not None
        _HAS_HASH64_RETRY_AT = 0.0
        return _HAS_HASH64
    except Exception as exc:
        logger.warning(
            "Could not probe ix_pc_hash64 (%s) — will retry in %.0fs",
            exc,
            _HAS_HASH64_PROBE_BACKOFF_SECONDS,
        )
        _HAS_HASH64 = None
        _HAS_HASH64_RETRY_AT = now + _HAS_HASH64_PROBE_BACKOFF_SECONDS
        return None


def _ensure_hash64_index(engine) -> None:
    """Create (or repair) ix_pc_hash64, the compact dedup index.

    Without a *valid* index every COPY chunk falls back to a per-row scan of
    the full credential_hash index and can time out. Repairs an invalid
    leftover from an interrupted build as well as a missing index.
    CONCURRENTLY needs autocommit, so a fresh connection is used.

    Only acts on a positively resolved "absent/invalid" probe: an unresolved
    probe (transient error/backoff) must never trigger the DROP boundary, or a
    connection blip would tear down and rebuild a valid multi-GB index.
    """
    if engine.dialect.name != "postgresql":
        return
    resolved = _has_hash64_index(engine)
    if resolved is None or resolved:
        return
    global _HAS_HASH64, _HAS_HASH64_RETRY_AT
    try:
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            # An invalid leftover from an interrupted CONCURRENTLY build still
            # satisfies CREATE INDEX IF NOT EXISTS, so it must be dropped first
            # or the repair silently no-ops and the dedup INSERT stays slow.
            conn.execute(text("DROP INDEX CONCURRENTLY IF EXISTS ix_pc_hash64"))
            conn.execute(
                text(
                    "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_pc_hash64 "
                    "ON parsed_credentials "
                    "((CAST((CAST((chr(120) || substring(credential_hash, 1, 16)) AS bit(64))) AS bigint)))"
                )
            )
        _HAS_HASH64 = None  # re-resolve against the live schema
        _HAS_HASH64_RETRY_AT = 0.0
    except Exception as exc:
        logger.warning("Could not create ix_pc_hash64 index: %s", exc)
        # Do not permanently cache False after a failed repair: the index may be
        # created concurrently later (missing indexes are being rebuilt), and a
        # transient DDL failure must not disable the fast dedup path.
        _HAS_HASH64 = None
        _HAS_HASH64_RETRY_AT = (
            time.monotonic() + _HAS_HASH64_PROBE_BACKOFF_SECONDS
        )


def _shutdown_parse_pool(pool: ProcessPoolExecutor, *, force: bool) -> None:
    """Shut down a parse pool, killing wedged workers on the failure path.

    On success ``shutdown(wait=True)`` waits for in-flight chunks, which is
    required to keep no worker writing after we move on. On the failure path a
    worker wedged in native code (e.g. OOM-adjacent stall) makes that wait hang
    forever — the pipeline process stays alive but makes no progress and the
    watchdog cannot recover it. Cancel pending futures, terminate, then kill
    surviving workers so the caller can fall back to the sequential parse.
    """
    if not force:
        pool.shutdown(wait=True)
        return

    try:
        pool.shutdown(wait=False, cancel_futures=True)
    except Exception as exc:
        logger.debug("Process pool shutdown(wait=False) failed: %s", exc)

    processes = list((getattr(pool, "_processes", None) or {}).values())
    for proc in processes:
        try:
            if proc.is_alive():
                proc.terminate()
        except Exception:
            pass
    deadline = time.monotonic() + 5.0
    for proc in processes:
        try:
            proc.join(timeout=max(0.0, deadline - time.monotonic()))
        except Exception:
            pass
    for proc in processes:
        try:
            if proc.is_alive():
                proc.kill()
        except Exception:
            pass


# Most COPY fields contain no control characters; one C-level scan per value
# (no allocation) avoids 4 str.replace dispatches per field. Module-level (not
# a class attribute) so the per-field hot path does no attribute lookup.
_COPY_ESCAPE_RE = re.compile(r"[\\\t\n\r]")


class ParseStage(PipelineStage):
    """Parse extracted stealer logs for credentials."""

    name = "parse"

    @staticmethod
    def _parallel_worker_count() -> int:
        """Number of parse worker processes to spawn.

        Configurable via TELECRIME_PARSE_WORKERS; defaults to a modest slice
        of the CPU budget so parse (pure-Python regex) scales across cores
        without starving the DB / download / web containers on the same host.
        """
        raw = os.environ.get("TELECRIME_PARSE_WORKERS", "")
        if raw.isdigit() and int(raw) >= 1:
            return int(raw)
        cpus = os.cpu_count() or 1
        return max(1, min(4, cpus - 1))

    async def _iter_parallel_credentials(
        self,
        file_path: Path,
        source_file: str,
        workers: int,
    ) -> AsyncGenerator[tuple[str, str | None, str, str, str | None, str | None, str | None, str, str], None]:
        """Parse a large file across worker processes, streaming results in order.

        The file is split into line chunks (cut only at labeled-block boundary
        lines) and each chunk is handed to a worker via a process pool. A
        dedicated producer thread reads chunks into a bounded thread-safe queue
        (≤ 2×workers) and a matching window of in-flight futures is kept; the
        consumer drains completed futures in submission order and is woken
        edge-triggered (no polling), keeping memory bounded and never blocking
        the event loop.

        Yields pre-processed tuples (url, domain, username, password,
        email_domain, application, profile, credential_hash, soft_hash) so the
        caller's batch-flush hot loop assembles COPY rows directly.
        """
        fh = _open_credential_file(file_path, "utf-8")
        if fh is None:
            return

        # Classify ONCE in this (submitting) process from the file's true head
        # and pass the decision to every chunk worker. Letting each worker
        # classify its own chunk's head would route a combo-dominant later
        # chunk of a mixed file through the combo fast path, silently dropping
        # its labeled credentials, and workers could disagree about the file.
        head, combo_decision = _read_combo_probe(fh)
        chunk_source = itertools.chain(head, fh)

        loop = asyncio.get_event_loop()

        # Chunks are read in a background thread (pure file I/O, no pool
        # interaction) and fed to this async generator, which performs all
        # pool.submit calls from the main event-loop thread. A thread-safe
        # queue.Queue (blocking put) is used so the reader can NEVER lose a
        # chunk: asyncio.Queue.put_nowait raises QueueFull in the loop thread
        # when full, silently dropping the chunk (and the sentinel), which
        # deadlocks the consumer forever.
        import queue as _queue

        chunks_q: _queue.Queue = _queue.Queue(maxsize=workers * 2)
        sentinel = object()
        # Set when the consumer stops early (early-skip break, pool failure):
        # the reader must then stop putting instead of blocking forever on a
        # full queue — otherwise the generator's `finally: await read_task`
        # hangs the whole pipeline permanently (the watchdog cannot recover a
        # hung-but-alive process).
        stop_event = threading.Event()

        # Edge-triggered wakeup for the consumer. The previous implementation
        # polled with `await asyncio.sleep(0.05)`: every completed chunk waited
        # up to 50 ms before being drained (a 0.3 s chunk lost ~8-15% wall
        # time) and the event loop woke 20×/s even when nothing changed. The
        # reader thread and pool completion callbacks now signal this event;
        # `call_soon_threadsafe` is the only cross-thread asyncio primitive
        # used, so the pool's internal locks are never re-entered from a
        # threaded wait (the deadlock the polling loop was avoiding).
        wakeup = asyncio.Event()

        def _notify() -> None:
            try:
                loop.call_soon_threadsafe(wakeup.set)
            except RuntimeError:
                # Loop already closed (generator aborted while a worker was
                # still finishing) — nothing to wake.
                pass

        def _read_chunks() -> None:
            try:
                for chunk in _iter_line_chunks(chunk_source):
                    while not stop_event.is_set():
                        try:
                            chunks_q.put(chunk, timeout=0.5)
                            break
                        except _queue.Full:
                            continue
                    if stop_event.is_set():
                        break
                    _notify()
            except Exception as exc:
                logger.warning("Parallel parse reader failed: %s", exc)
            finally:
                fh.close()
                # The sentinel must be delivered even when the queue is full
                # (normal EOF state: the fast reader races ahead of the
                # consumer). A put_nowait here would raise Full and silently
                # drop it, leaving the consumer spinning on submitting=True
                # forever. Poll with the same timeout loop the chunks use.
                while not stop_event.is_set():
                    try:
                        chunks_q.put(sentinel, timeout=0.5)
                        break
                    except _queue.Full:
                        continue
                _notify()

        read_task = loop.run_in_executor(None, _read_chunks)

        # spawn is required here. With fork, ProcessPoolExecutor starts its
        # workers *lazily on the first submit* — the forked children inherit the
        # parent's call-queue condition lock mid-acquisition, and every worker
        # then blocks forever in multiprocessing/synchronize.py (__enter__ on
        # the queue lock). This deadlock is independent of which thread submits,
        # so no warm-up trick can fix it. spawn re-executes the worker entry
        # point cleanly with no inherited locks. The pipeline runs as
        # `python -m telecrime run` whose __main__ guard prevents re-running
        # the CLI inside workers. Pool creation and all submits happen on the
        # main event-loop thread; the background thread only reads file chunks.
        ctx = multiprocessing.get_context("spawn")

        pool = ProcessPoolExecutor(max_workers=workers, mp_context=ctx)
        pool_failed = False
        try:
            in_flight: list[Future] = []
            submitting = True
            # If the spawn workers get OOM-killed (RAM-tight host), the pool
            # breaks: futures stay pending forever and this generator would
            # hang the pipeline. Detect no-progress and bail out so the caller
            # can fall back to the sequential parse.
            last_result_at = time.monotonic()
            try:
                while True:
                    # Refill the in-flight window from the reader thread.
                    # chunks_q is a thread-safe queue.Queue: drain it without
                    # blocking the event loop (get_nowait + idle event wait).
                    while len(in_flight) < workers * 2 and submitting:
                        try:
                            chunk = chunks_q.get_nowait()
                        except _queue.Empty:
                            break
                        if chunk is sentinel:
                            submitting = False
                            break
                        fut = pool.submit(
                            _parse_lines_chunk_worker,
                            (chunk, source_file, combo_decision),
                        )
                        # Callback runs in the pool's management thread; the
                        # notify is thread-safe and only sets the event.
                        fut.add_done_callback(lambda _f: _notify())
                        in_flight.append(fut)
                    if not in_flight and not submitting:
                        break
                    if (
                        not in_flight
                        and read_task.done()
                        and chunks_q.empty()
                    ):
                        # Belt-and-suspenders: reader thread finished, nothing
                        # queued and nothing in flight — done, even if the
                        # sentinel was somehow lost.
                        break
                    # Edge-triggered wait: a completed future or a queued chunk
                    # sets `wakeup` from its own thread, so results are drained
                    # immediately instead of up to a poll interval later. The
                    # timeout only bounds the no-progress check and recovers
                    # from an edge-missed signal; the event loop never blocks
                    # on a threaded pool wait (which could re-enter pool locks).
                    try:
                        await asyncio.wait_for(
                            wakeup.wait(),
                            timeout=_PARALLEL_PARSE_IDLE_WAIT_SECONDS,
                        )
                    except TimeoutError:
                        pass
                    wakeup.clear()
                    still_pending: list[Future] = []
                    progressed = False
                    for fut in in_flight:
                        if not fut.done():
                            still_pending.append(fut)
                            continue
                        try:
                            chunk_result = fut.result()
                        except Exception as exc:
                            logger.warning("Parallel parse chunk failed: %s", exc)
                            raise RuntimeError(
                                "parallel parse pool failed — falling back to sequential"
                            ) from exc
                        progressed = True
                        for tup in chunk_result:
                            yield tup
                    in_flight = still_pending
                    if progressed:
                        last_result_at = time.monotonic()
                    elif in_flight and time.monotonic() - last_result_at > 120:
                        # No result for 2 minutes while futures are pending →
                        # workers are dead (OOM). Abort instead of hanging.
                        raise RuntimeError(
                            "parallel parse produced no results for 120s — "
                            "falling back to sequential"
                        )
            finally:
                # Stop the reader and unblock it: it may be mid-put on a full
                # queue (consumer stopped via early-skip or a pool failure).
                # Bounded wait — a wedged reader must not hang the pipeline.
                stop_event.set()
                try:
                    while True:
                        chunks_q.get_nowait()
                except _queue.Empty:
                    pass
                try:
                    await asyncio.wait_for(asyncio.shield(read_task), timeout=5)
                except (TimeoutError, Exception):
                    pass
        except BaseException:
            # Includes GeneratorExit (early-skip break) and CancelledError:
            # never block on shutdown(wait=True) with a wedged worker.
            pool_failed = True
            raise
        finally:
            _shutdown_parse_pool(pool, force=pool_failed)

    async def run(self, ctx: PipelineContext) -> bool:
        """Parse credential files from successful extractions."""
        logger.info("Starting credential parsing")

        jobs = self._iter_jobs(ctx)
        # `not jobs` is dead on a generator — peek the first page instead.
        first = next(jobs, None)
        if first is None:
            logger.info("No completed extractions to parse")
            return True
        jobs = itertools.chain([first], jobs)

        total_credentials = 0
        total_duplicates = 0

        if ctx.has_soft_hash_column is None:
            db_columns = {
                col["name"]
                for col in inspect(ctx.session.get_bind()).get_columns("parsed_credentials")
            }
            ctx.has_soft_hash_column = "soft_credential_hash" in db_columns

        _apply_pg_bulk_settings(ctx.session)
        _ensure_hash64_index(ctx.session.get_bind())

        try:
            # Groups marked incomplete earlier in this run: a sibling job that
            # parses fully must not clear its group's flag (multi-job groups).
            incomplete_groups: set[int] = set()
            for job in jobs:
                try:
                    creds_found, dups_found, incomplete = (
                        await self._parse_job_outputs(
                            ctx, job, ctx.has_soft_hash_column
                        )
                    )
                    total_credentials += creds_found
                    total_duplicates += dups_found
                    ctx.credentials_parsed += creds_found
                    ctx.duplicates_skipped += dups_found
                    # Fully parsed: finalize may clean the group. An early-skipped
                    # file marks the group incomplete instead, so finalize
                    # keeps its archive for the next run.
                    if job.group_id is not None:
                        if incomplete:
                            incomplete_groups.add(job.group_id)
                            ctx.parse_failed_group_ids.add(job.group_id)
                        elif job.group_id not in incomplete_groups:
                            ctx.parse_failed_group_ids.discard(job.group_id)
                except Exception as e:
                    try:
                        ctx.session.rollback()
                    except Exception:
                        pass
                    try:
                        _apply_pg_bulk_settings(ctx.session)
                    except Exception as settings_error:
                        logger.warning(
                            "Could not restore parse DB settings: %s",
                            settings_error,
                        )
                    logger.error("Error parsing job %d: %s", job.id, e)
                    ctx.errors.append(f"Parse error for job {job.id}: {e}")
                    # The unparsed remainder of this job's files is only on
                    # disk. Tell finalize to leave the group EXTRACTED so the
                    # next run re-parses it instead of deleting the files.
                    if job.group_id is not None:
                        incomplete_groups.add(job.group_id)
                        ctx.parse_failed_group_ids.add(job.group_id)
                    # CRITICAL partial-loss edge: the next run's per-file
                    # pre-skip (parsed_source_files) skips a file as soon as
                    # ANY of its rows exist. If a chunk insert failed after
                    # earlier chunks of the SAME file succeeded (wedge-straddled
                    # double chunk failure), the file would be skipped next run
                    # and its ≤50K un-inserted rows lost forever. Delete the
                    # job's already-inserted rows so the whole job re-parses
                    # cleanly (ON CONFLICT dedups the re-inserts).
                    try:
                        removed = cast(
                            CursorResult,
                            ctx.session.execute(
                                delete(ParsedCredential).where(
                                    ParsedCredential.extraction_job_id == job.id
                                )
                            ),
                        ).rowcount
                        ctx.session.commit()
                        # The rows the marker counted are gone: dropping it
                        # prevents a resume that would skip re-parsing them.
                        _clear_parse_marker(ctx.session, job.id)
                        logger.info(
                            "Removed %d partial rows for failed job %d — will re-parse next run",
                            removed or 0,
                            job.id,
                        )
                    except Exception as del_err:
                        logger.warning(
                            "Could not clean partial rows for job %d: %s",
                            job.id, del_err,
                        )
                        try:
                            ctx.session.rollback()
                        except Exception:
                            pass
        finally:
            _reset_pg_bulk_settings(ctx.session)

        logger.info(
            "Parsed %d credentials total (%d duplicates skipped)",
            total_credentials,
            total_duplicates,
        )
        return True

    async def run_group(self, ctx: PipelineContext, group_id: int) -> tuple[int, int]:
        """Parse completed extraction jobs for a single EXTRACTED group."""
        jobs = self._iter_jobs(ctx, group_id=group_id)
        # `not jobs` would be dead on a generator — peek the first page.
        first = next(jobs, None)
        if first is None:
            return 0, 0
        jobs = itertools.chain([first], jobs)

        if ctx.has_soft_hash_column is None:
            db_columns = {
                col["name"]
                for col in inspect(ctx.session.get_bind()).get_columns("parsed_credentials")
            }
            ctx.has_soft_hash_column = "soft_credential_hash" in db_columns

        _apply_pg_bulk_settings(ctx.session)
        _ensure_hash64_index(ctx.session.get_bind())

        total_credentials = 0
        total_duplicates = 0
        try:
            # See ParseStage.run: a later complete job must not clear an
            # incomplete sibling's group flag.
            incomplete_groups: set[int] = set()
            for job in jobs:
                job_id = job.id
                try:
                    creds_found, dups_found, incomplete = (
                        await self._parse_job_outputs(
                            ctx, job, ctx.has_soft_hash_column
                        )
                    )
                    total_credentials += creds_found
                    total_duplicates += dups_found
                    ctx.credentials_parsed += creds_found
                    ctx.duplicates_skipped += dups_found
                    if job.group_id is not None:
                        if incomplete:
                            incomplete_groups.add(job.group_id)
                            ctx.parse_failed_group_ids.add(job.group_id)
                        elif job.group_id not in incomplete_groups:
                            ctx.parse_failed_group_ids.discard(job.group_id)
                except Exception as e:
                    # Same failure semantics as ParseStage.run: without this,
                    # an exception mid-file left partial rows committed, the
                    # next run's per-file pre-skip skipped the file, and
                    # finalize deleted the un-parsed remainder (data loss).
                    try:
                        ctx.session.rollback()
                    except Exception:
                        pass
                    try:
                        _apply_pg_bulk_settings(ctx.session)
                    except Exception as settings_error:
                        logger.warning(
                            "Could not restore parse DB settings: %s",
                            settings_error,
                        )
                    logger.error(
                        "Error parsing job %d (group %s): %s", job_id, group_id, e
                    )
                    ctx.errors.append(f"Parse error for job {job_id}: {e}")
                    # The un-parsed remainder is only on disk — tell finalize
                    # to leave the group EXTRACTED so the next run re-parses it
                    # instead of reclaiming the files.
                    incomplete_groups.add(group_id)
                    ctx.parse_failed_group_ids.add(group_id)
                    # Delete the job's already-inserted rows so the next run
                    # re-parses the whole job cleanly (ON CONFLICT dedups).
                    try:
                        removed = cast(
                            CursorResult,
                            ctx.session.execute(
                                delete(ParsedCredential).where(
                                    ParsedCredential.extraction_job_id == job_id
                                )
                            ),
                        ).rowcount
                        ctx.session.commit()
                        # The rows the marker counted are gone: dropping it
                        # prevents a resume that would skip re-parsing them.
                        _clear_parse_marker(ctx.session, job_id)
                        logger.info(
                            "Removed %d partial rows for failed job %d — will re-parse next run",
                            removed or 0,
                            job_id,
                        )
                    except Exception as del_err:
                        logger.warning(
                            "Could not clean partial rows for job %d: %s",
                            job_id, del_err,
                        )
                        try:
                            ctx.session.rollback()
                        except Exception:
                            pass
        finally:
            _reset_pg_bulk_settings(ctx.session)

        return total_credentials, total_duplicates

    def _iter_jobs(self, ctx: PipelineContext, group_id: int | None = None):
        """Yield completed extraction jobs whose groups are still EXTRACTED.

        Keyset-paged by ExtractionJob.id (limit 50) so a large crash backlog
        is never materialized in memory at once (each job pulls all its
        ExtractedOutput rows via selectinload).
        """
        last_id = 0
        while True:
            query = (
                select(ExtractionJob)
                .join(ArchiveGroup)
                .where(
                    ExtractionJob.status == ExtractionStatus.COMPLETED,
                    ArchiveGroup.status == GroupStatus.EXTRACTED,
                    ExtractionJob.id > last_id,
                )
                .options(
                    selectinload(ExtractionJob.outputs),
                    selectinload(ExtractionJob.group),
                )
                .order_by(ExtractionJob.id)
                .limit(50)
            )
            if group_id is not None:
                query = query.where(ArchiveGroup.id == group_id)
            jobs = list(ctx.session.execute(query).scalars().all())
            if not jobs:
                return
            yield from jobs
            last_id = jobs[-1].id
            ctx.session.expunge_all()

    async def _parse_job_outputs(
        self,
        ctx: PipelineContext,
        job: ExtractionJob,
        has_soft_hash_column: bool,
    ) -> tuple[int, int, bool]:
        """Parse credential files from a single extraction job.

        Processes credentials in batches of BATCH_SIZE to keep memory usage
        bounded regardless of individual file size.

        Returns:
            Tuple of (new_credentials, duplicates_skipped, incomplete) where
            ``incomplete`` is True when at least one file was early-skipped as
            duplicate-heavy — its tail was never parsed, so the caller must
            keep the group retryable and delete the file's partial rows.
        """
        credentials_found = 0
        duplicates_found = 0
        incomplete = False
        # A previous run early-skipped a file of this job: parse every file
        # fully this time so the never-parsed tail cannot be skipped forever.
        force_full = job.last_error_code == _EARLY_SKIP_CODE

        # Crash-resume state: the file that was mid-parse when the process
        # died, with the count/mode/hash needed to continue it instead of
        # re-probing every already-committed credential.
        resume_payload = _marker_payload(
            ctx.session.get(PipelineState, f"{_PARSE_MARKER_PREFIX}{job.id}")
        )
        resume_file = resume_payload.get("file") if resume_payload else None
        resume_seen = False

        # Get the extracted output files
        outputs = job.outputs

        # Find credential files among outputs
        credential_outputs = [o for o in outputs if is_credential_file(o.output_filename)]
        # ULP files first — highest-value credential lists
        credential_outputs.sort(
            key=lambda o: 0 if "ulp" in o.output_filename.lower() else 1
        )

        if not credential_outputs:
            return 0, 0, False

        logger.debug(
            "Found %d credential files in job %d",
            len(credential_outputs),
            job.id,
        )

        # Bulk-load already parsed source_file values for this job once.
        # This avoids an extra existence query per credential file. Batched:
        # a job with tens of thousands of outputs would otherwise exceed
        # PostgreSQL's 65,535 bind-parameter limit.
        source_paths = [str(Path(o.output_path)) for o in credential_outputs if o.output_path]
        parsed_source_files: set[str] = set()
        for batch_paths in _iter_in_batches(source_paths):
            parsed_source_files.update(
                ctx.session.execute(
                    select(ParsedCredential.source_file)
                    .where(
                        ParsedCredential.extraction_job_id == job.id,
                        ParsedCredential.source_file.in_(batch_paths),
                    )
                    .distinct()
                )
                .scalars()
                .all()
            )

        # Pre-skip files we've already seen multiple times.  When a credential
        # file's content has been observed ≥3× via first_seen_index, every row
        # in it is already in parsed_credentials with very high probability,
        # so the per-row ON CONFLICT check on the 270M-row credential_hash
        # unique index is wasted disk I/O.  Skipping the file outright avoids
        # 4GB ULP-combo files that come back as +0 new, 0 dups in recent_results.
        # Threshold of 2 means "we've seen this content in 3 separate archives";
        # finalize() always inserts the first row (creates new fsi entry with
        # duplicate_count=0); each subsequent archive bumps duplicate_count.
        # Threshold 1 (was 2): a file whose content was seen in 2 archives is
        # overwhelmingly a pure repost — skipping the 3rd copy saves minutes
        # of parse on multi-GB dumps while the queue rots (deleted-message
        # files convert recoverable archives into permanent losses).
        _preskip_dup_threshold = 1
        all_output_hashes = [o.output_hash for o in credential_outputs if o.output_hash]
        from telecrime.models import FirstSeenIndex
        preskip_hashes: set[str] = set()
        for hash_batch in _iter_in_batches(all_output_hashes):
            preskip_hashes.update(
                ctx.session.execute(
                    select(FirstSeenIndex.content_hash).where(
                        FirstSeenIndex.content_hash.in_(hash_batch),
                        FirstSeenIndex.duplicate_count >= _preskip_dup_threshold,
                    )
                ).scalars().all()
            )

        # Detect stealer type: first try SystemInfo.txt self-identification (highest confidence)
        all_filenames = [o.output_filename for o in outputs]
        sysinfo_stealer: str | None = None
        for output in outputs:
            if is_system_info_file(output.output_filename):
                sysinfo_path = Path(output.output_path)
                if sysinfo_path.exists():
                    try:
                        sysinfo = parse_system_info(sysinfo_path.read_text(errors="replace"))
                        sysinfo_stealer = sysinfo.stealer_name
                        try:
                            _save_system_info(ctx.session, job.id, sysinfo)
                        except Exception as e:
                            logger.warning("Could not save SystemInfo for job %d: %s", job.id, e)
                    except Exception as e:
                        logger.debug("Could not parse SystemInfo for job %d: %s", job.id, e)
                    break  # Only need one SystemInfo file
        stealer_type = detect_stealer_type(all_filenames, sysinfo_stealer=sysinfo_stealer)

        archive_domain_counts: Counter[str] = Counter()

        # Parse each credential file
        for file_idx, output in enumerate(credential_outputs):
            # Yield to event loop every 50 files so prefetch downloads can progress
            if file_idx % 50 == 0:
                await asyncio.sleep(0)

            file_path = Path(output.output_path)
            file_path_str = str(file_path)

            if not file_path.exists():
                logger.warning("Credential file missing: %s", file_path)
                if resume_file == file_path_str:
                    # The marked file is gone: nothing left to resume.
                    _clear_parse_marker(ctx.session, job.id)
                    resume_file = None
                continue

            # The parse mode must be known BEFORE the resume decision: the
            # parallel and sequential paths yield different (each deterministic)
            # credential sequences, so a resume count is only valid for the
            # mode that produced it.
            try:
                file_size = file_path.stat().st_size
            except OSError:
                file_size = 0
            workers = self._parallel_worker_count()
            use_parallel = file_size >= _PARALLEL_PARSE_MIN_BYTES and workers >= 1
            file_mode = "parallel" if use_parallel else "sequential"

            resume_count = 0
            if resume_file == file_path_str:
                resume_seen = True
                if (
                    resume_payload
                    and resume_payload.get("mode") == file_mode
                    and resume_payload.get("hash")
                    and output.output_hash
                    and resume_payload["hash"] == output.output_hash
                ):
                    resume_count = int(resume_payload.get("resume") or 0)
                    if resume_count:
                        logger.info(
                            "Resuming %s from credential %d (mode %s)",
                            file_path.name,
                            resume_count,
                            file_mode,
                        )

            # Check if we've already parsed this file (bypassed when a previous
            # early-skip means the file's tail is still unparsed, or when this
            # file carries a crash-resume marker).
            if (
                not force_full
                and resume_file != file_path_str
                and file_path_str in parsed_source_files
            ):
                logger.debug("Already parsed: %s", file_path)
                continue

            # Pre-skip when content_hash has been seen ≥3 times across archives.
            # The marked resume file is never pre-skipped: its marker must be
            # consumed and cleared.
            if (
                resume_file != file_path_str
                and output.output_hash
                and output.output_hash in preskip_hashes
            ):
                logger.info(
                    "Pre-skip (content seen ≥%d times): %s",
                    _preskip_dup_threshold + 1, output.output_filename,
                )
                continue

            # Record the in-progress file durably before the first row is
            # inserted: if this process is hard-killed mid-file, startup
            # recovery resumes it from the persisted credential count instead
            # of re-probing the already-committed prefix.
            _set_parse_marker(
                ctx.session,
                job.id,
                file_path_str,
                resume=resume_count,
                mode=file_mode,
                output_hash=output.output_hash,
            )

            # Stream credentials in batches to keep memory bounded.
            # Each batch is: compute hashes → DB dedup check → insert → flush → discard.
            # Cross-batch deduplication is handled by the DB hash check; seen_in_batch
            # only deduplicates within the current batch.
            batch: list = []
            # Sequence position: every yielded credential (including the ones
            # skipped by a resume) advances this, so the checkpoint value is
            # always a valid resume count for the same mode.
            file_cred_count = 0

            async def _flush_batch(b: list) -> tuple[int, int]:
                """Process one batch with bulk insert semantics."""
                if not b:
                    return 0, 0

                # Hot-loop locals: avoid attribute lookups per credential.
                _hash = ParsedCredential.compute_hash
                _soft_hash = ParsedCredential.compute_soft_hash if has_soft_hash_column else None
                _job_id = job.id
                _file_path_str = str(file_path)
                _source_archive = job.group.base_name if job.group else None
                _src_conv = output.source_conversation_id
                _src_msg = output.source_message_id
                _stealer = truncate_field(stealer_type, 50)

                rows: list[dict[str, object]] = []
                rows_append = rows.append

                if isinstance(b[0], tuple):
                    # Parallel path: workers already NUL-stripped, truncated and
                    # hashed; just assemble COPY rows.
                    for t in b:
                        (url_val, domain_trunc, user_val, pass_val,
                         email_domain_val, app_val, prof_val, h, soft_h) = t
                        row = {
                            "url": url_val,
                            "domain": domain_trunc,
                            "username": user_val,
                            "password": pass_val,
                            "email_domain": email_domain_val,
                            "application": app_val,
                            "profile": prof_val,
                            "extraction_job_id": _job_id,
                            "source_file": _file_path_str,
                            "source_archive": _source_archive,
                            "source_conversation_id": _src_conv,
                            "source_message_id": _src_msg,
                            "stealer_type": _stealer,
                            "credential_hash": h,
                        }
                        if has_soft_hash_column:
                            row["soft_credential_hash"] = soft_h
                        rows_append(row)
                else:
                    for cred in b:
                        # Inline truncate_field: NUL-byte check + slice. Field-level
                        # truncate_field call overhead is significant at 500/sec ×
                        # ~150M rows; inlining trims function call + arg packing
                        # cost from the inner loop.
                        d = cred.domain
                        if d:
                            if "\x00" in d:
                                d = d.replace("\x00", "")
                            domain_trunc = d[:255]
                        else:
                            domain_trunc = None
                        u = cred.url
                        if u:
                            if "\x00" in u:
                                u = u.replace("\x00", "")
                            url_val = u[:1024]
                        else:
                            url_val = ""
                        un = cred.username
                        if un:
                            if "\x00" in un:
                                un = un.replace("\x00", "")
                            user_val = un[:255]
                        else:
                            user_val = ""
                        pw = cred.password
                        if pw:
                            if "\x00" in pw:
                                pw = pw.replace("\x00", "")
                            pass_val = pw[:255]
                        else:
                            pass_val = ""
                        domain_or_url = cred.domain or cred.url or ""
                        if domain_or_url:
                            if "\x00" in domain_or_url:
                                _dou_clean = domain_or_url.replace("\x00", "")
                                domain_val = _dou_clean[:255]
                            else:
                                domain_val = domain_or_url[:255]
                        else:
                            domain_val = ""
                        _ed = cred.email_domain
                        if _ed:
                            if "\x00" in _ed:
                                _ed = _ed.replace("\x00", "")
                            email_domain_val = _ed[:255]
                        else:
                            email_domain_val = None
                        _app = cred.application
                        if _app:
                            if "\x00" in _app:
                                _app = _app.replace("\x00", "")
                            app_val = _app[:100]
                        else:
                            app_val = None
                        _prof = cred.profile
                        if _prof:
                            if "\x00" in _prof:
                                _prof = _prof.replace("\x00", "")
                            prof_val = _prof[:100]
                        else:
                            prof_val = None
                        row = {
                            "url": url_val,
                            "domain": domain_trunc,
                            "username": user_val,
                            "password": pass_val,
                            "email_domain": email_domain_val,
                            "application": app_val,
                            "profile": prof_val,
                            "extraction_job_id": _job_id,
                            "source_file": _file_path_str,
                            "source_archive": _source_archive,
                            "source_conversation_id": _src_conv,
                            "source_message_id": _src_msg,
                            "stealer_type": _stealer,
                            "credential_hash": _hash(domain_val, user_val, pass_val),
                        }
                        if _soft_hash is not None:
                            row["soft_credential_hash"] = _soft_hash(
                                domain_or_url, user_val, pass_val
                            )
                        rows_append(row)

                # Yield before the blocking INSERT so concurrent tasks (prefetch
                # downloads, progress heartbeat) get a chance to run.
                await asyncio.sleep(0)
                # These six provenance fields are identical for every row in
                # this flush (they change only per output file); letting the
                # COPY path escape them once per chunk removes 6 of 15
                # per-row escape+join operations. Values match the rows built
                # above exactly.
                inserted_rows = self._bulk_insert_credentials(
                    ctx,
                    rows,
                    constants={
                        "extraction_job_id": _job_id,
                        "source_file": _file_path_str,
                        "source_archive": _source_archive,
                        "source_conversation_id": _src_conv,
                        "source_message_id": _src_msg,
                        "stealer_type": _stealer,
                    },
                )
                for row in inserted_rows:
                    domain = row.get("domain")
                    if isinstance(domain, str) and domain:
                        archive_domain_counts[domain] += 1

                new_count = len(inserted_rows)
                dup_count = len(rows) - new_count
                return new_count, dup_count

            batches_since_commit = 0
            # Early-exit heuristic for duplicate-heavy files: stealer logs are
            # reposted across channels, so a file whose first batches are
            # ~all duplicates is almost certainly content we already parsed.
            # Aborting after a few high-dup batches avoids spending minutes on
            # per-row index lookups against the large credential_hash index
            # for rows that will all be rejected anyway.
            dup_batches_seen = 0
            dup_confirm_batches = 3  # consecutive batches above the threshold
            file_skipped_as_dup = False

            # Large files (>20MB) are parsed in parallel worker processes via
            # _iter_parallel_credentials; small files keep the sequential path
            # (lower overhead, and the tests exercise that path directly).
            # file_size/workers/mode were resolved above (before the resume
            # decision) so the parallel branch reuses them.
            # The mode actually producing the yields (the parallel fallback
            # switches it) and the resume count valid for that mode.
            active_mode = file_mode
            seq_skip = resume_count

            def _checkpoint() -> None:
                """Advance the crash-resume marker AFTER the row commit.

                Post-commit so the marker can never point past durable rows.
                """
                _update_parse_marker(
                    ctx.session,
                    job.id,
                    resume=file_cred_count,
                    mode=active_mode,
                    output_hash=output.output_hash,
                )

            async def _sequential_parse() -> None:
                nonlocal batch, file_cred_count, credentials_found
                nonlocal duplicates_found, batches_since_commit
                nonlocal dup_batches_seen, file_skipped_as_dup
                nonlocal seq_skip
                for cred in iter_credentials_file(file_path):
                    if file_skipped_as_dup:
                        break
                    file_cred_count += 1
                    if seq_skip > 0:
                        # Already committed by a previous attempt; the
                        # sequential parser is deterministic for an unchanged
                        # file, so counting skips the exact committed prefix.
                        seq_skip -= 1
                        continue
                    batch.append(cred)

                    if len(batch) >= _BATCH_SIZE:
                        new, dups = await _flush_batch(batch)
                        credentials_found += new
                        duplicates_found += dups
                        if ctx.display:
                            ctx.display.update_counts(
                                ctx.credentials_parsed + credentials_found,
                                ctx.duplicates_skipped + duplicates_found,
                            )
                        batch = []
                        batches_since_commit += 1
                        if batches_since_commit >= 2:
                            ctx.session.commit()
                            batches_since_commit = 0
                            _checkpoint()
                        await asyncio.sleep(0)
                        if not force_full and _is_dup_batch(new, dups, _BATCH_SIZE):
                            dup_batches_seen += 1
                            if dup_batches_seen >= dup_confirm_batches:
                                file_skipped_as_dup = True
                                logger.info(
                                    "Early-skip %s: %d consecutive batches at %.0f%% duplicates",
                                    file_path.name, dup_batches_seen, (dups / (new + dups)) * 100,
                                )
                        else:
                            dup_batches_seen = 0

            # workers >= 1: even a single worker process overlaps the CPU-bound regex +
            # hashing with the main process's DB inserts — the guard must not
            # be `> 1`, which silently fell back to the fully-serialized
            # sequential path whenever TELECRIME_PARSE_WORKERS=1.
            if use_parallel:
                try:
                    logger.info(
                        "Parsing %s in parallel (%d workers, %.1f MB)",
                        file_path.name, workers, file_size / 1024 / 1024,
                    )
                    skip_remaining = resume_count
                    async for tup in self._iter_parallel_credentials(
                        file_path, file_path_str, workers
                    ):
                        if file_skipped_as_dup:
                            break
                        file_cred_count += 1
                        if skip_remaining > 0:
                            # Already committed by a previous attempt; the
                            # parallel yield sequence is deterministic for the
                            # same file/mode, so counting skips the exact
                            # committed prefix without a DB probe.
                            skip_remaining -= 1
                            continue
                        batch.append(tup)

                        if len(batch) >= _BATCH_SIZE:
                            new, dups = await _flush_batch(batch)
                            credentials_found += new
                            duplicates_found += dups
                            if ctx.display:
                                ctx.display.update_counts(
                                    ctx.credentials_parsed + credentials_found,
                                    ctx.duplicates_skipped + duplicates_found,
                                )
                            batch = []
                            batches_since_commit += 1
                            if batches_since_commit >= 2:
                                ctx.session.commit()
                                batches_since_commit = 0
                                _checkpoint()
                            await asyncio.sleep(0)
                            if not force_full and _is_dup_batch(new, dups, _BATCH_SIZE):
                                dup_batches_seen += 1
                                if dup_batches_seen >= dup_confirm_batches:
                                    file_skipped_as_dup = True
                                    logger.info(
                                        "Early-skip %s: %d consecutive batches at %.0f%% duplicates",
                                        file_path.name, dup_batches_seen,
                                        (dups / (new + dups)) * 100,
                                    )
                            else:
                                dup_batches_seen = 0
                except Exception as exc:
                    # Parallel pool broke (e.g. spawn workers OOM-killed on a
                    # RAM-tight host -> BrokenProcessPool or 120s no-progress).
                    # Already-parsed rows live in the DB (dedup), so re-parsing
                    # the file sequentially is safe, not duplicated.
                    logger.warning(
                        "Parallel parse of %s failed (%s) — falling back to "
                        "sequential parse",
                        file_path.name, exc,
                    )
                    # Discard any pre-processed tuples left in the batch from
                    # the parallel path: _flush_batch dispatches on the first
                    # item's type, and the sequential fallback appends
                    # Credential objects — a mixed batch would crash the flush.
                    # The fallback re-parses from the START in sequential mode,
                    # so the parallel resume prefix does not apply: reset the
                    # counters and checkpoint under the new mode only after its
                    # first commit (an under-count is always safe).
                    batch = []
                    file_cred_count = 0
                    seq_skip = 0
                    active_mode = "sequential"
                    await _sequential_parse()
            else:
                await _sequential_parse()

            # Flush any remaining credentials
            new, dups = await _flush_batch(batch)
            credentials_found += new
            duplicates_found += dups
            if ctx.display:
                ctx.display.update_creds(ctx.credentials_parsed + credentials_found)
            # Commit after each file to release lock promptly
            ctx.session.commit()
            _checkpoint()

            if file_skipped_as_dup:
                # The confidence heuristic stopped early; the file's tail may
                # hold first-seen credentials. Finalize must not delete the
                # group, and the next run must re-parse the whole file: drop
                # this file's rows so the per-file pre-skip does not treat it
                # as complete (any existing row otherwise skips the file).
                removed = 0
                try:
                    removed = cast(
                        CursorResult,
                        ctx.session.execute(
                            delete(ParsedCredential).where(
                                ParsedCredential.extraction_job_id == job.id,
                                ParsedCredential.source_file == str(file_path),
                            )
                        ),
                    ).rowcount
                    ctx.session.commit()
                except Exception as exc:
                    try:
                        ctx.session.rollback()
                    except Exception:
                        pass
                    logger.warning(
                        "Could not clear partial rows for early-skipped %s: %s",
                        file_path.name,
                        exc,
                    )
                incomplete = True
                # Persist a marker so the next run parses this job's files
                # fully (force_full) — otherwise the deterministic duplicate
                # prefix makes every retry early-skip at the same point,
                # deleting the rows again and never parsing the tail.
                job.last_error_code = _EARLY_SKIP_CODE
                try:
                    ctx.session.commit()
                except Exception as exc:
                    try:
                        ctx.session.rollback()
                    except Exception:
                        pass
                    logger.warning(
                        "Could not persist early-skip marker for %s: %s",
                        file_path.name,
                        exc,
                    )
                logger.warning(
                    "Early-skip left %s incompletely parsed (%d partial rows removed); "
                    "group %s kept for re-parse (next run parses fully)",
                    file_path.name,
                    removed or 0,
                    job.group_id,
                )
            elif force_full and job.last_error_code == _EARLY_SKIP_CODE:
                # Full parse of this job completed; the early-skip retry
                # marker served its purpose.
                job.last_error_code = None
                ctx.session.commit()

            # File fully consumed (or its partial rows intentionally cleaned by
            # the early-skip branch): the marker has served its purpose.
            _clear_parse_marker(ctx.session, job.id)

            if file_cred_count:
                logger.info(
                    "Parsed %d credentials from %s (%d new, %d dups)",
                    file_cred_count,
                    output.output_filename,
                    credentials_found,
                    duplicates_found,
                )

        if resume_file and not resume_seen:
            # The marked file is not among this job's credential outputs
            # (removed or renamed): drop the stale marker so it cannot keep
            # blocking the pre-skip for this job on every run.
            _clear_parse_marker(ctx.session, job.id)

        # Send one notification per archive (not per file)
        if ctx.notifier and (credentials_found or duplicates_found):
            archive_name = f"job_{job.id}"
            if job.group and job.group.base_name:
                archive_name = job.group.base_name
            top_domains = archive_domain_counts.most_common(5)
            await ctx.notifier.archive_parsed(
                archive_name=archive_name,
                new_credentials=credentials_found,
                duplicates=duplicates_found,
                unique_domains=len(archive_domain_counts),
                top_domains=top_domains,
            )

        return credentials_found, duplicates_found, incomplete

    def _bulk_insert_credentials(
        self,
        ctx: PipelineContext,
        rows: list[dict[str, object]],
        constants: dict[str, object] | None = None,
    ) -> list[dict[str, object]]:
        """Insert a credential batch and return the rows that were newly inserted.

        Uses PostgreSQL `COPY ... FROM STDIN` into a temp staging table, then
        `INSERT ... SELECT ... ON CONFLICT (credential_hash) DO NOTHING` to
        dedupe against the live table. Benchmarks ~4× faster end-to-end than
        the previous chunked INSERT-VALUES path (12K rows/sec vs 2.7K on the
        live schema with all GIN indexes present). Falls back to the slower
        path on SQLite (test fixtures) since COPY is PG-specific.

        ``constants`` optionally carries field values that are identical for
        every row of the batch (the parse hot path's per-file provenance
        fields) so the COPY path can escape them once per chunk instead of
        once per row. Output is byte-identical to leaving it unset; the
        SQLite fallback ignores it.

        Each chunk runs inside its own SAVEPOINT so a failure of one chunk only
        discards that chunk — previously inserted chunks remain durable in the
        enclosing transaction and the reported count matches reality.
        """
        if not rows:
            return []

        if ctx.session.get_bind().dialect.name == "postgresql":
            return self._bulk_insert_via_copy(ctx, rows, constants)
        return self._bulk_insert_via_values(ctx, rows)

    # Columns used by the COPY path. Order must match the COPY column list in
    # `_bulk_insert_via_copy` and the destination INSERT-SELECT.
    _COPY_FIELDS: tuple[str, ...] = (
        "url", "domain", "username", "password",
        "email_domain", "application", "profile",
        "extraction_job_id", "source_file", "source_archive",
        "source_conversation_id", "source_message_id",
        "stealer_type", "credential_hash",
    )

    @staticmethod
    def _copy_escape(value: object) -> str:
        """Escape a single field for PostgreSQL COPY text format."""
        if value is None:
            return "\\N"
        s = str(value)
        if not _COPY_ESCAPE_RE.search(s):
            return s
        # COPY text-mode requires escaping these control bytes.
        return (
            s.replace("\\", "\\\\")
             .replace("\t", "\\t")
             .replace("\n", "\\n")
             .replace("\r", "\\r")
        )

    def _bulk_insert_via_copy(
        self,
        ctx: PipelineContext,
        rows: list[dict[str, object]],
        constants: dict[str, object] | None = None,
    ) -> list[dict[str, object]]:
        """COPY-into-staging + INSERT-SELECT ON CONFLICT (PostgreSQL-only)."""
        fields = self._COPY_FIELDS
        soft_field = "soft_credential_hash" if ctx.has_soft_hash_column else None
        if soft_field:
            fields = fields + (soft_field,)
        col_list = ", ".join(fields)

        inserted: list[dict[str, object]] = []

        for i in range(0, len(rows), _INSERT_CHUNK_SIZE):
            chunk = rows[i : i + _INSERT_CHUNK_SIZE]
            try:
                inserted.extend(
                    self._copy_insert_chunk(ctx, chunk, fields, col_list, constants)
                )
            except Exception as exc:
                logger.error(
                    "Credential COPY chunk failed after retry; aborting parse instead of "
                    "silently dropping %d credentials: %s",
                    len(chunk),
                    exc,
                )
                raise RuntimeError(
                    f"Credential COPY chunk failed after retry ({len(chunk)} rows)"
                ) from exc

        return inserted

    def _copy_insert_chunk(
        self,
        ctx: PipelineContext,
        chunk: list[dict[str, object]],
        fields: tuple[str, ...],
        col_list: str,
        constants: dict[str, object] | None = None,
    ) -> list[dict[str, object]]:
        """COPY-insert one chunk inside a savepoint; halve-and-retry on failure.

        A failed chunk is split in half and each half retried recursively so a
        single pathological row (or a timeout on a large anti-join) cannot sink
        the whole 50K chunk. Halving continues until a singleton fails, which
        raises — the caller aborts the parse instead of silently dropping rows
        (the job-wide delete-on-failure semantics stay with the caller).
        """
        savepoint = ctx.session.begin_nested()
        try:
            raw_conn = ctx.session.connection().connection
            cursor = raw_conn.cursor()
            try:
                # The DB server default is statement_timeout=5min and the
                # session-level SET (in _apply_pg_bulk_settings) is lost
                # when the pooled connection is recycled between commits.
                # Re-apply on the RAW connection that actually runs the
                # COPY/INSERT. A generous 10-min bound (not 0) still
                # allows legitimately slow disk-bound batches while
                # auto-cancelling the pathological 11+ minute anti-join
                # INSERTs (cold index reads on a 200M+ row table); the
                # chunk savepoint + halve-retry + abort-on-persistent-error
                # path keeps that safe — no rows are lost.
                cursor.execute("SET statement_timeout = 600000")
                cursor.execute("SET lock_timeout = 0")
                cursor.execute("SET synchronous_commit = off")
                # Session-level SETs from _apply_pg_bulk_settings are
                # lost when the pooled connection is recycled between
                # commits — re-apply on the raw connection too. A
                # default 32MB gin_pending_list_limit means 4x more
                # GIN flush stalls on the 23GB username trigram index.
                cursor.execute("SET gin_pending_list_limit = 134217728")
                cursor.execute("SET maintenance_work_mem = '1GB'")
                cursor.execute("SET work_mem = '64MB'")
                # Reuse the staging temp table across chunks within the
                # same transaction for fewer DDL ticks.
                cursor.execute(
                    "CREATE TEMP TABLE IF NOT EXISTS _pc_staging "
                    "(LIKE parsed_credentials INCLUDING DEFAULTS) "
                    "ON COMMIT DROP"
                )
                cursor.execute("TRUNCATE _pc_staging")
                # No unique index on the staging table: the Python-side
                # _seen_hashes set already drops duplicate credential
                # hashes before COPY, and PostgreSQL treats NULLs as
                # distinct, so the index never rejects anything. The
                # INSERT-SELECT below only needs the staging rows.

                buf = io.StringIO()
                write = buf.write
                esc = self._copy_escape
                # Per-file provenance fields (job id, source file/archive,
                # source ids, stealer type) are identical for every row of a
                # flush. Escaping them once per chunk instead of once per row
                # removes 6/15 of the escape+join work from the row loop.
                # Only used when they occupy one contiguous run of `fields`,
                # so concatenation order (and thus the COPY payload) is
                # byte-identical; otherwise the loop escapes every field.
                constant_middle: str | None = None
                prefix_fields: tuple[str, ...] = ()
                suffix_fields: tuple[str, ...] = ()
                if constants:
                    positions = [i for i, f in enumerate(fields) if f in constants]
                    if positions and positions == list(
                        range(positions[0], positions[-1] + 1)
                    ):
                        start, end = positions[0], positions[-1]
                        prefix_fields = fields[:start]
                        suffix_fields = fields[end + 1 :]
                        constant_middle = "\t".join(
                            esc(constants[f]) for f in fields[start : end + 1]
                        )
                # In-chunk dedup: repeated credential_hash rows (the
                # same victim's log line appearing twice in one file)
                # violate _pc_staging_hash during COPY — PostgreSQL
                # treats a COPY constraint violation as a hard error
                # that discards the WHOLE chunk. Deduplicating here
                # keeps the batch intact; dropped rows still count as
                # duplicates in the caller (dup_count = rows - new).
                _seen_hashes: set[str] = set()
                for row in chunk:
                    _h = row.get("credential_hash")
                    if isinstance(_h, str):
                        if _h in _seen_hashes:
                            continue
                        _seen_hashes.add(_h)
                    if constant_middle is not None:
                        if prefix_fields:
                            write("\t".join([esc(row.get(f)) for f in prefix_fields]))
                            write("\t")
                        write(constant_middle)
                        for f in suffix_fields:
                            write("\t")
                            write(esc(row.get(f)))
                        write("\n")
                    else:
                        write("\t".join([esc(row.get(f)) for f in fields]))
                        write("\n")
                buf.seek(0)
                cursor.copy_expert(
                    f"COPY _pc_staging ({col_list}) FROM STDIN",
                    buf,
                )

                # Two-stage dedup:
                #  1. Anti-join against a compact hash index to reject
                #     the bulk of duplicates in one pass. The 64-bit
                #     expression index is more cache-friendly than
                #     probing the full credential hash index for every
                #     row.
                #  2. ON CONFLICT (credential_hash) stays as the exact
                #     correctness backstop for hash collisions (risk
                #     collision risk remains negligible for the
                #     prefilter's purpose).
                # NULL credential_hashes pass straight through.
                # The SELECT list must be qualified with the staging
                # alias: both tables have url/domain/... and unqualified
                # references raise "AmbiguousColumn" and can make
                # every insert chunk fail.
                # NOT EXISTS (instead of LEFT JOIN ... OR p.id IS NULL)
                # lets the planner use an anti-join with the ix_pc_hash64
                # expression index; the LEFT JOIN variant degrades to a
                # Seq Scan of the whole parsed_credentials table per
                # chunk and can exceed the database statement timeout.
                _sel = ", ".join(f"s.{f}" for f in fields)
                if _has_hash64_index(ctx.session.get_bind()):
                    cursor.execute(
                        f"INSERT INTO parsed_credentials ({col_list}) "
                        f"SELECT {_sel} FROM _pc_staging s "
                        "WHERE s.credential_hash IS NULL "
                        "   OR NOT EXISTS (SELECT 1 FROM parsed_credentials p "
                        f"      WHERE {_hash64_expr('p')} = {_hash64_expr('s')}) "
                        "ON CONFLICT (credential_hash) DO NOTHING "
                        "RETURNING credential_hash, domain"
                    )
                else:
                    cursor.execute(
                        f"INSERT INTO parsed_credentials ({col_list}) "
                        f"SELECT {_sel} FROM _pc_staging s "
                        "WHERE s.credential_hash IS NULL "
                        "   OR NOT EXISTS (SELECT 1 FROM parsed_credentials p "
                        "      WHERE left(p.credential_hash, 32) = left(s.credential_hash, 32)) "
                        "ON CONFLICT (credential_hash) DO NOTHING "
                        "RETURNING credential_hash, domain"
                    )
                rows_returned = cursor.fetchall()
            finally:
                cursor.close()
            savepoint.commit()
            # Drain the trigram GIN pending lists OUTSIDE the timed INSERT:
            # pending entries accumulate across chunks (the transaction commits
            # per archive, not per chunk) and the NEXT INSERT-SELECT triggers an
            # inline merge that can blow the 10-min statement timeout — the
            # "Credential COPY chunk failed ... retrying" storm. Cleaning here
            # keeps the pending list near-empty so inserts stay fast. Best-
            # effort: a failed clean is a performance issue, not correctness.
            # IMPORTANT: run it inside its OWN savepoint — an error here would
            # otherwise abort the ENTIRE outer transaction, discarding the
            # just-committed chunk.
            if _trigram_indexes_present(ctx.session):
                try:
                    drain_sp = ctx.session.begin_nested()
                    try:
                        cursor = ctx.session.connection().connection.cursor()
                        try:
                            cursor.execute(
                                "SELECT gin_clean_pending_list('ix_pc_username_trgm'), "
                                "gin_clean_pending_list('ix_pc_domain_trgm')"
                            )
                            cursor.fetchall()
                        finally:
                            cursor.close()
                        drain_sp.commit()
                    except Exception:
                        drain_sp.rollback()
                except Exception:
                    pass
            return [
                {"credential_hash": credential_hash, "domain": domain}
                for credential_hash, domain in rows_returned
            ]
        except Exception as exc:
            try:
                savepoint.rollback()
            except Exception:
                pass
            if len(chunk) == 1:
                logger.warning(
                    "Credential COPY single row failed (%s): %s — continuing",
                    type(exc).__name__, exc,
                )
                raise RuntimeError(
                    f"Credential COPY single row failed ({len(chunk)} row)"
                ) from exc
            half = len(chunk) // 2
            logger.info(
                "Credential COPY chunk failed (%s: %s) — splitting %d rows in half and retrying",
                type(exc).__name__, exc, len(chunk),
            )
            first_half = self._copy_insert_chunk(
                ctx, chunk[:half], fields, col_list, constants
            )
            second_half = self._copy_insert_chunk(
                ctx, chunk[half:], fields, col_list, constants
            )
            return first_half + second_half

    def _bulk_insert_via_values(
        self,
        ctx: PipelineContext,
        rows: list[dict[str, object]],
        constants: dict[str, object] | None = None,
    ) -> list[dict[str, object]]:
        """Legacy chunked INSERT-VALUES path, used by SQLite test fixtures."""
        # SQLite has no COPY; the pre-escaped constants optimization is
        # PostgreSQL-only and the value rows are already assembled.
        del constants
        inserted: list[dict[str, object]] = []
        dialect_insert = get_dialect_insert(ctx.session)

        # SQLite has a 999 SQL-variable limit per statement; keep chunks small.
        for i in range(0, len(rows), _INSERT_CHUNK_SIZE_SQLITE):
            chunk = rows[i : i + _INSERT_CHUNK_SIZE_SQLITE]
            insert_stmt = (
                dialect_insert(ParsedCredential)
                .values(chunk)
                .on_conflict_do_nothing(index_elements=["credential_hash"])
                .returning(ParsedCredential.credential_hash, ParsedCredential.domain)
            )
            savepoint = ctx.session.begin_nested()
            try:
                result = ctx.session.execute(insert_stmt)
                rows_returned = result.fetchall()
                savepoint.commit()
                for credential_hash, domain in rows_returned:
                    inserted.append(
                        {
                            "credential_hash": credential_hash,
                            "domain": domain,
                        }
                    )
            except Exception as exc:
                logger.warning(
                    "Credential chunk insert failed (%s): %s — continuing",
                    type(exc).__name__,
                    exc,
                )
                try:
                    savepoint.rollback()
                except Exception:
                    raise

        return inserted
