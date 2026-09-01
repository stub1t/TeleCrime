"""Stage: Parse - parse extracted stealer log files for credentials."""

import asyncio
import io
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

from sqlalchemy import inspect, select, text
from sqlalchemy.orm import selectinload

from telecrime.database import get_dialect_insert
from telecrime.models import ArchiveGroup, ExtractionJob, ParsedCredential
from telecrime.models.system_info import SystemInfoRecord
from telecrime.pipeline.orchestrator import PipelineContext, PipelineStage
from telecrime.states import ExtractionStatus, GroupStatus
from telecrime.stealer.parser import (
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


def _iter_line_chunks(
    fh,
    chunk_lines: int = _PARALLEL_CHUNK_LINES,
) -> Iterator[list[str]]:
    """Yield lists of lines, cut only at blank/separator block boundaries.

    Splitting mid-block would let a labeled credential fall across two workers
    and be lost. We accumulate lines and only cut once we have at least
    `chunk_lines` buffered AND the current line is a blank or
    ``---``/``===``/``___`` separator. If no boundary appears before 4×
    `chunk_lines` the file is a line-independent combo list (or degenerate), so
    a hard cut there is safe.
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
            yield buf
            buf = []
    if buf:
        yield buf


def _parse_lines_chunk_worker(args: tuple[list[str], str]) -> list:
    """Worker entry point: parse a chunk of lines into pre-processed tuples.

    Kept at module level so the ProcessPoolExecutor can pickle it. The worker
    does the NUL-strip/truncation AND both SHA-256 hashes (the CPU-bound half
    of the row pipeline) so the main process only assembles COPY rows.
    Tuple shape mirrors what the sequential path builds in _flush_batch:
    (url, domain_row, username, password, email_domain, application, profile,
     credential_hash, soft_credential_hash). soft_credential_hash input is
    the domain-or-url (untruncated) exactly like the sequential path.
    """
    lines, source_file = args
    out = []
    for c in parse_credential_lines(iter(lines), source_file):
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
        domain_or_url = c.domain or c.url or ""
        if "\x00" in domain_or_url:
            domain_or_url = domain_or_url.replace("\x00", "")
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


def _reset_pg_bulk_settings(session) -> None:
    try:
        session.execute(text("SET synchronous_commit = on"))
        session.execute(text("SET statement_timeout = DEFAULT"))
        session.execute(text("SET gin_pending_list_limit = DEFAULT"))
        session.execute(text("SET maintenance_work_mem = DEFAULT"))
    except Exception:
        pass


def _is_dup_batch(new_count: int, dup_count: int, batch_size: int) -> bool:
    """True when a batch is dominated by duplicates (early-skip signal)."""
    total = new_count + dup_count
    return total >= batch_size // 2 and (dup_count / total) >= 0.95


def _hash64_expr(alias: str) -> str:
    """SQL expression for the compact 64-bit credential-hash fingerprint.

    First 16 hex chars of the SHA256 credential_hash cast to bigint. Matches
    the ix_pc_hash64 expression index exactly.
    """
    return f"CAST((CAST((chr(120) || substring({alias}.credential_hash, 1, 16)) AS bit(64))) AS bigint)"


_HAS_HASH64: bool | None = None  # resolved lazily against the live schema


def _has_hash64_index(engine) -> bool:
    """True when ix_pc_hash64 (compact dedup index) exists on parsed_credentials."""
    global _HAS_HASH64
    if _HAS_HASH64 is not None:
        return _HAS_HASH64
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT 1 FROM pg_indexes WHERE tablename='parsed_credentials' AND indexname='ix_pc_hash64'")
            ).fetchone()
        _HAS_HASH64 = row is not None
    except Exception:
        _HAS_HASH64 = False
    return _HAS_HASH64


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
        dedicated producer thread submits chunks to the pool with bounded
        in-flight futures (≤ 2×workers) and pushes *completed* futures onto an
        asyncio.Queue; this async generator drains the queue in submission
        order, keeping memory bounded and never blocking the event loop.

        Yields pre-processed tuples (url, domain, username, password,
        email_domain, application, profile, credential_hash, soft_hash) so the
        caller's batch-flush hot loop assembles COPY rows directly.
        """
        fh = _open_credential_file(file_path, "utf-8")
        if fh is None:
            return

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

        def _read_chunks() -> None:
            try:
                for chunk in _iter_line_chunks(fh):
                    while not stop_event.is_set():
                        try:
                            chunks_q.put(chunk, timeout=0.5)
                            break
                        except _queue.Full:
                            continue
                    if stop_event.is_set():
                        break
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

        with ProcessPoolExecutor(max_workers=workers, mp_context=ctx) as pool:
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
                    # blocking the event loop (get_nowait + short sleeps).
                    while len(in_flight) < workers * 2 and submitting:
                        try:
                            chunk = chunks_q.get_nowait()
                        except _queue.Empty:
                            break
                        if chunk is sentinel:
                            submitting = False
                            break
                        in_flight.append(
                            pool.submit(_parse_lines_chunk_worker, (chunk, source_file))
                        )
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
                    # Poll for completed futures without ever blocking the loop
                    # on a threaded wait() that could re-enter pool locks.
                    await asyncio.sleep(0.05)
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

    async def run(self, ctx: PipelineContext) -> bool:
        """Parse credential files from successful extractions."""
        logger.info("Starting credential parsing")

        jobs = self._get_jobs(ctx)

        if not jobs:
            logger.info("No completed extractions to parse")
            return True

        total_credentials = 0
        total_duplicates = 0

        if ctx.has_soft_hash_column is None:
            db_columns = {
                col["name"]
                for col in inspect(ctx.session.get_bind()).get_columns("parsed_credentials")
            }
            ctx.has_soft_hash_column = "soft_credential_hash" in db_columns

        _apply_pg_bulk_settings(ctx.session)

        try:
            for job in jobs:
                try:
                    creds_found, dups_found = await self._parse_job_outputs(
                        ctx, job, ctx.has_soft_hash_column
                    )
                    total_credentials += creds_found
                    total_duplicates += dups_found
                    ctx.credentials_parsed += creds_found
                    ctx.duplicates_skipped += dups_found
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
                        ctx.parse_failed_group_ids.add(job.group_id)
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
        jobs = self._get_jobs(ctx, group_id=group_id)
        if not jobs:
            return 0, 0

        if ctx.has_soft_hash_column is None:
            db_columns = {
                col["name"]
                for col in inspect(ctx.session.get_bind()).get_columns("parsed_credentials")
            }
            ctx.has_soft_hash_column = "soft_credential_hash" in db_columns

        _apply_pg_bulk_settings(ctx.session)

        total_credentials = 0
        total_duplicates = 0
        try:
            for job in jobs:
                creds_found, dups_found = await self._parse_job_outputs(
                    ctx, job, ctx.has_soft_hash_column
                )
                total_credentials += creds_found
                total_duplicates += dups_found
                ctx.credentials_parsed += creds_found
                ctx.duplicates_skipped += dups_found
        finally:
            _reset_pg_bulk_settings(ctx.session)

        return total_credentials, total_duplicates

    def _get_jobs(self, ctx: PipelineContext, group_id: int | None = None) -> list[ExtractionJob]:
        """Return completed extraction jobs whose groups are still EXTRACTED."""
        query = (
            select(ExtractionJob)
            .join(ArchiveGroup)
            .where(
                ExtractionJob.status == ExtractionStatus.COMPLETED,
                ArchiveGroup.status == GroupStatus.EXTRACTED,
            )
            .options(
                selectinload(ExtractionJob.outputs),
                selectinload(ExtractionJob.group),
            )
        )
        if group_id is not None:
            query = query.where(ArchiveGroup.id == group_id)
        return list(ctx.session.execute(query).scalars().all())

    async def _parse_job_outputs(
        self,
        ctx: PipelineContext,
        job: ExtractionJob,
        has_soft_hash_column: bool,
    ) -> tuple[int, int]:
        """Parse credential files from a single extraction job.

        Processes credentials in batches of BATCH_SIZE to keep memory usage
        bounded regardless of individual file size.

        Returns:
            Tuple of (new_credentials, duplicates_skipped).
        """
        credentials_found = 0
        duplicates_found = 0

        # Get the extracted output files
        outputs = job.outputs

        # Find credential files among outputs
        credential_outputs = [o for o in outputs if is_credential_file(o.output_filename)]
        # ULP files first — highest-value credential lists
        credential_outputs.sort(
            key=lambda o: 0 if "ulp" in o.output_filename.lower() else 1
        )

        if not credential_outputs:
            return 0, 0

        logger.debug(
            "Found %d credential files in job %d",
            len(credential_outputs),
            job.id,
        )

        # Bulk-load already parsed source_file values for this job once.
        # This avoids an extra existence query per credential file.
        source_paths = [str(Path(o.output_path)) for o in credential_outputs if o.output_path]
        parsed_source_files = set(
            ctx.session.execute(
                select(ParsedCredential.source_file)
                .where(
                    ParsedCredential.extraction_job_id == job.id,
                    ParsedCredential.source_file.in_(source_paths),
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
        _preskip_dup_threshold = 2
        all_output_hashes = [o.output_hash for o in credential_outputs if o.output_hash]
        from telecrime.models import FirstSeenIndex
        preskip_hashes: set[str] = set()
        if all_output_hashes:
            preskip_hashes = set(
                ctx.session.execute(
                    select(FirstSeenIndex.content_hash).where(
                        FirstSeenIndex.content_hash.in_(all_output_hashes),
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

            if not file_path.exists():
                logger.warning("Credential file missing: %s", file_path)
                continue

            # Check if we've already parsed this file
            if str(file_path) in parsed_source_files:
                logger.debug("Already parsed: %s", file_path)
                continue

            # Pre-skip when content_hash has been seen ≥3 times across archives.
            if output.output_hash and output.output_hash in preskip_hashes:
                logger.info(
                    "Pre-skip (content seen ≥%d times): %s",
                    _preskip_dup_threshold + 1, output.output_filename,
                )
                continue

            # Stream credentials in batches to keep memory bounded.
            # Each batch is: compute hashes → DB dedup check → insert → flush → discard.
            # Cross-batch deduplication is handled by the DB hash check; seen_in_batch
            # only deduplicates within the current batch.
            batch: list = []
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
                inserted_rows = self._bulk_insert_credentials(ctx, rows)
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
            file_size = file_path.stat().st_size if file_path.exists() else 0
            workers = self._parallel_worker_count()

            async def _sequential_parse() -> None:
                nonlocal batch, file_cred_count, credentials_found
                nonlocal duplicates_found, batches_since_commit
                nonlocal dup_batches_seen, file_skipped_as_dup
                for cred in iter_credentials_file(file_path):
                    if file_skipped_as_dup:
                        break
                    batch.append(cred)
                    file_cred_count += 1

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
                        await asyncio.sleep(0)
                        if _is_dup_batch(new, dups, _BATCH_SIZE):
                            dup_batches_seen += 1
                            if dup_batches_seen >= dup_confirm_batches:
                                file_skipped_as_dup = True
                                logger.info(
                                    "Early-skip %s: %d consecutive batches at %.0f%% duplicates",
                                    file_path.name, dup_batches_seen, (dups / (new + dups)) * 100,
                                )
                        else:
                            dup_batches_seen = 0

            if file_size >= _PARALLEL_PARSE_MIN_BYTES and workers > 1:
                try:
                    logger.info(
                        "Parsing %s in parallel (%d workers, %.1f MB)",
                        file_path.name, workers, file_size / 1024 / 1024,
                    )
                    async for tup in self._iter_parallel_credentials(
                        file_path, str(file_path), workers
                    ):
                        if file_skipped_as_dup:
                            break
                        batch.append(tup)
                        file_cred_count += 1

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
                            await asyncio.sleep(0)
                            if _is_dup_batch(new, dups, _BATCH_SIZE):
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
                    batch = []
                    file_cred_count = 0
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

            if file_cred_count:
                logger.info(
                    "Parsed %d credentials from %s (%d new, %d dups)",
                    file_cred_count,
                    output.output_filename,
                    credentials_found,
                    duplicates_found,
                )

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

        return credentials_found, duplicates_found

    def _bulk_insert_credentials(
        self,
        ctx: PipelineContext,
        rows: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        """Insert a credential batch and return the rows that were newly inserted.

        Uses PostgreSQL `COPY ... FROM STDIN` into a temp staging table, then
        `INSERT ... SELECT ... ON CONFLICT (credential_hash) DO NOTHING` to
        dedupe against the live table. Benchmarks ~4× faster end-to-end than
        the previous chunked INSERT-VALUES path (12K rows/sec vs 2.7K on the
        live schema with all GIN indexes present). Falls back to the slower
        path on SQLite (test fixtures) since COPY is PG-specific.

        Each chunk runs inside its own SAVEPOINT so a failure of one chunk only
        discards that chunk — previously inserted chunks remain durable in the
        enclosing transaction and the reported count matches reality.
        """
        if not rows:
            return []

        if ctx.session.get_bind().dialect.name == "postgresql":
            return self._bulk_insert_via_copy(ctx, rows)
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

    # Most fields contain no control characters; one C-level scan per value
    # (no allocation) avoids 4 str.replace dispatches per field.
    _COPY_ESCAPE_NEEDED = re.compile(r"[\\\t\n\r]")

    @staticmethod
    def _copy_escape(value: object) -> str:
        """Escape a single field for PostgreSQL COPY text format."""
        if value is None:
            return "\\N"
        s = str(value)
        if not ParseStage._COPY_ESCAPE_NEEDED.search(s):
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
            chunk_ok = False
            last_error: Exception | None = None
            for attempt in range(2):
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
                        # chunk savepoint + retry + abort-on-persistent-error
                        # path keeps that safe — no rows are lost.
                        cursor.execute("SET statement_timeout = 600000")
                        cursor.execute("SET lock_timeout = 0")
                        cursor.execute("SET synchronous_commit = off")
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
                            write("\t".join(esc(row.get(f)) for f in fields))
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
                    for credential_hash, domain in rows_returned:
                        inserted.append({
                            "credential_hash": credential_hash,
                            "domain": domain,
                        })
                    chunk_ok = True
                    break
                except Exception as exc:
                    last_error = exc
                    try:
                        savepoint.rollback()
                    except Exception:
                        pass
                    if attempt == 0:
                        logger.info(
                            "Credential COPY chunk failed on first attempt (%s: %s) — retrying",
                            type(exc).__name__, exc,
                        )
                    else:
                        logger.warning(
                            "Credential COPY chunk failed after retry (%s): %s — continuing",
                            type(exc).__name__, exc,
                        )
            if not chunk_ok:
                logger.error(
                    "Credential COPY chunk failed after retry; aborting parse instead of "
                    "silently dropping %d credentials: %s",
                    len(chunk),
                    last_error,
                )
                raise RuntimeError(
                    f"Credential COPY chunk failed after retry ({len(chunk)} rows)"
                ) from last_error

        return inserted

    def _bulk_insert_via_values(
        self,
        ctx: PipelineContext,
        rows: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        """Legacy chunked INSERT-VALUES path, used by SQLite test fixtures."""
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
