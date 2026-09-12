"""Stage 7: Finalize - cleanup archives, record provenance, detect duplicates."""

import asyncio
import logging
import shutil
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from sqlalchemy import delete, func, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import selectinload

from telecrime.models import (
    ArchiveGroup,
    ArchiveGroupPart,
    DownloadArtifact,
    ExtractedOutput,
    ExtractionJob,
    FirstSeenIndex,
    Message,
    ParsedCredential,
)
from telecrime.pipeline.constants import EXTRACTION_MAX_ATTEMPTS
from telecrime.pipeline.orchestrator import PipelineContext, PipelineStage
from telecrime.states import DownloadStatus, ExtractionStatus, GroupStatus

logger = logging.getLogger(__name__)


def _as_aware_utc(value: datetime) -> datetime:
    """Normalise naive DB datetimes and aware Python datetimes for comparison."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _first_seen_insert(session):
    """Dialect-specific INSERT constructor supporting ``on_conflict_do_update``.

    Production is PostgreSQL; SQLite support keeps the in-memory test fixtures
    (which exercise this path) working.
    """
    if session.get_bind().dialect.name == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as _insert
    else:
        from sqlalchemy.dialects.postgresql import insert as _insert
    return _insert(FirstSeenIndex)


class FinalizeStage(PipelineStage):
    """Cleanup source archives and record provenance."""

    name = "finalize"

    async def run(self, ctx: PipelineContext) -> bool:
        """Run the finalize stage."""
        logger.info("Starting finalization")

        # Prune any extracted_output rows that belong to CLEANED groups but
        # were never deleted (e.g. groups cleaned before the bulk-delete was
        # added).  This runs cheaply each cycle and drains the backlog without
        # waiting for the weekly vacuum job.
        if not ctx.dry_run:
            await self._prune_orphaned_extracted_outputs(ctx)

        # Process extracted groups (successful extractions)
        extracted_groups = ctx.session.execute(
            select(ArchiveGroup)
            .where(ArchiveGroup.status == GroupStatus.EXTRACTED)
            .options(
                selectinload(ArchiveGroup.parts)
                .selectinload(ArchiveGroupPart.artifact),
                selectinload(ArchiveGroup.extraction_jobs)
                .selectinload(ExtractionJob.outputs),
            )
        ).scalars().all()

        # Also process failed groups to reclaim disk space.
        # ONLY permanently-failed groups are cleaned here: transient
        # GroupStatus.FAILED (retryable extraction error) and EXTRACTING groups
        # must NOT have their downloaded archives deleted — startup recovery
        # resets them to READY so the extraction is retried. Deleting them
        # would silently destroy a recoverable group (see the AcquireStage
        # low-disk test comment).
        failed_groups = ctx.session.execute(
            select(ArchiveGroup)
            .where(ArchiveGroup.status.in_([
                GroupStatus.FAILED_TERMINAL,
            ]))
            .options(
                selectinload(ArchiveGroup.parts)
                .selectinload(ArchiveGroupPart.artifact),
                selectinload(ArchiveGroup.extraction_jobs),
            )
        ).scalars().all()

        if not extracted_groups and not failed_groups:
            # Still sweep for orphaned extraction/download leftovers. This is
            # important after crashes or older cleanup bugs because there may be
            # nothing new to finalize, but stale files can still consume disk.
            if not ctx.dry_run:
                for _sweep in (self._sweep_stale_directories, self._sweep_orphaned_downloads):
                    try:
                        await _sweep(ctx)
                    except (Exception, asyncio.CancelledError) as _e:
                        logger.warning("Finalize sweep %s failed: %s", _sweep.__name__, _e)
            logger.info("No groups to finalize")
            return True

        logger.info(
            "Finalizing %d extracted groups, %d failed groups",
            len(extracted_groups),
            len(failed_groups),
        )

        # Process successful extractions. Groups whose parse failed part-way
        # (ctx.parse_failed_group_ids) stay EXTRACTED: their credential files
        # are the only copy of the unparsed remainder, and the next run's
        # parse stage re-processes them.
        for group in extracted_groups:
            if group.id in ctx.parse_failed_group_ids:
                logger.warning(
                    "Skipping finalize for group %d — parse incomplete, "
                    "keeping extracted files for re-parse",
                    group.id,
                )
                continue
            try:
                await self._finalize_extracted_group(ctx, group)
            except Exception as e:
                logger.error("Error finalizing group %d: %s", group.id, e)
                try:
                    ctx.session.rollback()
                except Exception:
                    pass
                ctx.errors.append(f"Finalize error: {e}")

        # Cleanup failed groups to reclaim disk space
        for group in failed_groups:
            try:
                await self._finalize_failed_group(ctx, group)
            except Exception as e:
                logger.error("Error cleaning up failed group %d: %s", group.id, e)
                try:
                    ctx.session.rollback()
                except Exception:
                    pass

        # Sweep any orphaned extraction directories left behind by crashes.
        # Run both sweeps unconditionally — a CancelledError mid-loop above must
        # not prevent disk reclamation.  Errors here are logged and swallowed so
        # they don't mask the per-group errors already appended to ctx.errors.
        if not ctx.dry_run:
            for _sweep in (self._sweep_stale_directories, self._sweep_orphaned_downloads):
                try:
                    await _sweep(ctx)
                except (Exception, asyncio.CancelledError) as _e:
                    logger.warning("Finalize sweep %s failed: %s", _sweep.__name__, _e)

        ctx.session.commit()
        return True

    async def run_group(self, ctx: PipelineContext, group_id: int) -> bool:
        """Finalize a single group if it is ready for cleanup."""
        group = ctx.session.execute(
            select(ArchiveGroup)
            .where(ArchiveGroup.id == group_id)
            .options(
                selectinload(ArchiveGroup.parts).selectinload(ArchiveGroupPart.artifact),
                selectinload(ArchiveGroup.extraction_jobs).selectinload(ExtractionJob.outputs),
            )
        ).scalar_one_or_none()
        if group is None:
            return False

        if group.status == GroupStatus.EXTRACTED:
            if group.id in ctx.parse_failed_group_ids:
                logger.warning(
                    "Skipping finalize for group %d — parse incomplete, "
                    "keeping extracted files for re-parse",
                    group.id,
                )
                return False
            await self._finalize_extracted_group(ctx, group)
            return True

        if group.status == GroupStatus.FAILED_TERMINAL:
            await self._finalize_failed_group(ctx, group)
            return True

        return False

    async def _finalize_extracted_group(self, ctx: PipelineContext, group: ArchiveGroup) -> None:
        """Finalize one successfully extracted group."""
        await self._record_first_seen(ctx, group)

        if not ctx.dry_run:
            await self._cleanup_archives(ctx, group)
            await self._cleanup_extracted_files(ctx, group)

        # Denormalize credential count so the web UI can read it without a COUNT JOIN.
        job_ids = [job.id for job in group.extraction_jobs]
        if job_ids:
            group.credential_count = ctx.session.execute(
                select(func.count(ParsedCredential.id))
                .where(ParsedCredential.extraction_job_id.in_(job_ids))
            ).scalar_one()

        group.status = GroupStatus.CLEANED
        ctx.session.flush()

    async def _finalize_failed_group(self, ctx: PipelineContext, group: ArchiveGroup) -> None:
        """Cleanup one failed group to reclaim disk space."""
        # A group marked FAILED_TERMINAL with a PENDING job still holding the
        # archive is NOT terminal — the PENDING attempt never ran (e.g. a
        # wedged group whose old failed job hit the attempts cap while a
        # fresh retry is queued). Deleting the archive here would destroy a
        # recoverable download; only clean groups whose jobs are all settled.
        # A PENDING job already AT the attempts cap is genuinely terminal —
        # skipping cleanup for it would flap the group forever.
        _pending = any(
            job.status == ExtractionStatus.PENDING
            and (job.attempts_count or 0) < EXTRACTION_MAX_ATTEMPTS
            for job in group.extraction_jobs
        )
        if _pending:
            logger.warning(
                "Skipping cleanup of failed group %s — a PENDING extraction "
                "job still needs the archive",
                group.base_name,
            )
            group.status = GroupStatus.FAILED
            ctx.session.flush()
            return
        if not ctx.dry_run:
            await self._cleanup_archives(ctx, group)
            await self._cleanup_extracted_files(ctx, group)

        group.status = GroupStatus.CLEANED
        group.notes = (group.notes or "") + "\nArchives deleted after failed extraction"
        ctx.session.flush()
        logger.info("Cleaned up failed group: %s", group.base_name)

    async def _prune_orphaned_extracted_outputs(self, ctx: PipelineContext) -> None:
        """Delete extracted_output rows belonging to CLEANED groups.

        Rows should be deleted by _cleanup_extracted_files as each group is
        finalized, but a large backlog of historical rows may exist from groups
        cleaned before that deletion was added.  Pruning up to 10k rows per
        cycle keeps the table small without a long-running DELETE.
        """
        job_ids_subq = (
            select(ExtractionJob.id)
            .join(ArchiveGroup, ArchiveGroup.id == ExtractionJob.group_id)
            .where(ArchiveGroup.status == GroupStatus.CLEANED)
            .limit(10000)
        )
        deleted = cast(
            CursorResult,
            ctx.session.execute(
                delete(ExtractedOutput).where(
                    ExtractedOutput.job_id.in_(job_ids_subq)
                )
            ),
        )
        if deleted.rowcount:
            logger.info(
                "Pruned %d orphaned extracted_output rows from CLEANED groups",
                deleted.rowcount,
            )
            ctx.session.commit()

    async def _sweep_orphaned_downloads(self, ctx: PipelineContext) -> None:
        """Delete archive files in the downloads directory whose DB record is
        marked is_deleted=True or whose artifact group is now CLEANED.

        This catches files left by container restarts mid-cleanup, or by the
        concurrent-delete bug where unlink() raised FileNotFoundError before
        is_deleted was set.
        """
        downloads_dir = ctx.config.downloads_dir
        if not downloads_dir.exists():
            return

        # Files younger than this are skipped: the sequential pipeline's
        # prefetch tasks rename+commit files concurrently with this sweep, and
        # a READ COMMITTED snapshot taken before that commit does not see the
        # artifact row — the sweep would otherwise unlink a file the DB
        # considers COMPLETED (permanent archive loss). In-flight prefetches
        # are always recent; leftovers from previous runs are old.
        _recent_seconds = 10 * 60

        # Get the set of local_paths that are currently being downloaded
        # (DOWNLOADING status) — do not touch those.
        active_paths = {
            row[0]
            for row in ctx.session.execute(
                select(DownloadArtifact.local_path).where(
                    DownloadArtifact.status == DownloadStatus.DOWNLOADING,
                    DownloadArtifact.local_path.isnot(None),
                )
            )
        }

        candidates: list[tuple[Path, str]] = []
        for entry in downloads_dir.iterdir():
            if entry.is_dir() or entry.suffix.lower() in (".tmp", ".part"):
                continue
            entry_str = str(entry)
            if entry_str in active_paths:
                continue  # Currently downloading — leave alone
            try:
                if (time.time() - entry.stat().st_mtime) < _recent_seconds:
                    continue  # Recently written — possibly an in-flight prefetch
            except OSError:
                continue
            candidates.append((entry, entry_str))

        # One batched IN query per chunk instead of one unindexed local_path
        # seq scan per file (the directory can hold thousands of leftovers).
        artifacts_by_path: dict[str, list[tuple[DownloadArtifact, GroupStatus | None]]] = {}
        for i in range(0, len(candidates), self._BATCH_SIZE):
            chunk = [path for _, path in candidates[i : i + self._BATCH_SIZE]]
            rows = ctx.session.execute(
                select(DownloadArtifact, ArchiveGroup.status)
                .outerjoin(ArchiveGroupPart, ArchiveGroupPart.artifact_id == DownloadArtifact.id)
                .outerjoin(ArchiveGroup, ArchiveGroup.id == ArchiveGroupPart.group_id)
                .where(
                    DownloadArtifact.local_path.in_(chunk),
                    DownloadArtifact.is_deleted.is_(False),
                )
            ).all()
            for artifact, status in rows:
                artifacts_by_path.setdefault(artifact.local_path, []).append((artifact, status))

        for entry, entry_str in candidates:
            artifact_rows = artifacts_by_path.get(entry_str, [])
            if artifact_rows:
                # Legitimately present if any non-CLEANED grouped artifact or an
                # ungrouped artifact still claims it.  CLEANED groups no longer
                # need their source archives and can be reclaimed.
                if any(status is None or status != GroupStatus.CLEANED for _, status in artifact_rows):
                    continue

                try:
                    entry.unlink()
                    logger.info("Swept CLEANED-group download file: %s", entry.name)
                except FileNotFoundError:
                    logger.debug("CLEANED-group download already gone: %s", entry.name)
                except Exception as e:
                    logger.warning("Failed to sweep CLEANED download %s: %s", entry.name, e)
                    continue
                for artifact, _ in artifact_rows:
                    artifact.is_deleted = True
                continue

            try:
                entry.unlink()
                logger.info("Swept orphaned download file: %s", entry.name)
            except Exception as e:
                logger.warning("Failed to sweep download %s: %s", entry.name, e)

    async def _sweep_stale_directories(self, ctx: PipelineContext) -> None:
        """Remove extraction directories for groups that are already CLEANED.

        This catches directories orphaned by crashes or bugs where finalize
        marked a group as CLEANED but failed to delete its extracted files.
        """
        extracted_dir = ctx.config.extracted_dir
        if not extracted_dir.exists():
            return

        for entry in extracted_dir.iterdir():
            if not entry.is_dir() or not entry.name.startswith("group_"):
                continue

            try:
                group_id = int(entry.name.split("_", 1)[1])
            except (ValueError, IndexError):
                continue

            group = ctx.session.get(ArchiveGroup, group_id)
            if group is None or group.status == GroupStatus.CLEANED:
                try:
                    shutil.rmtree(entry)
                    logger.info("Swept stale extraction directory: %s", entry.name)
                except Exception as e:
                    logger.warning("Failed to sweep %s: %s", entry.name, e)

    _BATCH_SIZE = 500

    def _upsert_first_seen(
        self,
        session,
        *,
        output: ExtractedOutput,
        msg: Message | None,
        first_seen_ts: datetime,
    ) -> None:
        """Insert a first_seen row, atomically bumping duplicate_count when a
        concurrent finalize run already inserted the same content_hash."""
        insert_stmt = _first_seen_insert(session).values(
            content_hash=output.output_hash,
            content_type="extracted",
            first_seen_timestamp=first_seen_ts,
            first_seen_conversation_id=output.source_conversation_id,
            first_seen_message_id=output.source_message_id,
            first_seen_message_platform_id=msg.platform_id if msg else None,
            duplicate_count=0,
        )
        session.execute(
            insert_stmt.on_conflict_do_update(
                index_elements=[FirstSeenIndex.content_hash],
                set_={"duplicate_count": FirstSeenIndex.duplicate_count + 1},
            )
        )

    async def _record_first_seen(self, ctx: PipelineContext, group: ArchiveGroup) -> None:
        """Record first-seen timestamps for extracted outputs.

        Batches IN queries to avoid Postgres shared-memory exhaustion on large groups.
        """
        outputs = [o for job in group.extraction_jobs for o in job.outputs]
        if not outputs:
            return

        # --- 1. Bulk-fetch existing first_seen_index rows for this group ---
        all_hashes = [o.output_hash for o in outputs if o.output_hash]
        existing_map: dict[str, FirstSeenIndex] = {}
        for i in range(0, len(all_hashes), self._BATCH_SIZE):
            batch = all_hashes[i : i + self._BATCH_SIZE]
            rows = ctx.session.execute(
                select(FirstSeenIndex).where(FirstSeenIndex.content_hash.in_(batch))
            ).scalars().all()
            existing_map.update({r.content_hash: r for r in rows})

        # --- 2. Bulk-fetch message timestamps for ALL outputs (new and duplicate).
        # Duplicate outputs also need message timestamps to check if this archive
        # saw the content earlier than the recorded first_seen. Pre-fetching all
        # at once avoids an N+1 fallback in the duplicate branch below.
        all_msg_ids = {o.source_message_id for o in outputs if o.source_message_id}
        msg_map: dict[int, Message] = {}
        msg_ids_list = list(all_msg_ids)
        for i in range(0, len(msg_ids_list), self._BATCH_SIZE):
            id_batch = msg_ids_list[i : i + self._BATCH_SIZE]
            msgs = ctx.session.execute(
                select(Message).where(Message.id.in_(id_batch))
            ).scalars().all()
            msg_map.update({m.id: m for m in msgs})

        # --- 3. Process each output ---
        now = datetime.now(UTC)
        new_count = 0
        dup_count = 0
        seen_this_run: set[str] = set()
        # Earliest first_seen we have recorded this run, per hash (the prefetched
        # ORM rows are left untouched so stale in-memory state is never flushed).
        known_ts: dict[str, datetime] = {
            content_hash: row.first_seen_timestamp
            for content_hash, row in existing_map.items()
        }
        for output in outputs:
            existing = existing_map.get(output.output_hash)
            if existing is None:
                msg = msg_map.get(output.source_message_id) if output.source_message_id else None
                first_seen_ts = msg.platform_timestamp if msg else now
                # Atomic upsert: two finalize runs that both missed the row
                # cannot abort each other with an IntegrityError, and repeated
                # hashes within this run increment instead of double-inserting.
                self._upsert_first_seen(
                    ctx.session,
                    output=output,
                    msg=msg,
                    first_seen_ts=first_seen_ts,
                )
                if output.output_hash in seen_this_run:
                    dup_count += 1
                else:
                    seen_this_run.add(output.output_hash)
                    new_count += 1
            else:
                # Atomic increment: the old read-modify-write on the ORM object
                # lost concurrent updates.
                ctx.session.execute(
                    update(FirstSeenIndex)
                    .where(FirstSeenIndex.id == existing.id)
                    .values(duplicate_count=FirstSeenIndex.duplicate_count + 1),
                    execution_options={"synchronize_session": False},
                )
                # Check if this archive saw it earlier than recorded. The
                # timestamp predicate is re-checked in SQL so a stale snapshot
                # cannot overwrite an even earlier concurrent first_seen.
                msg = msg_map.get(output.source_message_id) if output.source_message_id else None
                known = known_ts.get(output.output_hash)
                if msg and (known is None or _as_aware_utc(msg.platform_timestamp) < _as_aware_utc(known)):
                    ctx.session.execute(
                        update(FirstSeenIndex)
                        .where(
                            FirstSeenIndex.id == existing.id,
                            FirstSeenIndex.first_seen_timestamp > msg.platform_timestamp,
                        )
                        .values(
                            first_seen_timestamp=msg.platform_timestamp,
                            first_seen_conversation_id=output.source_conversation_id,
                            first_seen_message_id=output.source_message_id,
                            first_seen_message_platform_id=msg.platform_id,
                        ),
                        execution_options={"synchronize_session": False},
                    )
                    known_ts[output.output_hash] = msg.platform_timestamp
                dup_count += 1

        logger.debug(
            "first_seen for group %s: %d new, %d dups",
            group.base_name,
            new_count,
            dup_count,
        )

    async def _cleanup_archives(self, ctx: PipelineContext, group: ArchiveGroup) -> None:
        """Delete source archive files after successful extraction."""
        # Check that at least some extraction outputs were produced, as a
        # sanity guard.  If ALL outputs are missing AND there are expected
        # outputs, skip — but only when this is the first finalize attempt.
        # On re-finalization (e.g. after a previous crash mid-cleanup) the
        # extraction directory may already be gone; log a warning and proceed
        # rather than abandoning archive cleanup.
        outputs = [o for job in group.extraction_jobs for o in job.outputs]
        missing = [o for o in outputs if not Path(o.output_path).exists()]
        if missing and len(missing) == len(outputs) and outputs:
            logger.warning(
                "All %d extraction outputs already deleted for group %s — "
                "proceeding with archive cleanup (re-finalize after crash)",
                len(missing),
                group.base_name,
            )

        # Delete archive parts
        # Repost-dedup can point an artifact in ANOTHER (still-active) group
        # at the same local_path. Deleting the file here would break that
        # group's extraction. Leave the file when another non-CLEANED group
        # shares it — _sweep_orphaned_downloads reclaims it once every sharing
        # group is CLEANED.
        part_ids = [p.artifact_id for p in group.parts if p.artifact_id is not None]
        shared_paths: set[str] = set()
        if part_ids:
            shared_paths = {
                row[0]
                for row in ctx.session.execute(
                    select(DownloadArtifact.local_path, ArchiveGroup.status)
                    .join(ArchiveGroupPart, ArchiveGroupPart.artifact_id == DownloadArtifact.id)
                    .join(ArchiveGroup, ArchiveGroup.id == ArchiveGroupPart.group_id)
                    .where(
                        DownloadArtifact.local_path.isnot(None),
                        DownloadArtifact.is_deleted.is_(False),
                        DownloadArtifact.id.not_in(part_ids),
                        ArchiveGroup.status != GroupStatus.CLEANED,
                    )
                )
                if row[0]
            }

        for part in group.parts:
            artifact = part.artifact
            if artifact.local_path and not artifact.is_deleted:
                archive_path = Path(artifact.local_path)

                if archive_path.exists() and str(archive_path) in shared_paths:
                    logger.info(
                        "Leaving %s — shared with another active group (repost dedup)",
                        archive_path.name,
                    )
                    continue

                if archive_path.exists():
                    try:
                        archive_path.unlink()
                        artifact.is_deleted = True
                        logger.debug("Deleted archive: %s", archive_path.name)
                    except FileNotFoundError:
                        # Deleted concurrently by another finalize run — fine.
                        artifact.is_deleted = True
                    except Exception as e:
                        # Transient failures (EBUSY, EIO) resolve on a retry.
                        # The group is still marked CLEANED by the caller, so
                        # leave is_deleted=False: the orphan sweep (or the next
                        # run's cleanup) reclaims the file. Retry once with a
                        # short backoff to avoid relying on that backstop.
                        try:
                            time.sleep(1.0)
                            archive_path.unlink()
                            artifact.is_deleted = True
                            logger.warning("Deleted %s after retry: %s", archive_path, e)
                        except FileNotFoundError:
                            artifact.is_deleted = True
                        except Exception as e2:
                            logger.warning(
                                "Failed to delete %s after retry: %s", archive_path, e2
                            )
                else:
                    artifact.is_deleted = True  # Already gone

        logger.info("Cleaned up archives for group: %s", group.base_name)

    async def _cleanup_extracted_files(self, ctx: PipelineContext, group: ArchiveGroup) -> None:
        """Delete extracted files after credentials have been parsed.

        This saves disk space - we keep the parsed credentials in the database
        but delete the source text files.
        """
        # Find the group extraction directory (data/extracted/group_N)
        group_extract_dir = ctx.config.extracted_dir / f"group_{group.id}"

        # Delete the entire extraction directory tree
        if group_extract_dir.exists():
            try:
                shutil.rmtree(group_extract_dir)
                logger.info("Deleted extraction directory: %s", group_extract_dir)
            except Exception as e:
                logger.warning("Failed to delete extraction directory %s: %s", group_extract_dir, e)

        # Bulk-delete ExtractedOutput rows for this group — the files are gone
        # and first_seen has already been recorded.  Keeping them only wastes
        # DB space (the table can grow to millions of rows per run).
        job_ids_subq = select(ExtractionJob.id).where(
            ExtractionJob.group_id == group.id
        )
        deleted = cast(
            CursorResult,
            ctx.session.execute(
                delete(ExtractedOutput).where(
                    ExtractedOutput.job_id.in_(job_ids_subq)
                )
            ),
        )
        if deleted.rowcount:
            logger.debug(
                "Pruned %d extracted_output rows for group %s",
                deleted.rowcount,
                group.id,
            )
