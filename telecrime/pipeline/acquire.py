"""Stage 4: Acquire - download files sequentially with idempotency."""

import asyncio
import hashlib
import logging
import shutil
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import or_, select
from sqlalchemy.orm import joinedload

from telecrime.logging_utils import log_context
from telecrime.models import (
    ArchiveGroup,
    ArchiveGroupPart,
    DownloadArtifact,
    FileAttachment,
    Message,
)
from telecrime.pipeline.orchestrator import PipelineContext, PipelineStage
from telecrime.states import DownloadStatus, GroupStatus
from telecrime.utils.retry import backoff_delay

_TELETHON_PERMANENT_ERRORS: tuple[type[BaseException], ...]
try:
    from telethon.errors import (
        FileReferenceEmptyError,
        FileReferenceExpiredError,
        FileReferenceInvalidError,
        MediaEmptyError,
        MediaInvalidError,
    )
    _TELETHON_PERMANENT_ERRORS = (
        FileReferenceEmptyError,
        FileReferenceExpiredError,
        FileReferenceInvalidError,
        MediaEmptyError,
        MediaInvalidError,
    )
except ImportError:
    _TELETHON_PERMANENT_ERRORS = ()

_PERMANENT_ERROR_SUBSTRINGS = (
    "Message not found in Telegram",
    "has no media attachment",
    "Download task completed but file not found",
)

logger = logging.getLogger(__name__)


def _is_permanent_error(e: BaseException) -> bool:
    """Return True if the error indicates the file is permanently gone from Telegram."""
    if _TELETHON_PERMANENT_ERRORS and isinstance(e, _TELETHON_PERMANENT_ERRORS):
        return True
    msg = str(e)
    return any(sub in msg for sub in _PERMANENT_ERROR_SUBSTRINGS)


class AcquireStage(PipelineStage):
    """Download files sequentially with crash-safe resume."""

    name = "acquire"

    def recover_stuck_downloads(self, session, downloads_dir: Path) -> int:
        """Reset artifacts stuck in DOWNLOADING state back to PENDING (or COMPLETED).

        Called at pipeline startup. When the process crashes mid-download,
        artifacts stay at DOWNLOADING and are never retried. This method:
        - Marks COMPLETED any artifact whose final file is already on disk
          (download finished but commit was missed before the crash).
        - Resets to PENDING any artifact whose file is absent (incomplete download).
        - Cleans up orphaned .partial files not referenced by any stuck artifact.

        Returns the number of artifacts recovered.
        """
        stuck = (
            session.execute(
                select(DownloadArtifact)
                .options(joinedload(DownloadArtifact.attachment))
                .where(DownloadArtifact.status == DownloadStatus.DOWNLOADING)
            )
            .scalars()
            .all()
        )

        # Collect temp paths BEFORE modifying them so orphan cleanup is accurate.
        tracked_temps = {a.temp_path for a in stuck if a.temp_path}

        recovered = 0
        for artifact in stuck:
            filename = artifact.attachment.filename if artifact.attachment else None
            expected_size = artifact.attachment.size if artifact.attachment else None
            existing_path: Path | None = None

            def _candidate_matches(candidate: Path) -> bool:
                """Only trust an on-disk file when its size matches the
                expected artifact size — a same-named file from ANOTHER
                artifact (different content) must not be attributed here."""
                if expected_size is None:
                    return False
                try:
                    return candidate.stat().st_size == expected_size
                except OSError:
                    return False

            # Check if the download already completed but the commit was missed.
            if (
                artifact.local_path
                and Path(artifact.local_path).exists()
                and _candidate_matches(Path(artifact.local_path))
            ):
                existing_path = Path(artifact.local_path)
            elif filename:
                # Also search collision-suffixed names (foo_1.zip ...): a
                # crash after the rename but before the commit leaves the
                # artifact DOWNLOADING with no local_path while the completed
                # file sits under a suffixed name.
                safe = self._sanitize_filename(filename)
                stem, suffix = Path(safe).stem, Path(safe).suffix
                for i in range(100):
                    cand = downloads_dir / safe if i == 0 else downloads_dir / f"{stem}_{i}{suffix}"
                    if cand.exists() and _candidate_matches(cand):
                        existing_path = cand
                        break

            if existing_path is not None:
                # File is on disk — mark COMPLETED without re-downloading.
                artifact.local_path = str(existing_path)
                artifact.temp_path = None
                artifact.status = DownloadStatus.COMPLETED
                # Size alone cannot distinguish a DIFFERENT same-name+same-size
                # file — verify content. Recovery is startup-only, so the
                # one-time hash read is cheap vs re-downloading multi-GB.
                try:
                    artifact.content_hash = self._compute_hash_sync(existing_path)
                    artifact.verified_size = existing_path.stat().st_size
                except Exception as hash_err:
                    logger.warning(
                        "Could not hash recovered artifact %d (%s): %s",
                        artifact.id, filename or "unknown", hash_err,
                    )
                logger.info(
                    "Recovered artifact %d (%s) → COMPLETED (file on disk)",
                    artifact.id, filename or "unknown",
                )
            else:
                # File absent — delete partial and reset for re-download.
                if artifact.temp_path:
                    try:
                        Path(artifact.temp_path).unlink(missing_ok=True)
                    except Exception:
                        pass
                artifact.temp_path = None
                artifact.local_path = None
                artifact.status = DownloadStatus.PENDING
                logger.info(
                    "Recovered artifact %d (%s) → PENDING (re-download needed)",
                    artifact.id, filename or "unknown",
                )

            recovered += 1

        # Clean up orphaned .partial files not referenced by any stuck artifact.
        tmp_dir = downloads_dir / ".tmp"
        if tmp_dir.exists():
            for partial in tmp_dir.glob("*.partial"):
                if str(partial) not in tracked_temps:
                    try:
                        partial.unlink(missing_ok=True)
                        logger.info("Removed orphaned partial file: %s", partial.name)
                    except Exception:
                        pass

        if recovered:
            session.commit()
            logger.info("Startup recovery: resolved %d stuck DOWNLOADING artifacts", recovered)

        return recovered

    def cleanup_stale_incomplete_groups(self, session, max_age_days: int = 30) -> int:
        """Mark very old INCOMPLETE groups with zero progress as FAILED_TERMINAL.

        Groups that have been INCOMPLETE for > max_age_days and have no
        COMPLETED parts are unlikely to ever succeed — they just clog the queue.
        Returns the number of groups cleaned up.
        """

        cutoff = datetime.now(UTC) - timedelta(days=max_age_days)
        stale_groups = (
            session.execute(
                select(ArchiveGroup)
                .where(
                    ArchiveGroup.status == GroupStatus.INCOMPLETE,
                    ArchiveGroup.updated_at < cutoff,
                )
                .options(joinedload(ArchiveGroup.parts).joinedload(ArchiveGroupPart.artifact))
            )
            .unique()
            .scalars()
            .all()
        )

        cleaned = 0
        for group in stale_groups:
            if not group.parts:
                continue
            # A group with a DOWNLOADING part is NOT stale: group.updated_at
            # only moves on status changes, so a first part stuck mid-download
            # across a long drive-wedge freeze leaves the timestamp older than
            # the cutoff even though work is active. A part still PENDING
            # after 30 days was never downloaded — that IS stale.
            if any(
                part.artifact.status == DownloadStatus.DOWNLOADING
                for part in group.parts
            ):
                continue
            any_completed = any(
                part.artifact.status == DownloadStatus.COMPLETED for part in group.parts
            )
            if any_completed:
                continue
            group.status = GroupStatus.FAILED_TERMINAL
            for part in group.parts:
                if part.artifact.status in (DownloadStatus.PENDING, DownloadStatus.FAILED):
                    part.artifact.status = DownloadStatus.FAILED_TERMINAL
            cleaned += 1

        if cleaned:
            session.commit()
            logger.info(
                "Cleaned up %d stale INCOMPLETE groups (older than %d days, zero progress)",
                cleaned,
                max_age_days,
            )
        return cleaned

    async def run(self, ctx: PipelineContext) -> bool:
        """Run the acquire stage."""
        logger.info("Starting file acquisition")

        self.cleanup_stale_incomplete_groups(ctx.session, max_age_days=30)

        # Find pending download job IDs only, then commit immediately.
        # Keeping the read transaction open for the entire download duration
        # (potentially many minutes) holds a PostgreSQL snapshot that prevents
        # autovacuum from reclaiming dead tuples across all tables the SELECT
        # touched (archive_groups, download_artifacts, etc.).
        from telecrime.pipeline.orchestrator import _download_priority

        pending_ids = (
            ctx.session.execute(
                select(DownloadArtifact.id)
                .join(DownloadArtifact.attachment)
                .outerjoin(
                    ArchiveGroupPart,
                    ArchiveGroupPart.artifact_id == DownloadArtifact.id,
                )
                .outerjoin(ArchiveGroup, ArchiveGroup.id == ArchiveGroupPart.group_id)
                .where(
                    DownloadArtifact.status.in_(
                        [DownloadStatus.PENDING, DownloadStatus.FAILED]
                    ),
                    # Never re-download parts whose group is already terminal
                    # or cleaned: finalize deleted the archive and the group
                    # will never be extracted — re-downloading (and the sweep
                    # unlink) repeated forever every run.
                    or_(
                        ArchiveGroup.id.is_(None),
                        ArchiveGroup.status.notin_(
                            [
                                GroupStatus.CLEANED,
                                GroupStatus.FAILED_TERMINAL,
                            ]
                        ),
                    ),
                )
                .order_by(
                    _download_priority(FileAttachment.filename),
                    DownloadArtifact.id,
                )
            )
            .scalars()
            .all()
        )
        # Release the read snapshot immediately — each download opens its own
        # short transaction via the per-artifact commit below.
        ctx.session.commit()

        if not pending_ids:
            logger.info("No pending downloads")
            return True

        logger.info("Found %d pending downloads", len(pending_ids))

        touched_group_ids: set[int] = set()

        # Process one at a time (sequential download constraint)
        for artifact_id in pending_ids:
            # Re-fetch the artifact in a fresh transaction for each download.
            artifact = ctx.session.get(
                DownloadArtifact,
                artifact_id,
                options=[
                    joinedload(DownloadArtifact.attachment)
                    .joinedload(FileAttachment.message)
                    .joinedload(Message.conversation),
                    joinedload(DownloadArtifact.group_part),
                ],
            )
            if artifact is None:
                ctx.session.commit()
                continue  # Deleted between our scan and now
            if artifact.status not in (DownloadStatus.PENDING, DownloadStatus.FAILED):
                ctx.session.commit()
                continue  # Already picked up by another process

            # Pre-extract relationship data and group_part info while the
            # transaction is still open (before _download_artifact commits it).
            group_part_group_id = (
                artifact.group_part.group_id if artifact.group_part else None
            )
            archive_name = artifact.attachment.filename or ""

            with log_context(artifact_id=artifact.id, archive_name=archive_name):
                if ctx.dry_run:
                    logger.info("[DRY RUN] Would download: %s", archive_name)
                    ctx.session.commit()
                    continue

                try:
                    success = await self._download_artifact(ctx, artifact)
                    if success:
                        ctx.files_downloaded += 1
                        if group_part_group_id is not None:
                            touched_group_ids.add(group_part_group_id)

                    # Commit the final status written by _download_artifact.
                    ctx.session.commit()
                except Exception as e:
                    logger.error("Error processing artifact %d: %s", artifact_id, e)
                    try:
                        ctx.session.rollback()
                    except Exception:
                        pass
                    ctx.errors.append(f"Download error: {e}")

        # Update group statuses
        await self._update_group_statuses(ctx, touched_group_ids)
        ctx.session.commit()

        logger.info("Downloaded %d files", ctx.files_downloaded)
        return True

    def _has_sufficient_disk(self, ctx: PipelineContext, min_free_mb: int = 10240) -> bool:
        """Check if there is enough free disk space before downloading."""
        try:
            usage = shutil.disk_usage(ctx.config.downloads_dir)
            free_mb = usage.free / (1024 * 1024)
            return free_mb >= min_free_mb
        except Exception as e:
            # Unknown disk state (unmounted dir, stat failure) is NOT "enough
            # disk" — downloading into a possibly-full/unavailable volume is
            # worse than waiting.
            logger.warning(
                "Disk usage check failed (%s) — treating as insufficient space",
                e,
            )
            return False

    async def _download_artifact(self, ctx: PipelineContext, artifact: DownloadArtifact) -> bool:
        """Download a single artifact.

        The session transaction is committed before network I/O begins so that
        a long download does not hold a PostgreSQL snapshot open (which blocks
        autovacuum on every table the initial SELECT touched).
        """
        if artifact.status == DownloadStatus.FAILED_TERMINAL:
            # A prefetch task already exhausted the retries for this artifact
            # (e.g. permanent Telegram error). The main-loop pop path calls
            # this unconditionally — don't burn another full retry cycle.
            logger.warning(
                "Artifact %d already FAILED_TERMINAL — skipping re-download",
                artifact.id,
            )
            return False
        attachment = artifact.attachment

        # Disk space guard: pause downloads if below configured threshold
        if not self._has_sufficient_disk(ctx, min_free_mb=ctx.config.extraction.min_free_disk_mb):
            logger.warning(
                "Skipping download — less than 10 GB free disk space. "
                "Waiting for finalize to reclaim space."
            )
            ctx.session.commit()
            return False

        # Pre-extract all relationship data while the transaction is open.
        # After the first ctx.session.commit() below, the artifact and its
        # relationships will be expired and we must not access them lazily.
        filename = attachment.filename or f"file_{attachment.id}"
        file_size = attachment.size or 0
        total_size_mb = file_size / 1024 / 1024
        platform_file_unique_id = attachment.platform_file_unique_id
        message = attachment.message
        conversation_platform_id = message.conversation.platform_id
        message_platform_id = message.platform_id
        channel_name = message.conversation.title or str(conversation_platform_id)

        # Determine destination path
        safe_filename = self._sanitize_filename(filename)
        dest_path = ctx.config.downloads_dir / safe_filename

        # Handle filename collisions
        counter = 1
        original_dest = dest_path
        while dest_path.exists():
            stem = original_dest.stem
            suffix = original_dest.suffix
            dest_path = original_dest.parent / f"{stem}_{counter}{suffix}"
            counter += 1

        # Download to temp file first, then rename (atomic)
        temp_dir = ctx.config.downloads_dir / ".tmp"
        temp_dir.mkdir(exist_ok=True)

        # Pre-download dedup: use Telegram's platform_file_unique_id to detect
        # identical files posted to multiple channels (same ID = guaranteed same content).
        # Saves bandwidth without needing slow SHA hashing of multi-GB archives.
        if platform_file_unique_id:
            existing = ctx.session.execute(
                select(DownloadArtifact)
                .join(FileAttachment, DownloadArtifact.attachment_id == FileAttachment.id)
                .where(
                    DownloadArtifact.status == DownloadStatus.COMPLETED,
                    DownloadArtifact.id != artifact.id,
                    DownloadArtifact.is_deleted.is_(False),
                    FileAttachment.platform_file_unique_id == platform_file_unique_id,
                    DownloadArtifact.local_path.isnot(None),
                )
                .limit(1)
            ).scalar_one_or_none()

            if existing and existing.local_path and Path(existing.local_path).exists():
                artifact.local_path = existing.local_path
                artifact.content_hash = existing.content_hash
                artifact.verified_size = existing.verified_size
                artifact.status = DownloadStatus.COMPLETED
                ctx.session.flush()
                ctx.session.commit()
                logger.info(
                    "Deduped %s (Telegram ID %s) — reusing existing download",
                    filename,
                    platform_file_unique_id,
                )
                return True

        max_retries = ctx.config.download.max_retries
        base_delay = ctx.config.download.retry_delay_seconds
        local_retry_count = 0

        for attempt in range(max_retries):
            temp_path: Path | None = None
            try:
                # Create temp file
                with tempfile.NamedTemporaryFile(
                    dir=temp_dir, delete=False, suffix=".partial"
                ) as tmp:
                    temp_path = Path(tmp.name)

                # Persist DOWNLOADING status + temp_path for crash recovery, and
                # commit immediately so the transaction snapshot is released before
                # the long network transfer begins.
                artifact.status = DownloadStatus.DOWNLOADING
                artifact.temp_path = str(temp_path)
                ctx.session.flush()
                ctx.session.commit()

                logger.info(
                    "Downloading: %s -> %s (%.1f MB)", filename, dest_path, total_size_mb
                )

                if ctx.notifier:
                    await ctx.notifier.downloading(channel_name, filename, total_size_mb)

                # Progress tracking with speed calculation
                download_start_time = time.time()
                last_log_time = download_start_time
                last_bytes = 0

                def progress_callback(current: int, total: int) -> None:
                    nonlocal last_log_time, last_bytes
                    now = time.time()
                    elapsed_since_log = now - last_log_time

                    # Update every 2 seconds (or every call if display is active for smoother bars)
                    min_interval = 0.5 if ctx.display else 2.0
                    if elapsed_since_log >= min_interval:
                        # Calculate current speed
                        bytes_since_last = current - last_bytes
                        speed_mbps = (bytes_since_last / elapsed_since_log) / (1024 * 1024)

                        # Calculate overall speed
                        total_elapsed = now - download_start_time
                        overall_speed_mbps = (
                            (current / total_elapsed) / (1024 * 1024) if total_elapsed > 0 else 0
                        )

                        # Progress percentage
                        pct = (current / total * 100) if total > 0 else 0

                        # ETA calculation
                        if overall_speed_mbps > 0:
                            remaining_mb = (total - current) / (1024 * 1024)
                            eta_seconds = remaining_mb / overall_speed_mbps
                            eta_str = (
                                f"{int(eta_seconds)}s"
                                if eta_seconds < 60
                                else f"{int(eta_seconds / 60)}m{int(eta_seconds % 60)}s"
                            )
                        else:
                            eta_str = "?"

                        if ctx.display:
                            ctx.display.download_progress(pct, speed_mbps, eta_str)
                        else:
                            logger.info(
                                "Progress: %.1f%% (%.1f/%.1f MB) - Speed: %.2f MB/s - ETA: %s",
                                pct,
                                current / (1024 * 1024),
                                total / (1024 * 1024),
                                speed_mbps,
                                eta_str,
                            )

                        last_log_time = now
                        last_bytes = current

                # Per-file timeout: 30 min or proportional to size (1 MB/s min)
                size_mb = total_size_mb or 100
                download_timeout = max(1800, int(size_mb * 2))
                # Stall threshold: full 5 min on first attempt; 60s on retries.
                # If the first attempt already stalled, the file is likely expired —
                # no point waiting another 5 min per retry to confirm it.
                stall_seconds = 300 if attempt == 0 else 60

                await ctx.adapter.download_message_media(
                    conversation_platform_id,
                    message_platform_id,
                    temp_path,
                    progress_callback=progress_callback,
                    timeout_seconds=download_timeout,
                    stall_seconds=stall_seconds,
                )

                # Log final download stats
                download_elapsed = time.time() - download_start_time
                if download_elapsed > 0:
                    final_speed = total_size_mb / download_elapsed
                    logger.info(
                        "Download complete: %.1f MB in %.1fs (avg %.2f MB/s)",
                        total_size_mb,
                        download_elapsed,
                        final_speed,
                    )

                # Verify size
                actual_size = temp_path.stat().st_size
                if file_size and actual_size != file_size:
                    raise RuntimeError(
                        f"Size mismatch: expected {file_size}, got {actual_size}"
                    )

                # Compute hash
                content_hash = await self._compute_hash(temp_path)

                # Atomic rename to final location. Never overwrite: another
                # concurrent prefetch download may have claimed dest_path since
                # the exists() check above (TOCTOU) — if it did, pick a new
                # unique name instead of silently clobbering the other file.
                if dest_path.exists():
                    counter = 1
                    stem = original_dest.stem
                    suffix = original_dest.suffix
                    while True:
                        candidate = original_dest.parent / f"{stem}_{counter}{suffix}"
                        if not candidate.exists():
                            dest_path = candidate
                            break
                        counter += 1
                temp_path.rename(dest_path)

                # Update artifact — safe to set attributes on expired object;
                # SQLAlchemy tracks them as pending without triggering a lazy load.
                artifact.local_path = str(dest_path)
                artifact.temp_path = None
                artifact.verified_size = actual_size
                artifact.content_hash = content_hash
                artifact.status = DownloadStatus.COMPLETED

                logger.info("Downloaded successfully: %s (%d bytes)", dest_path.name, actual_size)
                return True

            except asyncio.CancelledError:
                # Cancellation = the pipeline is shutting down. Clean up the
                # partial file, leave the artifact PENDING (startup recovery
                # resets DOWNLOADING→PENDING), and re-raise so the caller sees
                # the cancellation. Burning retry attempts on shutdown delays
                # the stop and corrupts the retry counter.
                if temp_path is not None:
                    try:
                        temp_path.unlink(missing_ok=True)
                    except Exception:
                        pass
                # Persist PENDING + clear the stale temp_path so the dashboard
                # doesn't show a phantom DOWNLOADING artifact with a dangling
                # path for the rest of the run.
                try:
                    artifact.temp_path = None
                    artifact.status = DownloadStatus.PENDING
                    ctx.session.commit()
                except Exception:
                    try:
                        ctx.session.rollback()
                    except Exception:
                        pass
                raise
            except Exception as e:
                local_retry_count += 1

                if temp_path is not None:
                    temp_path.unlink(missing_ok=True)

                # If the session was poisoned by a DB error, rollback before
                # touching model attributes (which would trigger autoflush on
                # a DEACTIVE session and raise PendingRollbackError).
                try:
                    ctx.session.rollback()
                except Exception:
                    pass

                artifact.error_message = repr(e)[:500]
                artifact.retry_count = local_retry_count
                artifact.temp_path = None

                if _is_permanent_error(e):
                    logger.warning("Download permanently unavailable for %s: %s", filename, e)
                    artifact.status = DownloadStatus.FAILED_TERMINAL
                    return False

                logger.error("Download failed for %s: %r", filename, e, exc_info=True)
                artifact.status = DownloadStatus.FAILED

                if attempt < max_retries - 1:
                    # Commit FAILED state and release the transaction before
                    # sleeping so we don't hold a connection idle during the delay.
                    ctx.session.flush()
                    ctx.session.commit()
                    delay = backoff_delay(attempt, base_delay, base_delay * 8)
                    logger.info(
                        "Retrying download in %.1fs (attempt %d/%d)",
                        delay,
                        attempt + 2,
                        max_retries,
                    )
                    await asyncio.sleep(delay)
                    continue

                artifact.status = DownloadStatus.FAILED_TERMINAL
                return False

        return False

    async def _compute_hash(self, path: Path) -> str:
        """Compute SHA256 hash of file without blocking the event loop."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._compute_hash_sync, path)

    @staticmethod
    def _compute_hash_sync(path: Path) -> str:
        sha256 = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(65536):
                sha256.update(chunk)
        return sha256.hexdigest()

    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for filesystem safety."""
        # Remove/replace dangerous characters
        safe = filename.replace("/", "_").replace("\\", "_").replace("\x00", "")
        # Limit length
        if len(safe) > 200:
            safe = safe[:200]
        return safe or "unnamed_file"

    async def _update_group_statuses(
        self, ctx: PipelineContext, group_ids: set[int] | None = None
    ) -> None:
        """Update archive group statuses based on download completions."""
        if group_ids:
            query = select(ArchiveGroup).where(
                ArchiveGroup.id.in_(group_ids),
                ArchiveGroup.status == GroupStatus.INCOMPLETE,
            )
        else:
            query = select(ArchiveGroup).where(ArchiveGroup.status == GroupStatus.INCOMPLETE)

        groups = (
            ctx.session.execute(
                query.options(
                    joinedload(ArchiveGroup.parts)
                    .joinedload(ArchiveGroupPart.artifact)
                    .joinedload(DownloadArtifact.attachment)
                )
            )
            .unique()
            .scalars()
            .all()
        )

        for group in groups:
            # Check if all parts are downloaded.  A FAILED_TERMINAL part is
            # "covered" when another part in the group holds the SAME physical
            # file (identical platform_file_unique_id) and is COMPLETED — the
            # repost copy can serve as the archive part.
            completed_unique_ids = {
                part.artifact.attachment.platform_file_unique_id
                for part in group.parts
                if part.artifact.attachment
                and part.artifact.status == DownloadStatus.COMPLETED
                and part.artifact.attachment.platform_file_unique_id
            }
            all_downloaded = all(
                part.artifact.status == DownloadStatus.COMPLETED
                or (
                    part.artifact.status == DownloadStatus.FAILED_TERMINAL
                    and part.artifact.attachment
                    and part.artifact.attachment.platform_file_unique_id
                    in completed_unique_ids
                )
                for part in group.parts
            )
            # Check if any parts are still pending, downloading or transiently
            # failed (FAILED is retryable — counting it as "active" prevents
            # the group from being marked FAILED_TERMINAL while a retryable
            # part is still recoverable on the next pass).
            any_active = any(
                part.artifact.status
                in (
                    DownloadStatus.PENDING,
                    DownloadStatus.DOWNLOADING,
                    DownloadStatus.FAILED,
                )
                for part in group.parts
            )
            # Check if any parts permanently failed
            any_terminal = any(
                part.artifact.status == DownloadStatus.FAILED_TERMINAL for part in group.parts
            )

            if group.parts and not any_active:
                if all_downloaded:
                    if len(group.parts) < group.expected_part_count:
                        logger.warning(
                            "Group %s has %d/%d parts but none pending — marking READY anyway",
                            group.base_name,
                            len(group.parts),
                            group.expected_part_count,
                        )
                    group.status = GroupStatus.READY
                elif any_terminal:
                    # All downloads either completed or permanently failed — group is unrecoverable
                    logger.warning(
                        "Group %s has permanently failed download(s) — marking FAILED_TERMINAL",
                        group.base_name,
                    )
                    group.status = GroupStatus.FAILED_TERMINAL
