"""Pipeline orchestrator - coordinates all pipeline stages."""

import asyncio
import json
import logging
import os
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session, joinedload

from telecrime.adapters.base import BaseAdapter
from telecrime.config import Config
from telecrime.database import get_session
from telecrime.extractor.seven_zip import SevenZipExtractor
from telecrime.models import (
    ArchiveGroup,
    ArchiveGroupPart,
    DownloadArtifact,
    FileAttachment,
    PipelineRun,
)
from telecrime.pipeline.lock import pipeline_run_lock
from telecrime.states import DownloadStatus, GroupStatus

if TYPE_CHECKING:
    from telecrime.notify import TelegramNotifier
    from telecrime.pipeline.display import PipelineDisplay

logger = logging.getLogger(__name__)


@dataclass
class PipelineContext:
    """Shared context passed through pipeline stages."""

    config: Config
    session: Session
    adapter: BaseAdapter
    dry_run: bool = False
    notifier: Optional["TelegramNotifier"] = None
    display: Optional["PipelineDisplay"] = None

    # Schema capability flags — computed once and cached to avoid repeated DB roundtrips
    has_soft_hash_column: bool | None = None

    # Statistics
    conversations_processed: int = 0
    messages_processed: int = 0
    files_discovered: int = 0
    files_downloaded: int = 0
    archives_extracted: int = 0
    credentials_parsed: int = 0
    duplicates_skipped: int = 0
    errors: list[str] = field(default_factory=list)

    async def notify(self, message: str):
        """Send a notification if notifier is available."""
        if self.notifier:
            await self.notifier.send(message)


class PipelineStage:
    """Base class for pipeline stages."""

    name: str = "base"

    async def run(self, ctx: PipelineContext) -> bool:
        """Execute the stage.

        Args:
            ctx: Pipeline context with config, session, adapter

        Returns:
            True if stage completed successfully, False otherwise
        """
        raise NotImplementedError


class Pipeline:
    """Main pipeline orchestrator."""

    def __init__(self, config: Config, session: Session, adapter: BaseAdapter):
        self.config = config
        self.session = session
        self.adapter = adapter
        self.stages: list[PipelineStage] = []

    def add_stage(self, stage: PipelineStage) -> "Pipeline":
        """Add a stage to the pipeline."""
        self.stages.append(stage)
        return self

    async def run(
        self,
        dry_run: bool = False,
        notifier: Optional["TelegramNotifier"] = None,
        display: Optional["PipelineDisplay"] = None,
    ) -> PipelineContext:
        """Run all pipeline stages in order.

        Args:
            dry_run: If True, don't actually download/extract files
            notifier: Optional notifier for progress updates

        Returns:
            PipelineContext with results and statistics
        """
        ctx = PipelineContext(
            config=self.config,
            session=self.session,
            adapter=self.adapter,
            dry_run=dry_run,
            notifier=notifier,
            display=display,
        )

        with pipeline_run_lock(self.config.data_dir):
            logger.info("Starting pipeline with %d stages", len(self.stages))
            run_record = _start_pipeline_run(self.session, mode="batch", dry_run=dry_run)
            stages_completed: list[str] = []
            stages_failed: list[str] = []

            if notifier:
                queue_size = self.session.execute(
                    select(func.count(DownloadArtifact.id)).where(
                        DownloadArtifact.status == DownloadStatus.PENDING
                    )
                ).scalar()
                free_disk_gb = None
                try:
                    import shutil as _shutil
                    free_disk_gb = _shutil.disk_usage(
                        str(self.config.downloads_dir)
                    ).free / (1024 ** 3)
                except Exception:
                    pass
                await notifier.pipeline_start(
                    self.config.extraction.target_extensions,
                    queue_size=int(queue_size or 0),
                    free_disk_gb=free_disk_gb,
                )

            try:
                for stage in self.stages:
                    logger.info("Running stage: %s", stage.name)
                    if display:
                        display.stage_start(stage.name)
                    if notifier:
                        await notifier.stage_start(stage.name)

                    try:
                        success = await stage.run(ctx)
                        if not success:
                            logger.warning("Stage %s returned failure, continuing...", stage.name)

                        _record_stage_success(stages_completed, stage.name)
                        if display:
                            display.stage_complete(stage.name)
                        if notifier:
                            await notifier.stage_complete(stage.name)

                    except (Exception, asyncio.CancelledError) as e:
                        logger.error("Stage %s failed with exception: %s", stage.name, e)
                        ctx.errors.append(f"{stage.name}: {str(e)}")
                        _record_stage_failure(stages_failed, stage.name)
                        try:
                            self.session.rollback()
                        except Exception:
                            pass
                        if display:
                            display.stage_error(stage.name)
                        if notifier:
                            try:
                                await notifier.error(str(e), stage=stage.name)
                            except Exception:
                                pass
                        # Continue with next stage - don't abort entire pipeline
            finally:
                _finish_pipeline_run(
                    self.session,
                    run_record,
                    ctx,
                    stages_completed=stages_completed,
                    stages_failed=stages_failed,
                )

        logger.info(
            "Pipeline complete. Processed %d conversations, %d messages, %d files, %d credentials",
            ctx.conversations_processed,
            ctx.messages_processed,
            ctx.files_discovered,
            ctx.credentials_parsed,
        )

        if notifier:
            elapsed = None
            if run_record is not None and run_record.started_at is not None:
                _now = datetime.now(UTC)
                _started = run_record.started_at
                if _started.tzinfo is None:
                    _started = _started.replace(tzinfo=UTC)
                elapsed = max(0, (_now - _started).total_seconds())
            await notifier.pipeline_complete(
                {
                    "archives_extracted": ctx.archives_extracted,
                    "credentials_parsed": ctx.credentials_parsed,
                    "duplicates_skipped": ctx.duplicates_skipped,
                    "errors": len(ctx.errors),
                    "elapsed_seconds": elapsed,
                    "Conversations": ctx.conversations_processed,
                    "Messages": ctx.messages_processed,
                    "Files discovered": ctx.files_discovered,
                    "Files downloaded": ctx.files_downloaded,
                }
            )

        return ctx


def create_default_pipeline(
    config: Config,
    session: Session,
    adapter: BaseAdapter,
) -> Pipeline:
    """Create pipeline with all default stages."""
    from telecrime.pipeline.acquire import AcquireStage
    from telecrime.pipeline.channel_discover import ChannelDiscoverStage
    from telecrime.pipeline.discover import DiscoverStage
    from telecrime.pipeline.enrich import EnrichStage
    from telecrime.pipeline.extract import ExtractStage
    from telecrime.pipeline.finalize import FinalizeStage
    from telecrime.pipeline.ingest import IngestStage
    from telecrime.pipeline.parse import ParseStage
    from telecrime.pipeline.plan import PlanStage

    pipeline = Pipeline(config, session, adapter)
    pipeline.add_stage(IngestStage())
    pipeline.add_stage(ChannelDiscoverStage())
    pipeline.add_stage(DiscoverStage())
    pipeline.add_stage(PlanStage())
    pipeline.add_stage(AcquireStage())
    pipeline.add_stage(EnrichStage())
    pipeline.add_stage(ExtractStage())
    pipeline.add_stage(ParseStage())
    pipeline.add_stage(FinalizeStage())

    return pipeline


async def _prefetch_download(
    ctx: PipelineContext,
    acquire_stage,
    artifact,
) -> bool:
    """Download an artifact in the background while the current one is processed.

    Only downloads the file — does NOT update group statuses or commit the session.
    The caller is responsible for calling _update_group_statuses and committing after
    awaiting this task, to avoid concurrent session access.
    """
    try:
        return await acquire_stage._download_artifact(ctx, artifact)
    except (Exception, asyncio.CancelledError) as e:
        logger.warning("Background download error for artifact %d: %s", artifact.id, e)
        return False


async def _process_ready_group(
    *,
    config: Config,
    engine,
    adapter: BaseAdapter,
    group_id: int,
    dry_run: bool,
    notifier: Optional["TelegramNotifier"],
) -> dict[str, Any]:
    """Process one READY group in an isolated DB session."""
    from telecrime.pipeline.extract import ExtractStage
    from telecrime.pipeline.finalize import FinalizeStage
    from telecrime.pipeline.parse import ParseStage

    with get_session(engine) as task_session:
        group = (
            task_session.execute(
                select(ArchiveGroup)
                .where(ArchiveGroup.id == group_id)
                .options(joinedload(ArchiveGroup.parts).joinedload(ArchiveGroupPart.artifact))
            )
            .unique()
            .scalar_one_or_none()
        )
        if group is None or group.status != GroupStatus.READY:
            return {"group_id": group_id, "skipped": True}

        task_ctx = PipelineContext(
            config=config,
            session=task_session,
            adapter=adapter,
            dry_run=dry_run,
            notifier=notifier,
            display=None,
        )
        extract_stage = ExtractStage()
        parse_stage = ParseStage()
        finalize_stage = FinalizeStage()
        extractor = SevenZipExtractor(config.extraction.extractor_path)

        success = await extract_stage._extract_group(task_ctx, group, extractor)
        if success:
            task_ctx.archives_extracted += 1
        task_session.commit()

        await parse_stage.run_group(task_ctx, group_id)
        task_session.commit()

        await finalize_stage.run_group(task_ctx, group_id)
        task_session.commit()

        return {
            "group_id": group_id,
            "skipped": False,
            "archives_extracted": task_ctx.archives_extracted,
            "credentials_parsed": task_ctx.credentials_parsed,
            "duplicates_skipped": task_ctx.duplicates_skipped,
            "errors": list(task_ctx.errors),
        }


async def _process_ready_groups_batch(
    *,
    ctx: PipelineContext,
    engine,
    group_ids: Sequence[int],
    parallel_groups: int,
) -> int:
    """Process READY groups in a small parallel batch using isolated sessions."""
    if not group_ids:
        return 0

    sem = asyncio.Semaphore(max(1, parallel_groups))

    async def _run(group_id: int) -> dict[str, Any]:
        async with sem:
            try:
                return await _process_ready_group(
                    config=ctx.config,
                    engine=engine,
                    adapter=ctx.adapter,
                    group_id=group_id,
                    dry_run=ctx.dry_run,
                    notifier=ctx.notifier,
                )
            except (Exception, asyncio.CancelledError) as e:
                logger.error("Error processing READY group %s: %s", group_id, e)
                return {
                    "group_id": group_id,
                    "skipped": False,
                    "archives_extracted": 0,
                    "credentials_parsed": 0,
                    "duplicates_skipped": 0,
                    "errors": [str(e)],
                }

    results = await asyncio.gather(*[_run(group_id) for group_id in group_ids])
    completed = 0
    for result in results:
        if result.get("skipped"):
            continue
        completed += 1
        ctx.archives_extracted += int(result.get("archives_extracted", 0))
        ctx.credentials_parsed += int(result.get("credentials_parsed", 0))
        ctx.duplicates_skipped += int(result.get("duplicates_skipped", 0))
        ctx.errors.extend(str(e) for e in result.get("errors", []))
    return completed


_PRIORITY_REINGEST_INTERVAL = timedelta(hours=1)
_FULL_REINGEST_INTERVAL = timedelta(hours=6)


def _download_priority(filename_col):
    """Return a CASE expression: ULP files first (0), .txt files second (1), rest last (2)."""
    lower = func.lower(filename_col)
    return case(
        (lower.contains("ulp"), 0),
        (lower.endswith(".txt"), 1),
        else_=2,
    )


def _artifact_group_id(session: "Session", artifact_id: int) -> "int | None":
    """Return the group_id for an artifact (cheap indexed lookup)."""
    return session.execute(
        select(ArchiveGroupPart.group_id).where(ArchiveGroupPart.artifact_id == artifact_id)
    ).scalar_one_or_none()


def _next_pending_artifact(session: "Session"):
    """Pick the next artifact to download, prioritising group completion.

    Strategy: find incomplete groups that already have downloaded parts, then
    pick a pending artifact from the group closest to completion (fewest
    remaining parts).  Falls back to ULP-first FIFO when no partially-downloaded
    groups exist.
    """
    # Sub-query: for each incomplete group, count pending parts
    _retriable = (DownloadStatus.PENDING, DownloadStatus.FAILED)
    pending_count = (
        select(
            ArchiveGroupPart.group_id.label("group_id"),
            func.count().label("pending_parts"),
        )
        .join(DownloadArtifact, ArchiveGroupPart.artifact_id == DownloadArtifact.id)
        .where(DownloadArtifact.status.in_(_retriable))
        .group_by(ArchiveGroupPart.group_id)
        .subquery()
    )

    # Find incomplete groups that have at least one completed part (partially downloaded)
    completed_count = (
        select(
            ArchiveGroupPart.group_id.label("group_id"),
            func.count().label("completed_parts"),
        )
        .join(DownloadArtifact, ArchiveGroupPart.artifact_id == DownloadArtifact.id)
        .where(DownloadArtifact.status == DownloadStatus.COMPLETED)
        .group_by(ArchiveGroupPart.group_id)
        .subquery()
    )

    # Pick group with fewest pending parts that has at least one completed part.
    # ULP groups always come first regardless of part count.
    best_group = (
        session.execute(
            select(pending_count.c.group_id)
            .join(completed_count, pending_count.c.group_id == completed_count.c.group_id)
            .join(ArchiveGroup, ArchiveGroup.id == pending_count.c.group_id)
            .join(ArchiveGroupPart, ArchiveGroupPart.group_id == ArchiveGroup.id)
            .join(DownloadArtifact, DownloadArtifact.id == ArchiveGroupPart.artifact_id)
            .join(FileAttachment, FileAttachment.id == DownloadArtifact.attachment_id)
            .where(ArchiveGroup.status == GroupStatus.INCOMPLETE)
            .group_by(pending_count.c.group_id, pending_count.c.pending_parts)
            .order_by(
                func.min(_download_priority(FileAttachment.filename)).asc(),
                pending_count.c.pending_parts.asc(),
            )
            .limit(1)
        )
        .scalar_one_or_none()
    )

    if best_group is not None:
        # Get a pending artifact from this group, ULP files first
        artifact = session.execute(
            select(DownloadArtifact)
            .join(ArchiveGroupPart, ArchiveGroupPart.artifact_id == DownloadArtifact.id)
            .join(FileAttachment, FileAttachment.id == DownloadArtifact.attachment_id)
            .where(
                ArchiveGroupPart.group_id == best_group,
                DownloadArtifact.status.in_(_retriable),
            )
            .options(joinedload(DownloadArtifact.attachment))
            .order_by(_download_priority(FileAttachment.filename), DownloadArtifact.id)
            .limit(1)
        ).scalar_one_or_none()
        if artifact:
            return artifact

    # Fallback: ULP files first, then newest groups first.
    # Older INCOMPLETE groups (> 30 days, zero completed parts) are deprioritised
    # so that a backlog of undownloadable archives does not block new ones.
    _stale_cutoff = datetime.now(UTC) - timedelta(days=30)
    stale_group_ids = (
        select(ArchiveGroupPart.group_id)
        .join(ArchiveGroup, ArchiveGroup.id == ArchiveGroupPart.group_id)
        .join(DownloadArtifact, DownloadArtifact.id == ArchiveGroupPart.artifact_id)
        .where(
            ArchiveGroup.status == GroupStatus.INCOMPLETE,
            ArchiveGroup.updated_at < _stale_cutoff,
        )
        .group_by(ArchiveGroupPart.group_id)
        .having(func.sum(case((DownloadArtifact.status == DownloadStatus.COMPLETED, 1), else_=0)) == 0)
        .scalar_subquery()
    )

    return session.execute(
        select(DownloadArtifact)
        .join(FileAttachment, DownloadArtifact.attachment_id == FileAttachment.id)
        .join(ArchiveGroupPart, ArchiveGroupPart.artifact_id == DownloadArtifact.id)
        .join(ArchiveGroup, ArchiveGroup.id == ArchiveGroupPart.group_id)
        .where(
            DownloadArtifact.status.in_(_retriable),
            ~ArchiveGroup.id.in_(stale_group_ids),
        )
        .options(joinedload(DownloadArtifact.attachment))
        .order_by(_download_priority(FileAttachment.filename), ArchiveGroup.updated_at.desc())
        .limit(1)
    ).scalar_one_or_none()


def _read_shutdown_request():
    from telecrime.scheduler import read_shutdown_request

    return read_shutdown_request()


def _update_display_shutdown(
    display,
    requested: bool,
    mode: str | None = None,
    requested_at: str | None = None,
    state: str | None = None,
) -> None:
    if display is None:
        return
    callback = getattr(display, "set_shutdown_state", None)
    if callable(callback):
        callback(
            requested=requested,
            mode=mode,
            requested_at=requested_at,
            state=state,
        )


async def run_sequential_pipeline(
    config: Config,
    session: Session,
    adapter: BaseAdapter,
    dry_run: bool = False,
    notifier: Optional["TelegramNotifier"] = None,
    limit: int | None = None,
    display: Optional["PipelineDisplay"] = None,
    prefetch_count: int = 2,
) -> PipelineContext:
    """Run pipeline processing one archive at a time, with N-archive prefetch.

    While extract+parse+finalize runs on archive N, downloads N+1 through N+prefetch_count
    run concurrently in the background.  prefetch_count=1 is the old single-prefetch
    behaviour; prefetch_count=2 (default) keeps two downloads queued.

    Benefits:
    - Immediate results (credentials available as archives are processed)
    - Better disk space management (cleanup after each archive)
    - Crash safety (progress saved per archive)
    - Prefetch eliminates idle network time during extract/parse/finalize

    Args:
        config: Application config
        session: Database session
        adapter: Platform adapter
        dry_run: Preview mode
        notifier: Optional notifier
        limit: Max archives to process (None = unlimited)

    Returns:
        PipelineContext with results
    """
    from telecrime.pipeline.acquire import AcquireStage
    from telecrime.pipeline.channel_discover import ChannelDiscoverStage, ChannelJoiner
    from telecrime.pipeline.discover import DiscoverStage
    from telecrime.pipeline.extract import ExtractStage
    from telecrime.pipeline.finalize import FinalizeStage
    from telecrime.pipeline.ingest import IngestStage
    from telecrime.pipeline.parse import ParseStage
    from telecrime.pipeline.plan import PlanStage

    ctx = PipelineContext(
        config=config,
        session=session,
        adapter=adapter,
        dry_run=dry_run,
        notifier=notifier,
        display=display,
    )

    with pipeline_run_lock(config.data_dir):
        logger.info("Starting sequential pipeline (one archive at a time, with prefetch)")
        run_record = _start_pipeline_run(session, mode="sequential", dry_run=dry_run)
        stages_completed: list[str] = []
        stages_failed: list[str] = []
        shutdown_after_current_archive = False

        def _shutdown_checkpoint(state: str = "draining") -> bool:
            request = _read_shutdown_request()
            if not request:
                _update_display_shutdown(display, requested=False)
                return False
            _update_display_shutdown(
                display,
                requested=True,
                mode=request.get("mode"),
                requested_at=request.get("requested_at"),
                state=state,
            )
            return True

        async def _drain_prefetch_queue(reset_pending: bool) -> None:
            if not _prefetch_queue:
                return

            for task, artifact in list(_prefetch_queue):
                if task.done():
                    try:
                        if task.result():
                            ctx.files_downloaded += 1
                    except (Exception, asyncio.CancelledError) as e:
                        logger.warning(
                            "Prefetch task for artifact %s raised: %s",
                            getattr(artifact, "id", "?"), e,
                        )
                    continue

                task.cancel()
                await asyncio.wait({task}, timeout=5.0)
                if not reset_pending:
                    continue

                if getattr(artifact, "temp_path", None):
                    try:
                        Path(str(artifact.temp_path)).unlink(missing_ok=True)
                    except Exception as e:
                        logger.debug("Could not remove temp file %s: %s", artifact.temp_path, e)
                if getattr(artifact, "status", None) == DownloadStatus.DOWNLOADING:
                    artifact.status = DownloadStatus.PENDING
                    artifact.temp_path = None

            await acquire_stage._update_group_statuses(ctx)
            session.commit()
            _prefetch_queue.clear()

        try:
            # Stage 1-4: Ingest, Channel Discover (DB only), Discover, Plan (run once)
            for stage in [IngestStage(), ChannelDiscoverStage(), DiscoverStage(), PlanStage()]:
                logger.info("Running stage: %s", stage.name)
                if display:
                    display.stage_start(stage.name)
                try:
                    success = await stage.run(ctx)
                    if not success:
                        logger.warning("Stage %s returned failure, continuing...", stage.name)
                    _record_stage_success(stages_completed, stage.name)
                    if display:
                        display.stage_complete(stage.name)
                        display.update_messages(ctx.messages_processed)
                    if notifier:
                        await notifier.stage_complete(stage.name)
                    session.commit()
                except (Exception, asyncio.CancelledError) as e:
                    logger.error("Stage %s failed with exception: %s", stage.name, e)
                    ctx.errors.append(f"{stage.name}: {str(e)}")
                    _record_stage_failure(stages_failed, stage.name)
                    if display:
                        display.stage_error(stage.name)
                    if notifier:
                        try:
                            await notifier.error(str(e), stage=stage.name)
                        except Exception:
                            pass
                    try:
                        session.rollback()
                    except Exception:
                        pass

            acquire_stage = AcquireStage()
            extract_stage = ExtractStage()
            parse_stage = ParseStage()
            finalize_stage = FinalizeStage()
            extractor = SevenZipExtractor(config.extraction.extractor_path)

            channel_joiner = ChannelJoiner()

            # Startup recovery: reset any artifacts stuck in DOWNLOADING state
            # from a previous crashed run so they are re-downloaded this run.
            acquire_stage.recover_stuck_downloads(session, config.downloads_dir)

            # Startup recovery: promote INCOMPLETE groups whose parts are all
            # COMPLETED to READY. recover_stuck_downloads may mark artifacts
            # COMPLETED (file already on disk) without updating group status,
            # leaving groups permanently stranded as INCOMPLETE.
            _incomplete_ready = (
                session.execute(
                    select(ArchiveGroup)
                    .options(
                        joinedload(ArchiveGroup.parts).joinedload(ArchiveGroupPart.artifact)
                    )
                    .where(ArchiveGroup.status == GroupStatus.INCOMPLETE)
                )
                .unique()
                .scalars()
                .all()
            )
            _promoted = 0
            _terminal = 0
            for _g in _incomplete_ready:
                if not _g.parts:
                    continue
                statuses = {p.artifact.status for p in _g.parts}
                no_active = not (statuses & {DownloadStatus.PENDING, DownloadStatus.DOWNLOADING})
                if no_active:
                    if statuses <= {DownloadStatus.COMPLETED}:
                        _g.status = GroupStatus.READY
                        _promoted += 1
                    elif DownloadStatus.FAILED_TERMINAL in statuses and DownloadStatus.COMPLETED not in statuses:
                        _g.status = GroupStatus.FAILED_TERMINAL
                        _terminal += 1
            if _promoted or _terminal:
                session.commit()
            if _promoted:
                logger.info("Startup recovery: promoted %d INCOMPLETE→READY groups (all parts downloaded)", _promoted)
            if _terminal:
                logger.info(
                    "Startup recovery: marked %d INCOMPLETE→FAILED_TERMINAL groups "
                    "(all downloads permanently failed)",
                    _terminal,
                )

            # Startup recovery: reset any groups stuck in EXTRACTING state (crash
            # during extraction) back to READY so they are re-processed this run.
            extracting_stuck = session.execute(
                select(ArchiveGroup).where(ArchiveGroup.status == GroupStatus.EXTRACTING)
            ).scalars().all()
            if extracting_stuck:
                logger.info(
                    "Resetting %d groups stuck in EXTRACTING → READY", len(extracting_stuck)
                )
                for _g in extracting_stuck:
                    _g.status = GroupStatus.READY
                session.commit()

            total_pending = (
                session.execute(
                    select(func.count(DownloadArtifact.id)).where(
                        DownloadArtifact.status == DownloadStatus.PENDING
                    )
                ).scalar()
                or 0
            )

            if display and total_pending:
                display.set_archive_total(min(total_pending, limit or total_pending))

            if dry_run:
                preview_count = min(total_pending, limit or total_pending)
                logger.info(
                    "Dry-run sequential mode: previewing %d pending downloads without acquisition",
                    preview_count,
                )
                preview_artifacts = (
                    session.execute(
                        select(DownloadArtifact)
                        .where(DownloadArtifact.status == DownloadStatus.PENDING)
                        .options(joinedload(DownloadArtifact.attachment))
                        .order_by(DownloadArtifact.id)
                        .limit(preview_count)
                    )
                    .scalars()
                    .all()
                )
                for artifact in preview_artifacts:
                    filename = (
                        artifact.attachment.filename
                        if artifact.attachment and artifact.attachment.filename
                        else f"artifact_{artifact.id}"
                    )
                    logger.info("[DRY RUN] Would download: %s", filename)
                _record_stage_success(stages_completed, "acquire")
                session.commit()
                return ctx

            processed = 0
            archives_to_process = limit or 999999
            # N-prefetch queue: list of (asyncio.Task, artifact) pairs in FIFO order.
            # Cap at 3 to avoid Telegram rate-limiting / temporary ban.
            _effective_prefetch = min(max(prefetch_count, 1), 3)
            _parallel_ready_groups = max(
                1,
                int(os.environ.get("TELECRIME_READY_GROUP_CONCURRENCY", "3")),
            )
            _prefetch_queue: list[tuple[asyncio.Task, DownloadArtifact]] = []
            engine = session.get_bind()

            # Track last re-ingest times. Initialised to now so the first reingest fires
            # after one full interval rather than immediately (ingest just ran at startup).
            _last_priority_reingest = datetime.now(UTC)
            _last_full_reingest = datetime.now(UTC)

            async def _run_reingest(priority_only: bool) -> None:
                """Re-ingest channels, classify new attachments, and queue new downloads.

                Errors are caught here so a Telegram hiccup can't abort the pipeline.
                """
                label = "priority" if priority_only else "full"
                try:
                    logger.info("Periodic %s re-ingest starting", label)
                    await IngestStage(priority_only=priority_only).run(ctx)
                    session.commit()
                    await DiscoverStage().run(ctx)
                    await PlanStage().run(ctx)
                    session.commit()
                    new_total = (
                        session.execute(
                            select(func.count(DownloadArtifact.id)).where(
                                DownloadArtifact.status == DownloadStatus.PENDING
                            )
                        ).scalar() or 0
                    )
                    if display:
                        display.set_archive_total(min(new_total, archives_to_process))
                    logger.info("Periodic %s re-ingest done; %d downloads pending", label, new_total)
                except (Exception, asyncio.CancelledError) as _e:
                    logger.warning("Periodic %s re-ingest failed, continuing: %s", label, _e)
                    try:
                        session.rollback()
                    except Exception:
                        pass

            while processed < archives_to_process:
                if shutdown_after_current_archive:
                    await _drain_prefetch_queue(reset_pending=True)
                    logger.info("Shutdown requested — stopping after current archive boundary")
                    break

                # Periodic re-ingest: keeps priority (time-limited) and all channels fresh
                # during long pipeline runs without blocking downloads.
                _now = datetime.now(UTC)
                if _now - _last_full_reingest >= _FULL_REINGEST_INTERVAL:
                    await _run_reingest(priority_only=False)
                    _last_full_reingest = _now
                    _last_priority_reingest = _now  # full reingest covers priority channels too
                elif _now - _last_priority_reingest >= _PRIORITY_REINGEST_INTERVAL:
                    await _run_reingest(priority_only=True)
                    _last_priority_reingest = _now

                # Process any READY groups before downloading more.
                # This frees disk space (finalize deletes archives) so
                # subsequent downloads have room.
                ready_groups = (
                    session.execute(
                        select(ArchiveGroup.id)
                        .join(ArchiveGroupPart, ArchiveGroupPart.group_id == ArchiveGroup.id)
                        .join(DownloadArtifact, DownloadArtifact.id == ArchiveGroupPart.artifact_id)
                        .join(FileAttachment, FileAttachment.id == DownloadArtifact.attachment_id)
                        .where(ArchiveGroup.status == GroupStatus.READY)
                        .group_by(ArchiveGroup.id)
                        .order_by(
                            func.min(_download_priority(FileAttachment.filename)).asc(),
                            func.min(ArchiveGroup.updated_at).asc(),
                        )
                    )
                    .scalars()
                    .all()
                )
                # Release the read snapshot before long extract/parse awaits.
                session.commit()
                if ready_groups:
                    logger.info(
                        "Processing %d READY groups before next download (parallel=%d)",
                        len(ready_groups),
                        _parallel_ready_groups,
                    )
                    if display:
                        display.stage_start("extract")
                    processed += await _process_ready_groups_batch(
                        ctx=ctx,
                        engine=engine,
                        group_ids=ready_groups,
                        parallel_groups=_parallel_ready_groups,
                    )
                    if display:
                        display.update_creds(ctx.credentials_parsed)
                        display.stage_complete("extract")
                    session.expire_all()
                    if _shutdown_checkpoint():
                        shutdown_after_current_archive = True

                if shutdown_after_current_archive:
                    await _drain_prefetch_queue(reset_pending=True)
                    logger.info("Shutdown requested after READY-group processing")
                    break

                # Sweep any orphaned EXTRACTED groups (stuck from previous runs or crashes).
                # This runs before each download attempt so disk pressure can't create a deadlock
                # where full disk → downloads fail → finalize never runs → disk stays full.
                extracted_orphan_count = session.execute(
                    select(func.count(ArchiveGroup.id)).where(
                        ArchiveGroup.status == GroupStatus.EXTRACTED
                    )
                ).scalar() or 0
                if extracted_orphan_count:
                    logger.info(
                        "Sweeping %d orphaned EXTRACTED groups before next download",
                        extracted_orphan_count,
                    )
                    # Parse credentials first — an EXTRACTED group may not yet have been
                    # parsed (e.g. previous run was killed mid-parse). Finalizing without
                    # parsing silently discards credentials from the archive.
                    # stage_start("parse") is required so the progress heartbeat marks
                    # progress (it only does so for stage in {extract, parse}); without
                    # it, parsing a large orphaned group runs with stage=None and the
                    # watchdog false-kills the pipeline at the 1200s stale threshold,
                    # producing an infinite kill/restart loop that never finalizes.
                    if display:
                        display.stage_start("parse")
                    await parse_stage.run(ctx)
                    if display:
                        display.stage_start("finalize")
                    await finalize_stage.run(ctx)
                    session.commit()
                    session.expire_all()

                if _prefetch_queue:
                    if _shutdown_checkpoint():
                        shutdown_after_current_archive = True
                        await _drain_prefetch_queue(reset_pending=True)
                        logger.info("Shutdown requested before starting another prefetched archive")
                        break
                    # Pop the next prefetched (task, artifact) pair from the queue.
                    _next_task, artifact = _prefetch_queue.pop(0)
                    # Await with a hard cap: a permanently-stuck Telethon reconnection
                    # loop can prevent the stall detector from firing, which would block
                    # the main pipeline forever.  30 min covers the worst-case scenario
                    # of stall_seconds(300) × max_retries(3) + retry delays.
                    _done, _ = await asyncio.wait({_next_task}, timeout=1800)
                    if _done:
                        try:
                            _prefetch_task_result = _next_task.result()
                        except (Exception, asyncio.CancelledError):
                            _prefetch_task_result = False
                    else:
                        _pfx_name = (
                            artifact.attachment.filename
                            if artifact.attachment
                            else str(artifact.id)
                        )
                        logger.error(
                            "Prefetch task for %s timed out after 1800s — cancelling",
                            _pfx_name,
                        )
                        _next_task.cancel()
                        await asyncio.wait({_next_task}, timeout=5.0)
                        # Reset artifact so recover_stuck_downloads picks it up next run
                        if artifact.status == DownloadStatus.DOWNLOADING:
                            artifact.status = DownloadStatus.PENDING
                            artifact.temp_path = None
                        session.commit()
                        _prefetch_task_result = False
                    needs_download = not _prefetch_task_result
                    # If prefetch succeeded, commit the in-memory status changes and
                    # transition the group from INCOMPLETE → READY so extraction
                    # can proceed. Without this commit the group query below would
                    # find no READY group and silently skip the archive.
                    if _prefetch_task_result:
                        ctx.files_downloaded += 1
                        _gid = _artifact_group_id(session, artifact.id)
                        await acquire_stage._update_group_statuses(
                            ctx, {_gid} if _gid else None
                        )
                        session.commit()
                else:
                    artifact = _next_pending_artifact(session)
                    if not artifact:
                        logger.info("No more pending downloads")
                        break
                    # Only skip if already downloaded (crash-resume case).
                    needs_download = artifact.status != DownloadStatus.COMPLETED

                filename = (
                    artifact.attachment.filename
                    if artifact.attachment and artifact.attachment.filename
                    else f"artifact_{artifact.id}"
                )
                logger.info("[%d] Processing: %s", processed + 1, filename)

                if display:
                    display.archive_start(filename)

                creds_before = ctx.credentials_parsed
                dups_before = ctx.duplicates_skipped

                try:
                    if needs_download:
                        if display:
                            display.stage_start("acquire")
                            total_size_mb = (
                                (artifact.attachment.size or 0) / 1024 / 1024
                                if artifact.attachment
                                else 0
                            )
                            display.download_start(filename, total_size_mb)

                        success = await acquire_stage._download_artifact(ctx, artifact)

                        if success:
                            ctx.files_downloaded += 1
                            _gid = _artifact_group_id(session, artifact.id)
                            await acquire_stage._update_group_statuses(
                                ctx, {_gid} if _gid else None
                            )
                            session.commit()

                        _record_stage_success(stages_completed, "acquire")

                        if display:
                            display.download_complete()

                        if not success:
                            logger.warning("Download failed for %s", filename)
                            session.commit()
                            processed += 1
                            continue

                    group = (
                        session.execute(
                            select(ArchiveGroup)
                            .join(ArchiveGroupPart)
                            .where(
                                ArchiveGroupPart.artifact_id == artifact.id,
                                ArchiveGroup.status == GroupStatus.READY,
                            )
                            .options(
                                joinedload(ArchiveGroup.parts).joinedload(ArchiveGroupPart.artifact)
                            )
                        )
                        .unique()
                        .scalar_one_or_none()
                    )

                    if group:
                        # The loaded group object is kept in memory (sessions use
                        # expire_on_commit=False); committing here prevents a
                        # long extraction from holding the SELECT transaction open.
                        session.commit()
                        # Fill prefetch queue up to _effective_prefetch slots while
                        # extract+parse+finalize runs. Each created task immediately
                        # starts (and sets artifact.status=DOWNLOADING) on the next
                        # asyncio yield, so _next_pending_artifact won't return it again.
                        if not dry_run and not _shutdown_checkpoint():
                            while (
                                len(_prefetch_queue) < _effective_prefetch
                                and processed + len(_prefetch_queue) + 1 < archives_to_process
                            ):
                                next_pending = _next_pending_artifact(session)
                                if not next_pending:
                                    break
                                task = asyncio.create_task(
                                    _prefetch_download(ctx, acquire_stage, next_pending)
                                )
                                _prefetch_queue.append((task, next_pending))
                                # Yield once so the task can start and mark artifact DOWNLOADING
                                # before the next _next_pending_artifact call.
                                await asyncio.sleep(0)
                                logger.debug(
                                    "Prefetch queued artifact %d (queue=%d)",
                                    next_pending.id,
                                    len(_prefetch_queue),
                                )

                        if display:
                            display.stage_start("extract")
                        logger.info("Extracting group: %s", group.base_name)
                        success = await extract_stage._extract_group(ctx, group, extractor)
                        if success:
                            ctx.archives_extracted += 1
                        _record_stage_success(stages_completed, "extract")
                        session.commit()

                        if display:
                            display.stage_start("parse")
                        await parse_stage.run(ctx)
                        _record_stage_success(stages_completed, "parse")
                        session.commit()

                        if display:
                            display.stage_start("finalize")
                        await finalize_stage.run(ctx)
                        _record_stage_success(stages_completed, "finalize")
                        session.commit()

                        # Opportunistically discover @mentions from just-parsed credentials and check/join one channel
                        try:
                            await channel_joiner.maybe_act(ctx)
                        except Exception as e:
                            logger.warning("Channel joiner error: %s", e)

                    processed += 1
                    new_creds = ctx.credentials_parsed - creds_before
                    new_dups = ctx.duplicates_skipped - dups_before
                    if display:
                        display.archive_complete(filename, new_creds, new_dups)
                    if _shutdown_checkpoint():
                        shutdown_after_current_archive = True
                    logger.info(
                        "Completed %d archives, %d credentials (%d dups) so far",
                        processed,
                        ctx.credentials_parsed,
                        ctx.duplicates_skipped,
                    )
                    # Release identity map after each archive to prevent unbounded
                    # memory growth over thousands of archives in a long pipeline run.
                    session.expire_all()

                except (Exception, asyncio.CancelledError) as e:
                    logger.error("Error processing %s: %s", filename, e)
                    ctx.errors.append(f"{filename}: {str(e)}")
                    current_stage = "acquire"
                    display_stage = getattr(display, "_current_stage", None) if display else None
                    if isinstance(display_stage, str) and display_stage:
                        current_stage = display_stage
                    _record_stage_failure(stages_failed, str(current_stage))
                    if display:
                        display.add_error()
                    try:
                        session.rollback()
                    except Exception:
                        pass
                    session.expire_all()
                    for _task, _ in _prefetch_queue:
                        if not _task.done():
                            _task.cancel()
                            await asyncio.wait({_task}, timeout=5.0)
                    _prefetch_queue.clear()

            await _drain_prefetch_queue(reset_pending=shutdown_after_current_archive)

            # Sweep for READY groups that weren't processed in the download
            # loop (e.g. groups from a previous run that are already READY).
            if not shutdown_after_current_archive:
                remaining_ready = (
                    session.execute(
                        select(ArchiveGroup.id)
                        .join(ArchiveGroupPart, ArchiveGroupPart.group_id == ArchiveGroup.id)
                        .join(DownloadArtifact, DownloadArtifact.id == ArchiveGroupPart.artifact_id)
                        .join(FileAttachment, FileAttachment.id == DownloadArtifact.attachment_id)
                        .where(ArchiveGroup.status == GroupStatus.READY)
                        .group_by(ArchiveGroup.id)
                        .order_by(
                            func.min(_download_priority(FileAttachment.filename)).asc(),
                            func.min(ArchiveGroup.updated_at).asc(),
                        )
                    )
                    .scalars()
                    .all()
                )
                session.commit()
                if remaining_ready:
                    logger.info(
                        "Post-download sweep: %d READY groups to extract", len(remaining_ready)
                    )
                    processed += await _process_ready_groups_batch(
                        ctx=ctx,
                        engine=engine,
                        group_ids=remaining_ready,
                        parallel_groups=_parallel_ready_groups,
                    )
                    if display:
                        display.update_creds(ctx.credentials_parsed)
                    session.expire_all()
                    if _shutdown_checkpoint():
                        shutdown_after_current_archive = True

            if shutdown_after_current_archive:
                _shutdown_req = _read_shutdown_request() or {}
                _update_display_shutdown(
                    display,
                    requested=True,
                    mode=_shutdown_req.get("mode"),
                    requested_at=_shutdown_req.get("requested_at"),
                    state="drained",
                )

            if channel_joiner and (
                channel_joiner.channels_checked or channel_joiner.channels_joined
            ):
                logger.info(
                    "Channel joiner: checked %d, joined %d channels during processing",
                    channel_joiner.channels_checked,
                    channel_joiner.channels_joined,
                )

            logger.info(
                "Sequential pipeline complete. Processed %d archives, %d credentials",
                processed,
                ctx.credentials_parsed,
            )

            return ctx
        finally:
            _finish_pipeline_run(
                session,
                run_record,
                ctx,
                stages_completed=stages_completed,
                stages_failed=stages_failed,
            )


def _start_pipeline_run(session: Session, *, mode: str, dry_run: bool) -> PipelineRun:
    """Create a persisted pipeline run record."""
    run = PipelineRun(
        mode=mode,
        status="running",
        dry_run=1 if dry_run else 0,
        started_at=datetime.now(UTC),
    )
    session.add(run)
    session.commit()
    return run


def _finish_pipeline_run(
    session: Session,
    run: PipelineRun,
    ctx: PipelineContext,
    *,
    stages_completed: list[str],
    stages_failed: list[str],
) -> None:
    """Persist final pipeline run counters and stage outcomes."""
    run_id = run.id
    try:
        session.rollback()
    except Exception:
        pass
    fresh_run = session.get(PipelineRun, run_id)
    if fresh_run is None:
        logger.warning("Pipeline run %s disappeared before finalization", run_id)
        return
    run = fresh_run

    finished_at = datetime.now(UTC)
    run.finished_at = finished_at
    started_at = run.started_at
    if started_at is not None and started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=UTC)
    if started_at is not None:
        run.duration_seconds = int((finished_at - started_at).total_seconds())
    else:
        run.duration_seconds = None
    run.status = "failed" if ctx.errors or stages_failed else "completed"
    run.stages_completed_json = json.dumps(stages_completed)
    run.stages_failed_json = json.dumps(stages_failed)
    run.errors_json = json.dumps([e[:8192] for e in ctx.errors])
    run.conversations_processed = ctx.conversations_processed
    run.messages_processed = ctx.messages_processed
    run.files_discovered = ctx.files_discovered
    run.files_downloaded = ctx.files_downloaded
    run.archives_extracted = ctx.archives_extracted
    run.credentials_parsed = ctx.credentials_parsed
    run.duplicates_skipped = ctx.duplicates_skipped
    session.commit()


def _record_stage_success(stages_completed: list[str], stage_name: str) -> None:
    """Record a successful stage once."""
    if stage_name not in stages_completed:
        stages_completed.append(stage_name)


def _record_stage_failure(stages_failed: list[str], stage_name: str) -> None:
    """Record a failed stage once."""
    if stage_name not in stages_failed:
        stages_failed.append(stage_name)
