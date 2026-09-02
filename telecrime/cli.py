"""CLI interface for Telecrime."""

import threading
from datetime import UTC, datetime
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from telecrime import __version__
from telecrime.config import load_config, save_config
from telecrime.database import ensure_runtime_schema, get_engine, get_session, init_db
from telecrime.logging_utils import configure_logging
from telecrime.utils.credential_dedup import soft_dedupe_credentials

app = typer.Typer(
    name="telecrime",
    help="Telecrime - pipeline for processing stealer logs from Telegram channels",
    no_args_is_help=True,
)
console = Console()


def get_config_and_engine(config_path: Path | None = None):
    """Load config and create database engine."""
    config = load_config(config_path)
    config.ensure_directories()
    engine = get_engine(config.database_url)
    return config, engine


@app.callback()
def main(
    version: bool = typer.Option(False, "--version", "-v", help="Show version and exit"),
) -> None:
    """Telecrime - pipeline for processing stealer logs from Telegram channels."""
    configure_logging()
    if version:
        console.print(f"telecrime version {__version__}")
        raise typer.Exit()


@app.command()
def init(
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
) -> None:
    """Initialize the database and create config file."""
    config, engine = get_config_and_engine(config_path)
    from telecrime.fts import ensure_fts

    # Create tables
    init_db(engine)
    schema_changes = ensure_runtime_schema(engine)
    ensure_fts(engine)
    console.print("[green]Database initialized[/green]")
    for change in schema_changes:
        console.print(f"[dim]{change}[/dim]")

    # Save default config if it doesn't exist
    if config_path is None:
        from telecrime.config import get_default_config_path

        config_path = get_default_config_path()

    if not config_path.exists():
        save_config(config, config_path)
        console.print(f"[green]Config file created:[/green] {config_path}")
    else:
        console.print(f"[yellow]Config file exists:[/yellow] {config_path}")


@app.command("telegram-auth-aux")
def telegram_auth_aux(
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
) -> None:
    """Authenticate the auxiliary Telegram session interactively.

    Run once after setting TELECRIME_TELEGRAM_AUX_SESSION_NAME. Telethon will
    prompt for the verification code on the configured phone number and
    persist a new <aux_session_name>.session file in the data directory.
    """
    import asyncio

    config, _ = get_config_and_engine(config_path)
    if not config.telegram.aux_session_name:
        console.print(
            "[red]TELECRIME_TELEGRAM_AUX_SESSION_NAME is not set.[/red] "
            "Set it (e.g. 'telecrime_aux') in .env or your config and re-run."
        )
        raise typer.Exit(1)

    aux_config = config.with_aux_telegram_session()
    session_path = aux_config.data_dir / f"{aux_config.telegram.session_name}.session"
    console.print(
        f"Authenticating aux session [cyan]{aux_config.telegram.session_name}[/cyan] "
        f"(file: {session_path})"
    )

    from telecrime.adapters.telegram import TelegramAdapter

    async def _run() -> None:
        adapter = TelegramAdapter(aux_config)
        try:
            await adapter.connect(timeout=300)
            assert adapter.client is not None
            me = await adapter.client.get_me()
            console.print(
                f"[green]Aux session ready[/green] as {me.first_name or ''} "
                f"(id={me.id})"
            )
        finally:
            await adapter.disconnect()

    asyncio.run(_run())


@app.command("telegram-auth-download")
def telegram_auth_download(
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
) -> None:
    """Authenticate all configured parallel download sessions interactively.

    Run once per session file. TELECRIME_DOWNLOAD_SESSIONS must be set to a
    comma-separated list of session names (e.g. 'download2,download3'). Each
    session needs its own Telegram account/phone number. Telethon will prompt
    for each verification code and persist a <name>.session file in the data
    directory.
    """
    import asyncio

    config, _ = get_config_and_engine(config_path)
    names = config.telegram.download_session_names
    if not names:
        console.print(
            "[red]TELECRIME_DOWNLOAD_SESSIONS is not set.[/red] "
            "Set it (e.g. 'download2,download3') in .env or your config and re-run."
        )
        raise typer.Exit(1)

    from telecrime.adapters.telegram import TelegramAdapter

    async def _run() -> None:
        for i, name in enumerate(names, start=1):
            session_config = config.with_download_session(i)
            session_path = (
                session_config.data_dir / f"{session_config.telegram.session_name}.session"
            )
            console.print(
                f"Authenticating download session [cyan]{name}[/cyan] "
                f"(file: {session_path})"
            )
            adapter = TelegramAdapter(session_config)
            try:
                await adapter.connect(timeout=300)
                assert adapter.client is not None
                me = await adapter.client.get_me()
                console.print(
                    f"[green]Download session {name} ready[/green] as "
                    f"{me.first_name or ''} (id={me.id})"
                )
            finally:
                await adapter.disconnect()

    asyncio.run(_run())


@app.command()
def repair(
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
    schema: bool = typer.Option(True, "--schema/--no-schema", help="Apply additive schema repairs"),
    runtime: bool = typer.Option(True, "--runtime/--no-runtime", help="Clear stale pipeline runtime state"),
) -> None:
    """Repair local runtime/schema drift without deleting data."""
    config, engine = get_config_and_engine(config_path)

    if schema:
        changes = ensure_runtime_schema(engine)
        if changes:
            console.print("[green]Schema repaired:[/green]")
            for change in changes:
                console.print(f"  - {change}")
        else:
            console.print("[green]Schema already compatible[/green]")

    if runtime:
        from telecrime.scheduler import repair_stale_runtime_state

        result = repair_stale_runtime_state(config, engine)
        console.print(f"[green]Runtime repair:[/green] {result}")


@app.command("recover-download-backlog")
def recover_download_backlog(
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be repaired without changing DB state"),
    limit: int | None = typer.Option(None, "--limit", help="Maximum items per repair category"),
) -> None:
    """Requeue already-downloaded files that were not parsed yet.

    This repairs two safe backlog classes:
    1. Archive groups marked CLEANED but with no extraction jobs and existing completed downloads.
    2. Completed standalone credential .txt downloads that were never assigned to an archive group.
    """
    import hashlib

    from sqlalchemy import exists, select

    from telecrime.models import (
        ArchiveGroup,
        ArchiveGroupPart,
        DownloadArtifact,
        ExtractionJob,
        FileAttachment,
    )
    from telecrime.states import DownloadStatus, GroupStatus
    from telecrime.stealer.patterns import is_credential_file

    _config, engine = get_config_and_engine(config_path)
    reset_groups = 0
    txt_groups = 0
    missing_paths = 0

    def _exists(path: str | None) -> bool:
        if not path:
            return False
        return Path(path).exists()

    with get_session(engine) as session:
        stale_group_query = (
            select(ArchiveGroup)
            .where(
                ArchiveGroup.status == GroupStatus.CLEANED,
                ~exists().where(ExtractionJob.group_id == ArchiveGroup.id),
                exists()
                .where(ArchiveGroupPart.group_id == ArchiveGroup.id)
                .where(ArchiveGroupPart.artifact_id == DownloadArtifact.id)
                .where(DownloadArtifact.status == DownloadStatus.COMPLETED)
                .where(DownloadArtifact.is_deleted.is_(False)),
            )
            .order_by(ArchiveGroup.updated_at.desc())
        )
        if limit:
            stale_group_query = stale_group_query.limit(limit)

        for group in session.execute(stale_group_query).scalars().all():
            artifacts = [part.artifact for part in group.parts if part.artifact]
            if not any(
                artifact.status == DownloadStatus.COMPLETED
                and not artifact.is_deleted
                and _exists(artifact.local_path)
                for artifact in artifacts
            ):
                missing_paths += 1
                continue

            reset_groups += 1
            console.print(f"[cyan]requeue archive group[/cyan] {group.id}: {group.base_name}")
            if not dry_run:
                group.status = GroupStatus.READY
                note = "Requeued by recover-download-backlog: CLEANED without extraction job"
                group.notes = f"{group.notes or ''}\n{note}".strip()

        ungrouped_txt_query = (
            select(DownloadArtifact)
            .join(FileAttachment, FileAttachment.id == DownloadArtifact.attachment_id)
            .outerjoin(ArchiveGroupPart, ArchiveGroupPart.artifact_id == DownloadArtifact.id)
            .where(
                DownloadArtifact.status == DownloadStatus.COMPLETED,
                DownloadArtifact.is_deleted.is_(False),
                ArchiveGroupPart.id.is_(None),
                FileAttachment.filename.isnot(None),
            )
            .order_by(DownloadArtifact.updated_at.desc())
        )
        if limit:
            ungrouped_txt_query = ungrouped_txt_query.limit(limit)

        for artifact in session.execute(ungrouped_txt_query).scalars().all():
            attachment = artifact.attachment
            filename = attachment.filename if attachment else None
            if not filename or not filename.lower().endswith(".txt") or not is_credential_file(filename):
                continue
            if not _exists(artifact.local_path):
                missing_paths += 1
                continue

            txt_groups += 1
            console.print(f"[cyan]group direct txt[/cyan] artifact {artifact.id}: {filename}")
            if dry_run:
                continue

            attachment.is_archive_candidate = True
            attachment.archive_type = "txt"
            fingerprint_src = f"direct-txt:{artifact.id}:{artifact.content_hash or artifact.local_path}"
            group = ArchiveGroup(
                fingerprint=hashlib.sha256(fingerprint_src.encode()).hexdigest(),
                base_name=filename,
                expected_part_count=1,
                detected_part_count=1,
                status=GroupStatus.READY,
                notes="Created by recover-download-backlog for standalone credential txt",
            )
            session.add(group)
            session.flush()
            session.add(
                ArchiveGroupPart(
                    group_id=group.id,
                    artifact_id=artifact.id,
                    part_index=0,
                    role="main",
                )
            )

        if dry_run:
            session.rollback()
        else:
            session.commit()

    action = "would repair" if dry_run else "repaired"
    console.print(
        f"[green]{action}[/green] {reset_groups} stale archive groups, "
        f"{txt_groups} direct txt downloads; skipped {missing_paths} missing paths"
    )


@app.command()
def dashboard(
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
    host: str = typer.Option("127.0.0.1", "--host", help="Bind host"),
    port: int = typer.Option(8000, "--port", help="Bind port"),
    reload: bool = typer.Option(False, "--reload", help="Auto-reload on code changes"),
) -> None:
    """Run the web dashboard (local-only by default)."""
    import os

    import uvicorn

    config, _engine = get_config_and_engine(config_path)

    # Keep web-side runtime caches aligned with the same configured data root.
    os.environ["TELECRIME_DATA_DIR"] = str(config.data_dir)
    from telecrime.web.app import create_app

    app_instance = create_app(config.database_url)
    console.print(f"[green]Dashboard running:[/green] http://{host}:{port}")
    uvicorn.run(app_instance, host=host, port=port, reload=reload)


fts_app = typer.Typer(name="fts", help="Manage full-text search index", no_args_is_help=True)
app.add_typer(fts_app)


@fts_app.command("rebuild")
def rebuild_fts(
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
) -> None:
    """Create or rebuild the credential FTS index."""
    from telecrime.fts import ensure_fts

    _config, engine = get_config_and_engine(config_path)
    if not ensure_fts(engine, rebuild=True):
        console.print("[red]Failed to rebuild FTS index[/red]")
        raise typer.Exit(1)

    console.print("[green]FTS index ready[/green]")


@app.command()
def run(
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
    dry_run: bool = typer.Option(False, "--dry-run", "-n", help="Don't actually download/extract"),
    sequential: bool = typer.Option(
        True,
        "--sequential/--batch",
        "-s/-b",
        help="Process one archive at a time (default) vs batch mode",
    ),
    limit: int | None = typer.Option(None, "--limit", "-l", help="Max archives to process"),
    prefetch: int = typer.Option(2, "--prefetch", help="Archives to pre-download concurrently (1-3, default 2)"),
) -> None:
    """Run the main pipeline.

    Default mode (--sequential): Download, extract, parse each archive before moving to next.
    Batch mode (--batch): Download all, then extract all, then parse all.
    """
    import asyncio

    from telecrime.pipeline.lock import PipelineAlreadyRunningError

    config, engine = get_config_and_engine(config_path)

    # Check Telegram credentials
    if not config.telegram.api_id or not config.telegram.api_hash:
        console.print("[red]Telegram credentials not configured![/red]")
        console.print(
            "Set TELECRIME_TELEGRAM_API_ID and TELECRIME_TELEGRAM_API_HASH environment variables"
        )
        console.print("Or configure them in the config file")
        raise typer.Exit(1)

    console.print(f"Database: {config.database_url}")
    console.print(f"Target extensions: {config.extraction.target_extensions}")
    console.print(f"Mode: {'sequential (one at a time)' if sequential else 'batch'}")
    if limit:
        console.print(f"Limit: {limit} archives")

    if dry_run:
        console.print("[yellow]DRY RUN MODE - no actual downloads or extractions[/yellow]")

    async def run_async():
        from telecrime.adapters.telegram import TelegramAdapter
        from telecrime.database import get_session
        from telecrime.notify import TelegramNotifier
        from telecrime.pipeline import create_default_pipeline
        from telecrime.pipeline.display import PipelineDisplay
        from telecrime.pipeline.lock import PipelineAlreadyRunningError, pipeline_run_lock
        from telecrime.pipeline.orchestrator import run_sequential_pipeline
        from telecrime.pipeline.progress import PipelineProgressWriter

        class _FanoutDisplay:
            def __init__(self, *targets):
                self._targets = targets

            def __getattr__(self, name):
                def _call(*args, **kwargs):
                    result = None
                    for target in self._targets:
                        attr = getattr(target, name, None)
                        if callable(attr):
                            result = attr(*args, **kwargs)
                    return result

                return _call

        adapter = TelegramAdapter(config)
        console_display = PipelineDisplay(console=console)

        # Parallel download sessions: one extra adapter per configured session
        # name. Each session (separate Telegram account) has its own download
        # speed budget, so N sessions multiply download throughput.
        download_adapters: list[TelegramAdapter] = []
        for i in range(1, len(config.telegram.download_session_names) + 1):
            download_adapters.append(
                TelegramAdapter(config.with_download_session(i))
            )

        try:
            with pipeline_run_lock(config.data_dir):
                # Construct PipelineProgressWriter inside the lock so its __init__
                # write doesn't clobber a running pipeline's progress file when this
                # process discovers the lock is already held.
                progress_display = PipelineProgressWriter()
                display = _FanoutDisplay(console_display, progress_display)
                console.print("Connecting to Telegram...")
                await adapter.connect()

                # Connect extra download sessions in parallel.
                for dl_adapter in download_adapters:
                    console.print(
                        f"Connecting download session {dl_adapter.config.telegram.session_name}..."
                    )
                    await dl_adapter.connect()
                if download_adapters:
                    console.print(
                        f"[green]Download pool: {1 + len(download_adapters)} sessions[/green]"
                    )

                # Create notifier for progress updates to Saved Messages
                notifier = TelegramNotifier(adapter.client, enabled=True)
                with get_session(engine) as session:
                    # Disable idle-in-transaction timeout for the pipeline session.
                    # The pipeline commits before each long network/I/O operation,
                    # so the session should never be idle-in-transaction for long, but
                    # this guard prevents a missed commit from killing the connection.
                    try:
                        from sqlalchemy import text as _text
                        session.execute(_text("SET idle_in_transaction_session_timeout = 0"))
                        session.execute(_text("SET lock_timeout = 0"))
                    except Exception:
                        pass
                    ctx = None
                    display.start()
                    try:
                        if sequential:
                            ctx = await run_sequential_pipeline(
                                config,
                                session,
                                adapter,
                                dry_run=dry_run,
                                notifier=notifier,
                                limit=limit,
                                display=display,
                                prefetch_count=prefetch,
                                download_adapters=download_adapters,
                            )
                        else:
                            pipeline = create_default_pipeline(config, session, adapter)
                            ctx = await pipeline.run(
                                dry_run=dry_run,
                                notifier=notifier,
                                display=display,
                            )
                    finally:
                        display.stop()
                        progress_display.finish()

                    if ctx is not None:
                        display.print_summary(ctx)

        except PipelineAlreadyRunningError as e:
            console.print(f"[yellow]{e}[/yellow]")
            raise typer.Exit(75) from e  # EX_TEMPFAIL — not an error, just busy

        finally:
            try:
                await adapter.disconnect()
            except Exception:
                pass
            for dl_adapter in download_adapters:
                try:
                    await dl_adapter.disconnect()
                except Exception:
                    pass
            # Drain any background tasks Telethon left behind (e.g.
            # Connection._send_loop, queue waiters from auto_reconnect=True).
            # Without this, asyncio.run() closes the loop while these are
            # still pending, producing "RuntimeError: Event loop is closed"
            # during interpreter shutdown and Python exits with code 1 —
            # which the scheduler then logs as "pipeline failed" even
            # though all work completed successfully.
            pending = [
                t for t in asyncio.all_tasks()
                if t is not asyncio.current_task()
            ]
            for t in pending:
                t.cancel()
            if pending:
                try:
                    await asyncio.wait(pending, timeout=2)
                except Exception:
                    pass

    coro = run_async()
    try:
        asyncio.run(coro)
    except PipelineAlreadyRunningError as e:
        console.print(f"[yellow]{e}[/yellow]")
        raise typer.Exit(75) from e  # EX_TEMPFAIL — not an error, just busy
    finally:
        # Ensure coroutine is closed if asyncio.run is mocked in tests.
        try:
            coro.close()
        except Exception:
            pass


@app.command()
def status(
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
) -> None:
    """Show pipeline status and statistics."""
    _, engine = get_config_and_engine(config_path)

    from telecrime.models import (
        ArchiveGroup,
        Conversation,
        DownloadArtifact,
        ExtractedOutput,
        ExtractionJob,
        FileAttachment,
        Message,
    )

    with get_session(engine) as session:
        table = Table(title="Telecrime Status")
        table.add_column("Entity", style="cyan")
        table.add_column("Count", justify="right", style="green")

        table.add_row("Conversations", str(session.query(Conversation).count()))
        table.add_row("Messages", str(session.query(Message).count()))
        table.add_row("File Attachments", str(session.query(FileAttachment).count()))
        table.add_row(
            "Archive Candidates",
            str(
                session.query(FileAttachment)
                .filter(FileAttachment.is_archive_candidate == True)
                .count()
            ),
        )
        table.add_row("Download Artifacts", str(session.query(DownloadArtifact).count()))
        table.add_row("Archive Groups", str(session.query(ArchiveGroup).count()))
        table.add_row("Extraction Jobs", str(session.query(ExtractionJob).count()))
        table.add_row("Extracted Outputs", str(session.query(ExtractedOutput).count()))

        console.print(table)


@app.command()
def diagnostics(
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
) -> None:
    """Show pipeline health, backlog, and failure hotspots."""
    import json

    from sqlalchemy import func

    from telecrime.models import (
        ArchiveGroup,
        Conversation,
        DownloadArtifact,
        ExtractionJob,
        FileAttachment,
        Message,
        ParsedCredential,
        PipelineRun,
    )
    from telecrime.states import DownloadStatus, ExtractionStatus, GroupStatus

    _config, engine = get_config_and_engine(config_path)

    with get_session(engine) as session:
        backlog = Table(title="Pipeline Diagnostics")
        backlog.add_column("Metric", style="cyan")
        backlog.add_column("Value", justify="right", style="green")

        archive_candidates = (
            session.query(FileAttachment)
            .filter(FileAttachment.is_archive_candidate == True)
            .count()
        )
        backlog.add_row("Archive candidates", f"{archive_candidates:,}")
        backlog.add_row(
            "Pending downloads",
            f"{session.query(DownloadArtifact).filter(DownloadArtifact.status == DownloadStatus.PENDING).count():,}",
        )
        backlog.add_row(
            "Ready groups",
            f"{session.query(ArchiveGroup).filter(ArchiveGroup.status == GroupStatus.READY).count():,}",
        )
        backlog.add_row(
            "Groups extracting",
            f"{session.query(ArchiveGroup).filter(ArchiveGroup.status == GroupStatus.EXTRACTING).count():,}",
        )
        backlog.add_row(
            "Groups extracted",
            f"{session.query(ArchiveGroup).filter(ArchiveGroup.status == GroupStatus.EXTRACTED).count():,}",
        )
        backlog.add_row(
            "Pending extraction jobs",
            f"{session.query(ExtractionJob).filter(ExtractionJob.status == ExtractionStatus.PENDING).count():,}",
        )
        backlog.add_row("Parsed credentials", f"{session.query(ParsedCredential).count():,}")
        console.print(backlog)

        failure_table = Table(title="Failure Summary")
        failure_table.add_column("Type", style="cyan")
        failure_table.add_column("Count", justify="right", style="yellow")

        _dl_failed = session.query(DownloadArtifact).filter(
            DownloadArtifact.status.in_([DownloadStatus.FAILED, DownloadStatus.FAILED_TERMINAL])
        ).count()
        failure_table.add_row("Download failures", f"{_dl_failed:,}")
        _ex_failed = session.query(ExtractionJob).filter(
            ExtractionJob.status.in_([
                ExtractionStatus.FAILED,
                ExtractionStatus.FAILED_TERMINAL,
                ExtractionStatus.PASSWORD_NEEDED,
            ])
        ).count()
        failure_table.add_row("Extraction failures", f"{_ex_failed:,}")
        console.print(failure_table)

        top_extract_errors = (
            session.query(
                ExtractionJob.last_error_code,
                func.count(ExtractionJob.id).label("count"),
            )
            .filter(ExtractionJob.last_error_code.isnot(None))
            .group_by(ExtractionJob.last_error_code)
            .order_by(func.count(ExtractionJob.id).desc())
            .limit(10)
            .all()
        )
        if top_extract_errors:
            error_table = Table(title="Top Extraction Errors")
            error_table.add_column("Error code", style="cyan")
            error_table.add_column("Count", justify="right", style="yellow")
            for error_code, count in top_extract_errors:
                error_table.add_row(str(error_code), f"{count:,}")
            console.print(error_table)

        recent_runs = (
            session.query(PipelineRun).order_by(PipelineRun.started_at.desc()).limit(10).all()
        )
        if recent_runs:
            runs_table = Table(title="Recent Pipeline Runs")
            runs_table.add_column("ID", style="cyan")
            runs_table.add_column("Mode")
            runs_table.add_column("Status")
            runs_table.add_column("Started")
            runs_table.add_column("Duration", justify="right")
            runs_table.add_column("Creds", justify="right")
            runs_table.add_column("Errors", justify="right")
            for run in recent_runs:
                error_count = len(json.loads(run.errors_json or "[]"))
                runs_table.add_row(
                    str(run.id),
                    run.mode,
                    run.status,
                    str(run.started_at),
                    str(run.duration_seconds or 0),
                    f"{run.credentials_parsed:,}",
                    str(error_count),
                )
            console.print(runs_table)

        channel_rows = (
            session.query(
                Conversation.title,
                Conversation.username,
                func.count(FileAttachment.id).label("archives"),
                func.max(Conversation.last_ingested_at).label("last_ingested_at"),
            )
            .join(Message, Message.conversation_id == Conversation.id)
            .join(FileAttachment, FileAttachment.message_id == Message.id)
            .filter(FileAttachment.is_archive_candidate == True)
            .group_by(Conversation.id)
            .order_by(func.count(FileAttachment.id).desc())
            .limit(10)
            .all()
        )
        if channel_rows:
            channel_table = Table(title="Top Channel Backlog / Throughput")
            channel_table.add_column("Channel", style="cyan")
            channel_table.add_column("Archives", justify="right")
            channel_table.add_column("Last Ingested")
            for row in channel_rows:
                name = row.username or row.title or "(unknown)"
                channel_table.add_row(name, f"{row.archives:,}", str(row.last_ingested_at or "-"))
            console.print(channel_table)


@app.command()
def retry(
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
    job_id: int | None = typer.Option(None, "--job", "-j", help="Specific job ID to retry"),
    downloads: bool = typer.Option(False, "--downloads", "-d", help="Retry failed downloads"),
    extractions: bool = typer.Option(False, "--extractions", "-e", help="Retry failed extractions"),
    terminal: bool = typer.Option(False, "--terminal", help="Include terminal failures too"),
) -> None:
    """Retry failed jobs."""
    from telecrime.models import DownloadArtifact, ExtractionJob
    from telecrime.states import DownloadStatus, ExtractionStatus, GroupStatus

    _, engine = get_config_and_engine(config_path)

    with get_session(engine) as session:
        reset_count = 0

        if downloads or (not downloads and not extractions):
            # Reset failed downloads to pending
            statuses = [DownloadStatus.FAILED]
            if terminal:
                statuses.append(DownloadStatus.FAILED_TERMINAL)
            failed_downloads = session.query(DownloadArtifact).filter(
                DownloadArtifact.status.in_(statuses)
            )

            if job_id:
                failed_downloads = failed_downloads.filter(DownloadArtifact.id == job_id)

            for artifact in failed_downloads.all():
                artifact.status = DownloadStatus.PENDING
                artifact.error_message = None
                reset_count += 1

        if extractions or (not downloads and not extractions):
            # Reset failed extractions
            extraction_statuses = [ExtractionStatus.FAILED, ExtractionStatus.PASSWORD_NEEDED]
            if terminal:
                extraction_statuses.append(ExtractionStatus.FAILED_TERMINAL)
            failed_jobs = session.query(ExtractionJob).filter(
                ExtractionJob.status.in_(extraction_statuses)
            )

            if job_id:
                failed_jobs = failed_jobs.filter(ExtractionJob.id == job_id)

            for job in failed_jobs.all():
                job.status = ExtractionStatus.PENDING
                job.last_error_code = None
                job.last_error_message = None

                # Reset group status too
                if job.group:
                    job.group.status = GroupStatus.READY

                reset_count += 1

        session.commit()
        console.print(f"[green]Reset {reset_count} jobs for retry[/green]")


@app.command()
def failures(
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
    downloads: bool = typer.Option(False, "--downloads", "-d", help="Show failed downloads"),
    extractions: bool = typer.Option(False, "--extractions", "-e", help="Show failed extractions"),
    limit: int = typer.Option(20, "--limit", "-l", help="Rows per section"),
) -> None:
    """Show failed download and extraction jobs with error context."""
    from telecrime.models import DownloadArtifact, ExtractionJob
    from telecrime.states import DownloadStatus, ExtractionStatus

    _config, engine = get_config_and_engine(config_path)
    show_downloads = downloads or (not downloads and not extractions)
    show_extractions = extractions or (not downloads and not extractions)

    with get_session(engine) as session:
        if show_downloads:
            table = Table(title="Failed Downloads")
            table.add_column("ID", style="cyan")
            table.add_column("Status", style="yellow")
            table.add_column("File")
            table.add_column("Error")
            rows = (
                session.query(DownloadArtifact)
                .filter(
                    DownloadArtifact.status.in_(
                        [DownloadStatus.FAILED, DownloadStatus.FAILED_TERMINAL]
                    )
                )
                .order_by(DownloadArtifact.id.desc())
                .limit(limit)
                .all()
            )
            for artifact in rows:
                filename = None
                if artifact.attachment:
                    filename = artifact.attachment.filename or artifact.attachment.platform_file_id
                table.add_row(
                    str(artifact.id),
                    str(artifact.status.value),
                    filename or "(unknown)",
                    (artifact.error_message or "")[:120],
                )
            console.print(table)
            if not rows:
                console.print("[green]No failed downloads[/green]")

        if show_extractions:
            table = Table(title="Failed Extractions")
            table.add_column("ID", style="cyan")
            table.add_column("Status", style="yellow")
            table.add_column("Archive")
            table.add_column("Error Code")
            table.add_column("Error")
            extraction_rows = (
                session.query(ExtractionJob)
                .filter(
                    ExtractionJob.status.in_(
                        [
                            ExtractionStatus.FAILED,
                            ExtractionStatus.FAILED_TERMINAL,
                            ExtractionStatus.PASSWORD_NEEDED,
                        ]
                    )
                )
                .order_by(ExtractionJob.id.desc())
                .limit(limit)
                .all()
            )
            for job in extraction_rows:
                archive_name = (
                    job.group.base_name
                    if job.group and job.group.base_name
                    else f"group_{job.group_id}"
                )
                table.add_row(
                    str(job.id),
                    str(job.status.value),
                    archive_name,
                    job.last_error_code or "",
                    (job.last_error_message or "")[:120],
                )
            console.print(table)
            if not extraction_rows:
                console.print("[green]No failed extractions[/green]")


@app.command()
def reprocess(
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
    group_id: int | None = typer.Option(None, "--group-id", help="Target archive group ID"),
    archive_name: str | None = typer.Option(
        None, "--archive-name", help="Target archive base name"
    ),
    stage: str = typer.Option("parse", "--stage", help="Stage to reset: parse or extract"),
) -> None:
    """Reset a specific archive group back to a pipeline stage."""
    from telecrime.models import ArchiveGroup, ExtractedOutput, ParsedCredential
    from telecrime.states import ExtractionStatus, GroupStatus

    if not group_id and not archive_name:
        console.print("[red]Specify --group-id or --archive-name[/red]")
        raise typer.Exit(1)

    if stage not in {"parse", "extract"}:
        console.print("[red]Stage must be 'parse' or 'extract'[/red]")
        raise typer.Exit(1)

    _config, engine = get_config_and_engine(config_path)
    with get_session(engine) as session:
        query = session.query(ArchiveGroup)
        if group_id is not None:
            query = query.filter(ArchiveGroup.id == group_id)
        if archive_name:
            query = query.filter(ArchiveGroup.base_name == archive_name)

        groups = query.all()
        if not groups:
            console.print("[yellow]No matching archive groups found[/yellow]")
            raise typer.Exit(1)

        reset_groups = 0
        deleted_credentials = 0
        deleted_outputs = 0

        for group in groups:
            jobs = list(group.extraction_jobs)
            if stage == "parse":
                for job in jobs:
                    deleted_credentials += (
                        session.query(ParsedCredential)
                        .filter(ParsedCredential.extraction_job_id == job.id)
                        .delete(synchronize_session=False)
                    )
                group.status = GroupStatus.EXTRACTED
            else:
                for job in jobs:
                    deleted_credentials += (
                        session.query(ParsedCredential)
                        .filter(ParsedCredential.extraction_job_id == job.id)
                        .delete(synchronize_session=False)
                    )
                    deleted_outputs += (
                        session.query(ExtractedOutput)
                        .filter(ExtractedOutput.job_id == job.id)
                        .delete(synchronize_session=False)
                    )
                    job.status = ExtractionStatus.PENDING
                    job.last_error_code = None
                    job.last_error_message = None
                    job.used_password_id = None
                    job.password_attempts = 0
                group.status = GroupStatus.READY

            reset_groups += 1

        session.commit()
        console.print(f"[green]Reset {reset_groups} group(s) to {stage} stage[/green]")
        if deleted_credentials:
            console.print(f"Removed {deleted_credentials} parsed credential rows")
        if deleted_outputs:
            console.print(f"Removed {deleted_outputs} extracted output rows")


@app.command()
def clean(
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
    downloads: bool = typer.Option(False, "--downloads", "-d", help="Clean downloaded archives"),
    force: bool = typer.Option(False, "--force", "-f", help="Don't ask for confirmation"),
) -> None:
    """Clean up downloaded files."""
    config, _ = get_config_and_engine(config_path)

    if not downloads:
        console.print("[yellow]Specify --downloads to clean[/yellow]")
        raise typer.Exit(1)

    if not force:
        console.print(f"Will delete: {config.downloads_dir}")
        if not typer.confirm("Continue?"):
            raise typer.Exit()

    if downloads and config.downloads_dir.exists():
        import shutil

        shutil.rmtree(config.downloads_dir)
        config.downloads_dir.mkdir(parents=True, exist_ok=True)
        console.print("[green]Downloads cleaned[/green]")


@app.command()
def creds(
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
) -> None:
    """Show credential statistics."""
    from sqlalchemy import func

    from telecrime.models import ParsedCredential

    _, engine = get_config_and_engine(config_path)

    with get_session(engine) as session:
        total = session.query(ParsedCredential).count()

        if total == 0:
            console.print("[yellow]No credentials in database[/yellow]")
            console.print("Run the pipeline or use process_folder.py to extract credentials")
            raise typer.Exit()

        unique_domains = session.query(func.count(func.distinct(ParsedCredential.domain))).scalar()
        unique_users = session.query(func.count(func.distinct(ParsedCredential.username))).scalar()

        table = Table(title="Credential Statistics")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", justify="right", style="green")

        table.add_row("Total Credentials", f"{total:,}")
        table.add_row("Unique Domains", f"{unique_domains:,}")
        table.add_row("Unique Usernames", f"{unique_users:,}")

        console.print(table)

        # Top domains
        console.print("\n[bold]Top 15 Domains:[/bold]")
        top_domains = (
            session.query(ParsedCredential.domain, func.count(ParsedCredential.id).label("count"))
            .group_by(ParsedCredential.domain)
            .order_by(func.count(ParsedCredential.id).desc())
            .limit(15)
            .all()
        )

        domain_table = Table()
        domain_table.add_column("Domain", style="cyan")
        domain_table.add_column("Count", justify="right", style="green")

        for domain, count in top_domains:
            domain_table.add_row(domain or "(empty)", f"{count:,}")

        console.print(domain_table)

        # Stealer types
        console.print("\n[bold]Stealer Types:[/bold]")
        types = (
            session.query(
                ParsedCredential.stealer_type, func.count(ParsedCredential.id).label("count")
            )
            .group_by(ParsedCredential.stealer_type)
            .all()
        )

        for stype, count in types:
            console.print(f"  {stype or 'unknown'}: {count:,}")


@app.command()
def search(
    query: str = typer.Argument(..., help="Search query (domain, username, or URL pattern)"),
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
    domain: bool = typer.Option(False, "--domain", "-d", help="Search in domain field"),
    username: bool = typer.Option(False, "--user", "-u", help="Search in username field"),
    url: bool = typer.Option(False, "--url", help="Search in URL field"),
    limit: int = typer.Option(50, "--limit", "-n", help="Max results to show"),
    show_password: bool = typer.Option(False, "--show-pass", "-p", help="Show full passwords"),
    stealer: str | None = typer.Option(
        None, "--stealer", "-s", help="Filter by stealer type (exact match)"
    ),
    app: str | None = typer.Option(None, "--app", "-a", help="Filter by application (ILIKE)"),
    email_domain: str | None = typer.Option(
        None, "--email-domain", "-e", help="Filter by email domain"
    ),
    source: str | None = typer.Option(None, "--source", help="Filter by source archive pattern"),
) -> None:
    """Search credentials by domain, username, or URL.

    Uses FTS5 full-text search when available (falls back to ILIKE if FTS unavailable).
    The text query searches domain/user/url with OR logic.
    Additional filters (--stealer, --app, etc.) use AND logic.
    """
    from telecrime.fts import fts_available, fts_count, fts_search
    from telecrime.models import ParsedCredential

    _, engine = get_config_and_engine(config_path)

    # Default to searching all fields if none specified
    if not domain and not username and not url:
        domain = username = url = True

    has_fts = fts_available(engine)

    filter_values = {
        "stealer": stealer,
        "app": app,
        "email_domain": email_domain,
        "source": source,
    }
    fts_filters = {key: value for key, value in filter_values.items() if value is not None}

    with get_session(engine) as session:
        # Determine which columns to search
        fts_columns = []
        if domain:
            fts_columns.append("domain")
        if username:
            fts_columns.append("username")
        if url:
            fts_columns.append("url")

        results = None
        total = 0

        if has_fts:
            try:
                ids = fts_search(
                    session,
                    query,
                    columns=fts_columns,
                    limit=limit,
                    filters=fts_filters,
                )
                if ids:
                    total = fts_count(session, query, columns=fts_columns, filters=fts_filters)
                    rows = (
                        session.query(ParsedCredential).filter(ParsedCredential.id.in_(ids)).all()
                    )
                    ordered = {cred_id: idx for idx, cred_id in enumerate(ids)}
                    results = soft_dedupe_credentials(
                        sorted(rows, key=lambda cred: ordered[cred.id])
                    )[:limit]
                else:
                    total = 0
                    results = []
            except Exception:
                has_fts = False  # fall through to ILIKE

        if not has_fts:
            try:
                from sqlalchemy import or_

                q = session.query(ParsedCredential)
                text_conditions = []
                if domain:
                    text_conditions.append(ParsedCredential.domain.ilike(f"%{query}%"))
                if username:
                    text_conditions.append(ParsedCredential.username.ilike(f"%{query}%"))
                if url:
                    text_conditions.append(ParsedCredential.url.ilike(f"%{query}%"))
                q = q.filter(or_(*text_conditions))
                if stealer:
                    q = q.filter(ParsedCredential.stealer_type == stealer)
                if app:
                    q = q.filter(ParsedCredential.application.ilike(f"%{app}%"))
                if email_domain:
                    q = q.filter(ParsedCredential.email_domain.ilike(f"%{email_domain}%"))
                if source:
                    q = q.filter(ParsedCredential.source_archive.ilike(f"%{source}%"))
                total = q.count()
                results = soft_dedupe_credentials(q.limit(limit * 5).all(), limit=limit)
            except Exception as exc:
                # Never hang the CLI on an unbounded ILIKE count over 319M
                # rows (the FTS fallback path has no 30s statement_timeout).
                try:
                    session.rollback()
                except Exception:
                    pass
                console.print(f"[yellow]Search failed: {exc}[/yellow]")
                return

        if not results:
            console.print(f"[yellow]No results for '{query}'[/yellow]")
            raise typer.Exit()

        console.print(f"[green]Found {total:,} results (showing {len(results)})[/green]\n")

        table = Table()
        table.add_column("Domain", style="cyan", max_width=30)
        table.add_column("Username", style="yellow", max_width=30)
        table.add_column("Password", style="red", max_width=20)
        table.add_column("App", style="blue", max_width=12)
        table.add_column("Stealer", style="magenta", max_width=10)
        table.add_column("Source", style="dim", max_width=25)

        for cred in results:
            pwd = (
                cred.password
                if show_password
                else (
                    cred.password[:8] + "..."
                    if cred.password and len(cred.password) > 8
                    else cred.password
                )
            )
            table.add_row(
                cred.domain or "-",
                cred.username or "-",
                pwd or "-",
                cred.application or "-",
                cred.stealer_type or "-",
                cred.source_archive or "-",
            )

        console.print(table)

        if total > limit:
            console.print("\n[dim]Use --limit to see more results[/dim]")
        elif not has_fts:
            console.print(
                "[dim]FTS unavailable; using slower fallback search. "
                "Run `telecrime fts rebuild` to enable indexing.[/dim]"
            )


@app.command()
def domains(
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
    limit: int = typer.Option(50, "--limit", "-n", help="Number of domains to show"),
    pattern: str | None = typer.Option(None, "--filter", "-f", help="Filter domains by pattern"),
) -> None:
    """List domains with credential counts."""
    from sqlalchemy import func

    from telecrime.models import ParsedCredential

    _, engine = get_config_and_engine(config_path)

    with get_session(engine) as session:
        q = session.query(
            ParsedCredential.domain, func.count(ParsedCredential.id).label("count")
        ).group_by(ParsedCredential.domain)

        if pattern:
            q = q.filter(ParsedCredential.domain.ilike(f"%{pattern}%"))

        q = q.order_by(func.count(ParsedCredential.id).desc()).limit(limit)

        results = q.all()

        if not results:
            console.print("[yellow]No domains found[/yellow]")
            raise typer.Exit()

        table = Table(title=f"Top {len(results)} Domains")
        table.add_column("#", justify="right", style="dim")
        table.add_column("Domain", style="cyan")
        table.add_column("Credentials", justify="right", style="green")

        for i, (domain, count) in enumerate(results, 1):
            table.add_row(str(i), domain or "(empty)", f"{count:,}")

        console.print(table)


# Stats subcommand group
stats_app = typer.Typer(name="stats", help="Analytics and statistics", no_args_is_help=True)
app.add_typer(stats_app)


@stats_app.command("stealers")
def stats_stealers(
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
    top_domains: int | None = typer.Option(
        None, "--top-domains", "-t", help="Show top N domains per stealer"
    ),
    by_channel: bool = typer.Option(
        False, "--by-channel", help="Show stealer distribution by channel"
    ),
    timeline: bool = typer.Option(False, "--timeline", help="Show monthly credential trends"),
) -> None:
    """Analyze stealer type distribution and statistics."""
    from sqlalchemy import func

    from telecrime.models import Conversation, ParsedCredential

    _, engine = get_config_and_engine(config_path)

    with get_session(engine) as session:
        total = session.query(ParsedCredential).count()

        if total == 0:
            console.print("[yellow]No credentials in database[/yellow]")
            raise typer.Exit()

        # Basic stealer breakdown
        console.print("\n[bold]Stealer Type Distribution:[/bold]")
        stealer_stats = (
            session.query(
                ParsedCredential.stealer_type,
                func.count(ParsedCredential.id).label("count"),
                func.count(func.distinct(ParsedCredential.domain)).label("unique_domains"),
            )
            .group_by(ParsedCredential.stealer_type)
            .order_by(func.count(ParsedCredential.id).desc())
            .all()
        )

        table = Table()
        table.add_column("Stealer Type", style="cyan")
        table.add_column("Credentials", justify="right", style="green")
        table.add_column("%", justify="right", style="yellow")
        table.add_column("Unique Domains", justify="right", style="blue")

        for stype, count, domains in stealer_stats:
            pct = (count / total) * 100
            table.add_row(
                stype or "(unknown)",
                f"{count:,}",
                f"{pct:.1f}%",
                f"{domains:,}",
            )

        console.print(table)

        # Top domains per stealer
        if top_domains:
            console.print(f"\n[bold]Top {top_domains} Domains per Stealer Type:[/bold]")
            for stype, _, _ in stealer_stats:
                if stype is None:
                    continue
                console.print(f"\n[cyan]{stype}:[/cyan]")
                domains = (
                    session.query(
                        ParsedCredential.domain, func.count(ParsedCredential.id).label("count")
                    )
                    .filter(ParsedCredential.stealer_type == stype)
                    .group_by(ParsedCredential.domain)
                    .order_by(func.count(ParsedCredential.id).desc())
                    .limit(top_domains)
                    .all()
                )

                for domain, count in domains:
                    console.print(f"  {domain or '(empty)'}: {count:,}")

        # Distribution by channel
        if by_channel:
            console.print("\n[bold]Stealer Distribution by Source Channel:[/bold]")
            channel_stats = (
                session.query(
                    Conversation.title,
                    ParsedCredential.stealer_type,
                    func.count(ParsedCredential.id).label("count"),
                )
                .join(ParsedCredential.source_conversation)
                .group_by(Conversation.title, ParsedCredential.stealer_type)
                .order_by(Conversation.title, func.count(ParsedCredential.id).desc())
                .all()
            )

            if channel_stats:
                table = Table()
                table.add_column("Channel", style="cyan", max_width=40)
                table.add_column("Stealer", style="magenta")
                table.add_column("Count", justify="right", style="green")

                for title, stype, count in channel_stats:
                    table.add_row(
                        (title or "-")[:40],
                        stype or "(unknown)",
                        f"{count:,}",
                    )
                console.print(table)
            else:
                console.print("[dim]No channel attribution data available[/dim]")

        # Monthly timeline
        if timeline:
            console.print("\n[bold]Monthly Credential Timeline:[/bold]")
            _month_expr = func.to_char(ParsedCredential.created_at, "YYYY-MM")
            monthly = (
                session.query(
                    _month_expr.label("month"),
                    ParsedCredential.stealer_type,
                    func.count(ParsedCredential.id).label("count"),
                )
                .group_by(_month_expr, ParsedCredential.stealer_type)
                .order_by(_month_expr.desc())
                .limit(100)
                .all()
            )

            if monthly:
                # Pivot data by month
                months_data: dict[str, dict[str, int]] = {}
                stealers_seen: set[str] = set()
                for month, stype, count in monthly:
                    month_key = str(month)
                    stealer_key = stype or "(unknown)"
                    if month_key not in months_data:
                        months_data[month_key] = {}
                    months_data[month_key][stealer_key] = count
                    stealers_seen.add(stealer_key)

                table = Table(title="Credentials by Month")
                table.add_column("Month", style="cyan")
                for stype in sorted(stealers_seen):
                    table.add_column(stype, justify="right")
                table.add_column("Total", justify="right", style="bold")

                for month in sorted(months_data.keys(), reverse=True)[:12]:
                    row = [month]
                    month_total = 0
                    for stype in sorted(stealers_seen):
                        count = months_data[month].get(stype, 0)
                        month_total += count
                        row.append(f"{count:,}" if count else "-")
                    row.append(f"{month_total:,}")
                    table.add_row(*row)

                console.print(table)
            else:
                console.print("[dim]No timeline data available[/dim]")


@app.command()
def export(
    output: Path = typer.Argument(..., help="Output CSV file path"),
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
    domain: str | None = typer.Option(
        None, "--domain", "-d", help="Filter by domain (supports %)"
    ),
    username: str | None = typer.Option(None, "--user", "-u", help="Filter by username pattern"),
    stealer: str | None = typer.Option(None, "--stealer", "-s", help="Filter by stealer type"),
    limit: int | None = typer.Option(None, "--limit", "-n", help="Max records to export"),
) -> None:
    """Export credentials to CSV file."""
    import csv

    from telecrime.models import ParsedCredential

    _, engine = get_config_and_engine(config_path)

    with get_session(engine) as session:
        q = session.query(ParsedCredential)

        if domain:
            q = q.filter(ParsedCredential.domain.ilike(domain.replace("*", "%")))
        if username:
            q = q.filter(ParsedCredential.username.ilike(username.replace("*", "%")))
        if stealer:
            q = q.filter(ParsedCredential.stealer_type == stealer)
        if limit:
            q = q.limit(limit)

        total = q.count()

        if total == 0:
            console.print("[yellow]No matching credentials to export[/yellow]")
            raise typer.Exit()

        console.print(f"Exporting {total:,} credentials to {output}...")

        fieldnames = [
            "url",
            "domain",
            "username",
            "password",
            "application",
            "profile",
            "source_archive",
            "source_file",
            "stealer_type",
        ]

        with open(output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for cred in q.yield_per(10000):
                writer.writerow(
                    {
                        "url": cred.url,
                        "domain": cred.domain,
                        "username": cred.username,
                        "password": cred.password,
                        "application": cred.application,
                        "profile": cred.profile,
                        "source_archive": cred.source_archive,
                        "source_file": cred.source_file,
                        "stealer_type": cred.stealer_type,
                    }
                )

        console.print(f"[green]Exported {total:,} credentials to {output}[/green]")


@app.command()
def channels(
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
    discover: bool = typer.Option(
        False, "--discover", "-d", help="Discover channels from database"
    ),
    check: bool = typer.Option(
        False, "--check", help="Check if channels still exist (requires Telegram)"
    ),
    export: Path | None = typer.Option(None, "--export", "-e", help="Export to text file"),
    show_all: bool = typer.Option(
        False, "--all", "-a", help="Show all channels including inactive"
    ),
    subscribe: bool = typer.Option(
        False, "--subscribe", "-s", help="Subscribe to discovered channels"
    ),
    filter_pattern: str | None = typer.Option(
        None, "--filter", "-f", help="Filter channels by username pattern"
    ),
    sub_limit: int = typer.Option(50, "--limit", "-n", help="Max channels to subscribe"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview without joining"),
    delay: float = typer.Option(2.0, "--delay", help="Seconds between join attempts"),
    stealer_only: bool = typer.Option(
        True,
        "--stealer-only/--all-channels",
        help="Only subscribe to stealer-log channels (default: True)",
    ),
    dork: bool = typer.Option(False, "--dork", help="Discover private channels via DuckDuckGo dorking"),
    dork_keywords: list[str] | None = typer.Option(
        None, "--dork-keywords", help="Custom keywords for dorking (default: stealer log, credentials passwords, ...)"
    ),
) -> None:
    """List and manage tracked Telegram channels."""
    import asyncio

    from sqlalchemy import func

    from telecrime.channels.discover import (
        discover_channels_from_db,
        persist_discovery_state,
        save_discovered_channels,
        update_channel_stats,
    )
    from telecrime.channels.service import (
        build_subscription_query,
        mark_channel_check_failed,
        mark_channel_checked,
        mark_channel_join_failed,
        mark_channel_join_result,
    )
    from telecrime.models import TelegramChannel

    config, engine = get_config_and_engine(config_path)

    with get_session(engine) as session:
        # Discover new channels from database
        if discover:
            console.print("[cyan]Discovering channels from database...[/cyan]")
            scan_result = discover_channels_from_db(session)
            new_count, updated_count = save_discovered_channels(session, scan_result.channels)
            persist_discovery_state(session, scan_result)
            update_channel_stats(session)
            console.print(
                f"[green]Discovered {new_count} new channels, updated {updated_count}[/green]"
            )

        # Dork for private channels via DuckDuckGo
        if dork:
            from telecrime.channels.discover import discover_channels_via_dork
            console.print("[cyan]Dorking DuckDuckGo for private Telegram invite links...[/cyan]")
            new_dork, found_links = discover_channels_via_dork(
                session, keywords=dork_keywords or None
            )
            console.print(
                f"[green]Dorking found {len(found_links)} invite links, {new_dork} new saved[/green]"
            )

        # Check channels via Telegram API
        if check:
            from telecrime.adapters.telegram import TelegramAdapter

            if not config.telegram.api_id or not config.telegram.api_hash:
                console.print("[red]Telegram credentials required for --check[/red]")
                raise typer.Exit(1)

            async def check_channels():
                adapter = TelegramAdapter(config)
                try:
                    await adapter.connect()
                    console.print("[cyan]Checking channel status...[/cyan]")

                    channels_to_check = (
                        session.query(TelegramChannel)
                        .filter(
                            (TelegramChannel.username != None)
                            | (TelegramChannel.platform_id != None)
                        )
                        .all()
                    )

                    checked = 0
                    for channel in channels_to_check:
                        try:
                            if channel.username:
                                entity = await adapter.client.get_entity(f"@{channel.username}")
                            elif channel.platform_id:
                                entity = await adapter.client.get_entity(channel.platform_id)
                            else:
                                continue

                            mark_channel_checked(channel, entity)
                            checked += 1

                        except Exception as e:
                            mark_channel_check_failed(channel, str(e))

                        if checked % 10 == 0:
                            session.commit()
                            console.print(f"  Checked {checked} channels...")

                    session.commit()
                    console.print(f"[green]Checked {checked} channels[/green]")

                finally:
                    await adapter.disconnect()

            asyncio.run(check_channels())

        # Subscribe to channels
        if subscribe:
            from telecrime.adapters.telegram import TelegramAdapter

            if not config.telegram.api_id or not config.telegram.api_hash:
                console.print("[red]Telegram credentials required for --subscribe[/red]")
                raise typer.Exit(1)

            sub_query = build_subscription_query(
                session,
                stealer_only=stealer_only,
                filter_pattern=filter_pattern,
            )

            if stealer_only:
                console.print(
                    "[dim]Filtering for stealer-log channels (use --all-channels to disable)[/dim]"
                )

            channels_to_sub = sub_query.limit(sub_limit).all()

            if not channels_to_sub:
                console.print("[yellow]No channels to subscribe to[/yellow]")
            else:
                if dry_run:
                    console.print(
                        f"[cyan]DRY RUN: Would subscribe to {len(channels_to_sub)} channels:[/cyan]"
                    )
                    for ch in channels_to_sub:
                        console.print(f"  {ch.display_name} - {ch.title or '-'}")
                else:

                    async def subscribe_to_channels():
                        adapter = TelegramAdapter(config)
                        try:
                            await adapter.connect()
                            console.print(
                                f"[cyan]Subscribing to {len(channels_to_sub)} channels...[/cyan]"
                            )

                            joined = 0
                            skipped = 0
                            failed = 0

                            for i, channel in enumerate(channels_to_sub):
                                target = channel.username or channel.invite_link
                                console.print(
                                    f"  [{i + 1}/{len(channels_to_sub)}] Joining {channel.display_name}...",
                                    end=" ",
                                )

                                try:
                                    success = await adapter.join_conversation(
                                        channel.platform_id or 0,
                                        username=target,
                                    )

                                    if mark_channel_join_result(channel, success) == "joined":
                                        joined += 1
                                        console.print("[green]OK[/green]")
                                    else:
                                        failed += 1
                                        console.print("[red]FAILED[/red]")

                                except Exception as e:
                                    error_msg = str(e)
                                    result = mark_channel_join_failed(channel, error_msg)

                                    if result == "already":
                                        skipped += 1
                                        console.print("[yellow]ALREADY[/yellow]")
                                    elif (
                                        "private" in error_msg.lower()
                                        or "invite" in error_msg.lower()
                                    ):
                                        failed += 1
                                        console.print("[red]PRIVATE[/red]")
                                    else:
                                        failed += 1
                                        console.print(f"[red]ERROR: {error_msg[:50]}[/red]")

                                # Commit every 10 channels for crash safety
                                if (i + 1) % 10 == 0:
                                    session.commit()

                                # Rate limiting delay
                                if i < len(channels_to_sub) - 1:
                                    await asyncio.sleep(delay)

                            session.commit()
                            console.print(
                                f"\n[green]Done! Joined: {joined}, Already: {skipped}, Failed: {failed}[/green]"
                            )

                        finally:
                            await adapter.disconnect()

                    asyncio.run(subscribe_to_channels())

        # Get channels from database
        query = session.query(TelegramChannel)
        if not show_all:
            query = query.filter(TelegramChannel.is_active == True)

        channels_list = query.order_by(TelegramChannel.source, TelegramChannel.username).all()

        if not channels_list:
            console.print(
                "[yellow]No channels in database. Use --discover to find channels.[/yellow]"
            )
            raise typer.Exit()

        # Export to file - only channels with valid Telegram URLs
        if export:
            # Filter to only channels with proper t.me links (have username)
            exportable = [
                ch
                for ch in channels_list
                if ch.telegram_link and ch.telegram_link.startswith("https://t.me/")
            ]

            with open(export, "w", encoding="utf-8") as f:
                f.write("# Telegram Stealer Log Channels\n")
                f.write(f"# Generated: {datetime.now(UTC).isoformat()}\n")
                f.write(f"# Total: {len(exportable)} channels with valid URLs\n\n")

                for ch in sorted(
                    exportable, key=lambda x: x.username.lower() if x.username else ""
                ):
                    f.write(f"{ch.telegram_link}\n")

            console.print(
                f"[green]Exported {len(exportable)} channels with valid URLs to {export}[/green]"
            )

        # Summary by source
        console.print("\n[bold]Channel Summary by Source:[/bold]")
        source_counts = (
            session.query(TelegramChannel.source, func.count(TelegramChannel.id))
            .group_by(TelegramChannel.source)
            .all()
        )

        for source, count in source_counts:
            console.print(f"  {source}: {count}")

        # Status summary
        active = session.query(TelegramChannel).filter(TelegramChannel.is_active == True).count()
        inactive = session.query(TelegramChannel).filter(TelegramChannel.is_active == False).count()
        subscribed = (
            session.query(TelegramChannel).filter(TelegramChannel.is_subscribed == True).count()
        )

        console.print("\n[bold]Status:[/bold]")
        console.print(f"  Active: {active}")
        console.print(f"  Inactive/Deleted: {inactive}")
        console.print(f"  Subscribed: {subscribed}")

        # Show channels table
        table = Table(title=f"Channels ({len(channels_list)})")
        table.add_column("Channel", style="cyan")
        table.add_column("Title", style="dim", max_width=30)
        table.add_column("Source", style="yellow")
        table.add_column("Status", style="green")
        table.add_column("Msgs", justify="right")

        for ch in channels_list[:50]:  # Limit display
            status = "Active"
            if not ch.is_active:
                status = "[red]Deleted[/red]"
            elif not ch.is_accessible:
                status = "[yellow]Private[/yellow]"
            elif ch.is_subscribed:
                status = "[green]Subscribed[/green]"

            table.add_row(
                ch.display_name,
                (ch.title or "-")[:30],
                ch.source,
                status,
                str(ch.messages_seen) if ch.messages_seen else "-",
            )

        console.print(table)

        if len(channels_list) > 50:
            console.print(
                f"[dim]Showing 50 of {len(channels_list)} channels. Use --export to see all.[/dim]"
            )


@app.command()
def process(
    folder: Path = typer.Argument(..., help="Folder containing archive files"),
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
    delete_after: bool = typer.Option(
        False, "--delete", "-d", help="Delete archives after processing"
    ),
    limit: int | None = typer.Option(None, "--limit", "-n", help="Process only N archives"),
    output: Path | None = typer.Option(None, "--output", "-o", help="Also export to CSV"),
) -> None:
    """Process a folder of archives (standalone mode)."""
    import subprocess

    config, _engine = get_config_and_engine(config_path)

    # Build command for process_folder.py
    cmd = ["uv", "run", "python", "process_folder.py", str(folder)]

    if config_path:
        cmd.extend(["--config", str(config_path)])
    else:
        cmd.extend(["--database", config.database_url])

    if delete_after:
        cmd.append("--delete-after")
    if limit:
        cmd.extend(["--limit", str(limit)])
    if output:
        cmd.extend(["--output", str(output)])

    console.print(f"[cyan]Running: {' '.join(cmd)}[/cyan]\n")

    # Run the process_folder.py script
    result = subprocess.run(cmd, cwd=Path(__file__).parent.parent)
    raise typer.Exit(result.returncode)


@app.command("reparse-stealers")
def reparse_stealers_cmd(
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
    dry_run: bool = typer.Option(False, "--dry-run", "-n", help="Count rows without writing"),
    limit: int | None = typer.Option(
        None, "--limit", "-l", help="Max ExtractionJobs to process"
    ),
) -> None:
    """Backfill stealer_type on ParsedCredential rows where it is NULL."""
    from telecrime.scheduler import _reparse_stealers_impl

    _, engine = get_config_and_engine(config_path)
    result = _reparse_stealers_impl(engine, limit=limit, dry_run=dry_run)
    console.print(f"[green]{result}[/green]")


@app.command("shutdown-request")
def shutdown_request_cmd(
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
    clear: bool = typer.Option(False, "--clear", help="Clear an existing shutdown request"),
    wait: bool = typer.Option(False, "--wait", help="Wait until the running pipeline has drained"),
    notify: bool = typer.Option(
        True,
        "--notify/--no-notify",
        help="Send a desktop notification after --wait completes",
    ),
    timeout: int | None = typer.Option(
        None,
        "--timeout",
        help="Maximum seconds to wait with --wait",
    ),
    mode: str = typer.Option(
        "finish_archive",
        "--mode",
        help="Shutdown mode to request",
    ),
    reason: str | None = typer.Option(None, "--reason", help="Operator note for the request"),
) -> None:
    """Request or clear a graceful pipeline shutdown."""
    from telecrime.scheduler import (
        TelecrimeWorker,
        _shutdown_state,
        clear_shutdown_request,
        read_shutdown_request,
        send_desktop_notification,
        write_shutdown_request,
    )

    config, engine = get_config_and_engine(config_path)

    if clear:
        clear_shutdown_request()
        console.print("[green]Cleared shutdown request[/green]")
        raise typer.Exit()

    request = write_shutdown_request(mode=mode, reason=reason)
    state = _shutdown_state(config) or "requested"
    console.print(
        "[yellow]Graceful shutdown requested[/yellow] "
        f"({request['mode']}, state={state}, at={request['requested_at']})"
    )
    if reason:
        console.print(f"Reason: {reason}")
    active = read_shutdown_request()
    if active:
        console.print("New work will not start until the request is cleared.")
    if wait:
        worker = TelecrimeWorker(config, engine)
        console.print("[cyan]Waiting for running pipeline work to drain...[/cyan]")
        drained = worker.wait_until_drained(timeout_seconds=timeout)
        if not drained:
            console.print("[red]Timed out waiting for pipeline shutdown[/red]")
            raise typer.Exit(124)
        console.print("[green]Pipeline shutdown complete.[/green]")
        if notify:
            result = send_desktop_notification(
                "Telecrime pipeline stopped",
                "Pipeline shutdown is complete.",
            )
            if result != "sent":
                console.print(f"[yellow]Desktop notification {result}[/yellow]")


@app.command("channels-export")
def channels_export(
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
    output_dir: Path = typer.Option(
        Path("."), "--output-dir", "-o", help="Directory to write channels.md and channels.txt"
    ),
    commit: bool = typer.Option(False, "--commit", help="git commit the generated files"),
    push: bool = typer.Option(False, "--push", help="git push after commit (implies --commit)"),
    push_github: bool = typer.Option(
        False, "--push-github", help="git push to the 'github' remote after commit"
    ),
) -> None:
    """Export channel lists: channels.md (subscribed + candidates) and channels.txt (all active).

    Only channels that are both active AND accessible are exported — channels
    reported as deleted, banned, private, or otherwise unreachable by Telegram
    are filtered out. With --commit/--push the generated lists are committed
    and pushed to the configured git remote (e.g. the public GitHub repo).
    """
    import subprocess

    from telecrime.channels.export import export_reports
    from telecrime.config import load_config
    from telecrime.database import get_engine, get_session

    config = load_config(config_path)
    engine = get_engine(config.database_url)

    with get_session(engine) as session:
        md_path, txt_path = export_reports(session, output_dir)

    console.print(f"[green]Written:[/green] {md_path}")
    console.print(f"[green]Written:[/green] {txt_path}")

    if push:
        commit = True
        _push_remote = "origin"
    elif push_github:
        commit = True
        _push_remote = "github"
    else:
        _push_remote = None

    if commit:
        result = subprocess.run(
            ["git", "add", str(md_path), str(txt_path)],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            console.print(f"[red]git add failed:[/red] {result.stderr.strip()}")
            raise typer.Exit(1)

        result = subprocess.run(
            ["git", "commit", "-m", "chore: update channel lists [auto]", "--allow-empty"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            console.print(f"[yellow]git commit:[/yellow] {result.stdout.strip()}")
        else:
            console.print(f"[green]Committed:[/green] {result.stdout.splitlines()[0] if result.stdout else 'ok'}")

    if _push_remote:
        result = subprocess.run(
            ["git", "push", _push_remote],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            console.print(f"[red]git push failed:[/red] {result.stderr.strip()}")
            raise typer.Exit(1)
        console.print(f"[green]Pushed to {_push_remote}.[/green]")


@app.command()
def worker(
    config_path: Path | None = typer.Option(None, "--config", "-c", help="Config file path"),
    pipeline_hours: int = typer.Option(4, "--pipeline-hours", help="Hours between pipeline runs"),
    channel_hours: int = typer.Option(1, "--channel-hours", help="Hours between channel join runs"),
    watchdog_minutes: int = typer.Option(
        10, "--watchdog-minutes", help="Minutes between pipeline watchdog checks"
    ),
    health_hours: int = typer.Option(
        2, "--health-hours", help="Hours between pipeline health/progress checks"
    ),
    vacuum_hours: int = typer.Option(
        168, "--vacuum-hours", help="Hours between VACUUM runs (default: weekly)"
    ),
) -> None:
    """Run the background scheduler (periodic pipeline, channel joins, VACUUM)."""
    import signal

    from telecrime.scheduler import JOB_DEFS, TelecrimeWorker, send_desktop_notification

    config, engine = get_config_and_engine(config_path)

    # Override intervals from CLI args
    JOB_DEFS["pipeline"]["interval_hours"] = pipeline_hours
    JOB_DEFS["channel_join"]["interval_hours"] = channel_hours
    JOB_DEFS["pipeline_watchdog"]["interval_minutes"] = watchdog_minutes
    JOB_DEFS["pipeline_health"]["interval_hours"] = health_hours
    JOB_DEFS["vacuum"]["interval_hours"] = vacuum_hours

    has_tg = config.telegram.api_id and config.telegram.api_hash
    console.print("[cyan]Starting Telecrime worker[/cyan]")
    console.print(f"  Pipeline:    every {pipeline_hours}h")
    console.print(f"  Channel join: every {channel_hours}h")
    console.print(f"  Watchdog:    every {watchdog_minutes}m")
    console.print(f"  Health:      every {health_hours}h")
    console.print(f"  VACUUM:      every {vacuum_hours}h")
    if not has_tg:
        console.print(
            "[yellow]Warning: Telegram credentials not configured — pipeline and channel jobs disabled[/yellow]"
        )

    # Clear stale shutdown requests BEFORE starting the worker so APScheduler's
    # coalesced job catchup on startup doesn't see the file and skip all jobs.
    # Signal-triggered requests always belong to a prior run — drop unconditionally.
    from telecrime.scheduler import clear_shutdown_request, read_shutdown_request
    _stale = read_shutdown_request()
    if _stale:
        try:
            _reason = _stale.get("reason", "")
            if "sigterm" in _reason or "sigint" in _reason or "sigkill" in _reason or "cli-stop" in _reason:
                clear_shutdown_request()
                _stale = None
        except Exception:
            pass

    w = TelecrimeWorker(config, engine)
    w.start()

    stop_event = threading.Event()
    shutdown_requested = threading.Event()

    def _sig_handler(sig, frame):
        del frame
        signal_name = signal.Signals(sig).name
        if not shutdown_requested.is_set():
            request = w.request_shutdown(reason=f"signal:{signal_name.lower()}")
            console.print(
                "\n[yellow]Graceful shutdown requested[/yellow] "
                f"({request['mode']} at {request['requested_at']}). "
                "Waiting for running work to drain."
            )
            shutdown_requested.set()
        stop_event.set()

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    console.print("[green]Worker running. Press Ctrl+C to stop.[/green]")
    if _stale and w.is_shutdown_requested():
        shutdown_requested.set()
        stop_event.set()

    while True:
        if not stop_event.wait(timeout=5):
            continue
        if not shutdown_requested.is_set():
            shutdown_requested.set()
            request = w.request_shutdown(reason="cli-stop")
            console.print(
                "[yellow]Graceful shutdown requested[/yellow] "
                f"({request['mode']} at {request['requested_at']})."
            )
        if w.can_exit():
            break
        console.print("[cyan]Waiting for running jobs to finish current work...[/cyan]")

    w.stop()
    notify_result = send_desktop_notification(
        "Telecrime worker stopped",
        "Pipeline shutdown is complete.",
    )
    if notify_result != "sent":
        console.print(f"[yellow]Desktop notification {notify_result}[/yellow]")
    console.print("[green]Worker stopped.[/green]")


if __name__ == "__main__":
    app()
