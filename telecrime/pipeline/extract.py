"""Stage 6: Extract - run 7z with password candidates, unrar fallback."""

import asyncio
import hashlib
import logging
import shutil
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import joinedload

from telecrime.extractor.interface import ExtractionResult
from telecrime.extractor.seven_zip import SevenZipExtractor
from telecrime.extractor.unrar import UnrarExtractor
from telecrime.logging_utils import log_context
from telecrime.models import (
    ArchiveGroup,
    ArchiveGroupPart,
    DownloadArtifact,
    ExtractedOutput,
    ExtractionJob,
    PasswordCandidate,
)
from telecrime.passwords.extractor import MAX_FAILED_ATTEMPTS, extract_passwords_from_context
from telecrime.passwords.ranker import deduplicate_candidates, rank_passwords
from telecrime.pipeline.orchestrator import PipelineContext, PipelineStage
from telecrime.states import ExtractionStatus, GroupStatus

logger = logging.getLogger(__name__)

_RAR5_SIGNATURE = b"\x52\x61\x72\x21\x1a\x07\x01\x00"


def _is_rar5(path: Path) -> bool:
    """Return True if the file starts with the RAR5 magic bytes."""
    try:
        with open(path, "rb") as f:
            return f.read(8) == _RAR5_SIGNATURE
    except Exception:
        return False


def _get_group_message_text(group) -> str | None:
    """Return the Telegram message text/caption for the first part of a group."""
    try:
        first_part = group.parts[0] if group.parts else None
        if not first_part or not first_part.artifact:
            return None
        attachment = first_part.artifact.attachment
        if not attachment:
            return None
        msg = attachment.message
        if not msg:
            return None
        return msg.text or msg.caption
    except Exception:
        return None


def _extraction_timeout(ctx, archive: Path) -> int:
    """Extraction timeout proportional to archive size.

    A fixed cap (config max_extraction_seconds=600) kills legitimately slow
    extractions of multi-GB archives (tens of thousands of small files take
    30-60+ min on this hardware), failing the job and retrying in a loop that
    never completes — same bug class as the fixed 300s download budget.
    """
    size_mb: float = 0
    try:
        size_mb = archive.stat().st_size / (1024 * 1024)
    except OSError:
        pass
    return max(ctx.config.extraction.max_extraction_seconds, int(size_mb * 3))


class ExtractStage(PipelineStage):
    """Extract archives using 7z with password inference."""

    name = "extract"

    async def run(self, ctx: PipelineContext) -> bool:
        """Run the extract stage."""
        logger.info("Starting archive extraction")

        # Fetch IDs only, then commit immediately so the read transaction is
        # released before the potentially long 7z extraction begins.
        # Groups whose job is PASSWORD_NEEDED are retried once per run (next
        # run), not re-extracted every pass — a password learned mid-run is
        # picked up by the next run's READY sweep.
        from telecrime.pipeline.orchestrator import _pwd_needed_exists_expr

        ready_ids = (
            ctx.session.execute(
                select(ArchiveGroup.id)
                .where(
                    ArchiveGroup.status == GroupStatus.READY,
                    ~_pwd_needed_exists_expr(),
                )
            )
            .scalars()
            .all()
        )
        ctx.session.commit()

        if not ready_ids:
            logger.info("No archive groups ready for extraction")
            return True

        logger.info("Found %d groups ready for extraction", len(ready_ids))

        extractor: SevenZipExtractor | UnrarExtractor = SevenZipExtractor(
            ctx.config.extraction.extractor_path
        )

        for group_id in ready_ids:
            # Re-fetch with relationships in a fresh transaction.
            group = ctx.session.get(
                ArchiveGroup,
                group_id,
                options=[
                    joinedload(ArchiveGroup.parts)
                    .joinedload(ArchiveGroupPart.artifact)
                    .joinedload(DownloadArtifact.attachment)
                ],
            )
            if group is None or group.status != GroupStatus.READY:
                ctx.session.commit()
                continue

            with log_context(group_id=group.id, archive_name=group.base_name):
                if ctx.dry_run:
                    logger.info("[DRY RUN] Would extract group: %s", group.base_name)
                    ctx.session.commit()
                    continue

                try:
                    success = await self._extract_group(ctx, group, extractor)
                    if success:
                        ctx.archives_extracted += 1
                    ctx.session.commit()
                except Exception as e:
                    logger.error("Error extracting group %d: %s", group_id, e)
                    try:
                        ctx.session.rollback()
                    except Exception:
                        pass
                    ctx.errors.append(f"Extract error: {e}")

        return True

    async def _extract_group(
        self,
        ctx: PipelineContext,
        group: ArchiveGroup,
        extractor: SevenZipExtractor | UnrarExtractor,
    ) -> bool:
        """Extract a single archive group."""
        # Create or get extraction job
        job = ctx.session.execute(
            select(ExtractionJob).where(
                ExtractionJob.group_id == group.id,
                ExtractionJob.status.in_(
                    [
                        ExtractionStatus.PENDING,
                        ExtractionStatus.PASSWORD_NEEDED,
                    ]
                ),
            )
        ).scalar_one_or_none()

        if job is None:
            job = ExtractionJob(
                group_id=group.id,
                status=ExtractionStatus.PENDING,
                target_extensions=",".join(ctx.config.extraction.target_extensions),
            )
            ctx.session.add(job)
            ctx.session.flush()

        # Get archive paths
        archive_paths = []
        for part in sorted(group.parts, key=lambda p: p.part_index):
            if part.artifact.local_path:
                archive_paths.append(Path(part.artifact.local_path))

        if not archive_paths:
            logger.error("No local paths for group %s", group.base_name)
            job.status = ExtractionStatus.FAILED_TERMINAL
            job.last_error_message = "No archive files found"
            return False

        # Main archive is the first one (or only one)
        main_archive = archive_paths[0]

        # Dispatch direct .txt files — skip 7z and the disk space check entirely
        first_part = group.parts[0] if group.parts else None
        _attach = first_part.artifact.attachment if (first_part and first_part.artifact) else None
        if _attach and _attach.archive_type == "txt":
            return await self._handle_direct_txt(ctx, group, job, main_archive)

        # Resource guard: free disk check (not needed for direct txt files above).
        # Low disk is transient — finalize reclaims space and the scheduler skips
        # runs below threshold. Leave the group READY and the job PENDING so this
        # group is retried on a later run; do NOT mark it FAILED (that would feed
        # it to QuarantineStage and silently remove a healthy group from the
        # backlog over long disk-pressured unattended runs).
        if not self._has_sufficient_disk(ctx):
            logger.warning(
                "Skipping extraction of group %s — less than %d MB free disk. "
                "Group stays READY for retry once space is reclaimed.",
                group.base_name,
                ctx.config.extraction.min_free_disk_mb,
            )
            return False

        # Get password candidates
        passwords = await self._get_password_candidates(ctx, group)

        # Output directory
        output_dir = ctx.config.extracted_dir / f"group_{group.id}"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Mark as in-progress and commit immediately so the transaction snapshot
        # is released before the potentially long 7z subprocess runs.
        group.status = GroupStatus.EXTRACTING
        job.status = ExtractionStatus.IN_PROGRESS
        job.attempts_count += 1
        ctx.session.flush()
        ctx.session.commit()

        target_exts = ctx.config.extraction.target_extensions
        _nested_exts = {"zip", "rar", "7z"}

        # RAR5 archives: 7z support is incomplete — it can list headers but fails
        # extraction with PASSWORD_REQUIRED (even for unencrypted archives).
        # Detect the RAR5 signature and switch to unrar immediately to avoid
        # wasting time on the 7z attempt + full password-testing loop.
        is_rar = main_archive.suffix.lower() == ".rar"
        if is_rar and isinstance(extractor, SevenZipExtractor) and UnrarExtractor.available():
            if _is_rar5(main_archive):
                logger.info("RAR5 archive detected — using unrar directly: %s", main_archive.name)
                extractor = UnrarExtractor()

        # Check if archive contains any matching files before extracting.
        # list_contents returns [] both when listing succeeds with no files
        # and when it fails (e.g. password-protected). We distinguish by also
        # listing all contents: if all_contents is non-empty but no extensions
        # match, we can skip extraction entirely.
        logger.info("Checking %s for matching files: %s", main_archive.name, target_exts)
        all_contents = await extractor.list_contents(main_archive, password=None)

        # Whether to do a two-pass extraction (outer → nested archives → txt)
        do_nested = False
        # Must be initialized BEFORE the `if all_contents:` block: list_contents
        # returns [] for password-protected archives (or listing failures), and
        # `matching_files[0]` below would otherwise raise UnboundLocalError,
        # skipping the password-fallback path entirely (regression: "cannot
        # access local variable 'matching_files'" in pipeline logs).
        matching_files: list[str] = []
        if all_contents:
            # Listing succeeded — check for matching extensions
            normalized_exts = {ext.lower().lstrip(".") for ext in target_exts}
            matching_files = [
                f for f in all_contents if Path(f).suffix.lower().lstrip(".") in normalized_exts
            ]
            if matching_files:
                logger.info("Found %d matching files in %s", len(matching_files), main_archive.name)
            else:
                nested_count = sum(
                    1 for f in all_contents
                    if Path(f).suffix.lower().lstrip(".") in _nested_exts
                )
                if nested_count:
                    # Outer archive contains nested zip/rar files — extract all to
                    # get inner archives, then recursively extract those for txt.
                    logger.info(
                        "Archive %s has %d nested archives — attempting recursive extraction",
                        main_archive.name,
                        nested_count,
                    )
                    do_nested = True
                else:
                    logger.info(
                        "Archive %s has %d files but none match %s — skipping",
                        main_archive.name,
                        len(all_contents),
                        target_exts,
                    )
                    group.status = GroupStatus.EXTRACTED
                    job.status = ExtractionStatus.COMPLETED
                    job.last_error_message = "No files matching target extensions"
                    return True
        else:
            logger.debug("Could not list contents of %s (may need password)", main_archive.name)

        # For nested-archive mode, extract the outer archive without extension
        # filtering so the inner zip/rar files land in output_dir.
        outer_exts = [] if do_nested else target_exts

        # Pick a file from the listing to use for fast password pre-screening.
        # For ZipCrypto archives, filenames are visible without a password, so
        # all_contents may be non-empty even for password-protected archives.
        # Pass the FULL relative path (e.g. "dir/file.txt") — not just the
        # basename — so 7z can locate the specific file without scanning all 60k+
        # entries. A directory entry or basename-only filter matches 0 files,
        # causing test_password to report "Everything is Ok" for any password.
        first_file: str | None = matching_files[0] if matching_files else None

        # Try extraction with the given extractor, with password fallback
        result = await self._try_extract_with_passwords(
            extractor, main_archive, output_dir, outer_exts, passwords, job, ctx,
            first_file=first_file,
        )

        # If 7z fails on a .rar file (UNSUPPORTED_FORMAT, or all passwords fail
        # because 7z can't handle the RAR5 encryption method), retry with unrar.
        # (RAR5 archives are already handled above — extractor was switched to unrar.)
        sevenz_failed = not result.success and is_rar and isinstance(extractor, SevenZipExtractor)
        if sevenz_failed and UnrarExtractor.available():
            logger.info("7z failed for RAR (%s) — retrying with unrar: %s", result.error_code, main_archive.name)
            unrar = UnrarExtractor()
            # Clean output dir from 7z's failed attempt (may have 0-byte files)
            shutil.rmtree(output_dir, ignore_errors=True)
            output_dir.mkdir(parents=True, exist_ok=True)
            # Undo times_failed increments from 7z attempts — 7z failures on
            # RAR5 archives don't mean the password is wrong
            for pwd_candidate in passwords:
                if pwd_candidate.times_failed > 0:
                    pwd_candidate.times_failed = max(0, pwd_candidate.times_failed - 1)
            job.password_attempts = 0
            result = await self._try_extract_with_passwords(
                unrar, main_archive, output_dir, outer_exts, passwords, job, ctx,
            )
        elif sevenz_failed:
            logger.warning("unrar not installed — cannot extract RAR5 archive: %s", main_archive.name)

        if result.success:
            txt_files = result.extracted_files

            if do_nested:
                # Outer extracted nested archives — now recurse into them for txt.
                txt_files = await self._try_nested_extraction(
                    output_dir, target_exts, extractor, passwords, ctx,
                )
                if not txt_files:
                    logger.info(
                        "Nested archives in %s yielded no txt files — skipping",
                        main_archive.name,
                    )
                    group.status = GroupStatus.EXTRACTED
                    job.status = ExtractionStatus.COMPLETED
                    job.last_error_message = "No files matching target extensions (nested archives had no txt)"
                    return True

            await self._record_outputs(ctx, job, group, txt_files, output_dir)
            group.status = GroupStatus.EXTRACTED
            job.status = ExtractionStatus.COMPLETED
            logger.info(
                "Successfully extracted %d files from %s",
                len(txt_files),
                main_archive.name,
            )
            return True

        elif result.requires_password:
            job.status = ExtractionStatus.PASSWORD_NEEDED
            # Return the group to READY so startup recovery and future runs
            # retry it (a password learned from a later archive in the same
            # conversation should unlock it). Leaving it EXTRACTING would
            # strand it — neither the READY sweep nor the EXTRACTED sweep
            # picks it up for the rest of the run.
            group.status = GroupStatus.READY
            msg_text = _get_group_message_text(group)
            msg_suffix = f" | Message: {msg_text[:300]}" if msg_text else ""
            if passwords:
                job.last_error_message = f"All {len(passwords)} password candidates failed"
                logger.warning(
                    "Archive %s: all %d password candidates failed%s",
                    main_archive.name,
                    len(passwords),
                    msg_suffix,
                )
            else:
                job.last_error_message = "Archive requires password but none available"
                logger.warning(
                    "Archive %s needs password but none available%s",
                    main_archive.name,
                    msg_suffix,
                )
            return False

        else:
            # Extraction failed — but partial output may contain nested archives.
            # Try to recover txt from any zip/rar files extracted before the error.
            if output_dir.exists():
                nested_txts = await self._try_nested_extraction(
                    output_dir, target_exts, extractor, passwords, ctx,
                )
                if nested_txts:
                    logger.info(
                        "Recovered %d txt files from nested archives despite outer extraction error",
                        len(nested_txts),
                    )
                    await self._record_outputs(ctx, job, group, nested_txts, output_dir)
                    group.status = GroupStatus.EXTRACTED
                    job.status = ExtractionStatus.COMPLETED
                    return True

            # Corruption and unsupported formats won't improve on retry
            terminal = result.error_code in ("CORRUPTED", "UNSUPPORTED_FORMAT", "CANNOT_OPEN")
            job.status = ExtractionStatus.FAILED_TERMINAL if terminal else ExtractionStatus.FAILED
            job.last_error_code = result.error_code
            job.last_error_message = result.error_message
            group.status = GroupStatus.FAILED
            logger.error("Extraction failed for %s: %s", main_archive.name, result.error_message)
            return False

    async def _handle_direct_txt(
        self,
        ctx: PipelineContext,
        group: ArchiveGroup,
        job: ExtractionJob,
        txt_path: Path,
    ) -> bool:
        """Handle a direct .txt credential file — hardlink into extracted_dir, skip 7z."""
        output_dir = ctx.config.extracted_dir / f"group_{group.id}"
        output_dir.mkdir(parents=True, exist_ok=True)

        dest = output_dir / txt_path.name
        try:
            if dest.exists() and dest.stat().st_ino == txt_path.stat().st_ino:
                pass  # already hardlinked from a prior run, nothing to do
            else:
                try:
                    dest.hardlink_to(txt_path)
                except OSError:
                    shutil.copy2(txt_path, dest)
        except Exception as e:
            logger.error("Failed to link/copy txt file %s: %s", txt_path.name, e)
            job.status = ExtractionStatus.FAILED_TERMINAL
            job.last_error_message = str(e)
            group.status = GroupStatus.FAILED
            return False

        await self._record_outputs(ctx, job, group, [dest], output_dir)
        group.status = GroupStatus.EXTRACTED
        job.status = ExtractionStatus.COMPLETED
        logger.info("Direct txt file linked: %s", txt_path.name)
        return True

    async def _try_nested_extraction(
        self,
        output_dir: Path,
        target_exts: list[str],
        extractor,
        passwords: list[PasswordCandidate],
        ctx: PipelineContext,
    ) -> list[Path]:
        """Find nested zip/rar/7z files in output_dir and extract txt from each.

        Used when an outer archive contains per-victim zip files rather than
        raw txt files (e.g. PegasusCloud distributes each victim as a separate
        zip inside the outer RAR).  Returns all extracted target-extension paths.
        """
        _nested_exts = {".zip", ".rar", ".7z"}
        _max_nested = 500

        nested_archives = [
            f for f in output_dir.rglob("*")
            if f.is_file() and f.suffix.lower() in _nested_exts
        ]
        if not nested_archives:
            return []

        if len(nested_archives) > _max_nested:
            logger.warning(
                "Found %d nested archives — capping at %d", len(nested_archives), _max_nested
            )
            nested_archives = nested_archives[:_max_nested]

        logger.info("Extracting %d nested archives for txt files", len(nested_archives))
        password_values = [None] + [c.value for c in passwords]
        txt_files: list[Path] = []

        for nested in nested_archives:
            nested_out = nested.parent / (nested.stem + "_inner")
            nested_out.mkdir(parents=True, exist_ok=True)
            for password in password_values:
                result = await extractor.extract(
                    nested,
                    nested_out,
                    password=password,
                    target_extensions=target_exts,
                    timeout_seconds=_extraction_timeout(ctx, nested),
                )
                if result.success:
                    txt_files.extend(result.extracted_files)
                    break
                elif not result.requires_password:
                    # Non-password failure — no point retrying with other passwords
                    break

        return txt_files

    async def _try_extract_with_passwords(
        self,
        extractor,
        main_archive: Path,
        output_dir: Path,
        target_exts: list[str],
        passwords: list[PasswordCandidate],
        job: ExtractionJob,
        ctx: PipelineContext,
        first_file: str | None = None,
    ):
        """Try extraction without password, then with each candidate."""
        logger.info("Attempting extraction of %s with %s", main_archive.name, type(extractor).__name__)

        # If we have a known first_file (ZipCrypto — listing succeeded without password),
        # run a 0.57s quick test with an empty password before attempting the full 2-minute
        # no-password extraction.  A ZipCrypto archive with a non-empty password will fail
        # the empty-password test immediately, saving ~2 min per archive.  Unencrypted
        # archives return "Everything is Ok" and proceed to full extraction normally.
        _skip_nopassword = False
        if first_file and passwords:
            _skip_nopassword = not await extractor.test_password(
                main_archive, "", first_file=first_file, timeout_seconds=30,
            )
            if _skip_nopassword:
                logger.debug("Quick empty-password test failed — skipping no-password extract attempt")

        if _skip_nopassword:
            result = ExtractionResult(success=False, needs_password=True)
        else:
            result = await extractor.extract(
                main_archive,
                output_dir,
                target_extensions=target_exts,
                password=None,
                timeout_seconds=_extraction_timeout(ctx, main_archive),
            )

        if result.requires_password:
            # Pre-screen candidates with a fast test before committing to a full
            # extraction.  For ZipCrypto-encrypted ZIPs, a wrong password causes
            # 7z to write corrupt data for every file before reporting failure —
            # test_password catches wrong passwords in seconds with no disk writes.
            for pwd_candidate in passwords:
                job.password_attempts += 1
                logger.debug("Trying password candidate %d", pwd_candidate.id)

                # Quick pre-screen: skip obvious wrong passwords without extracting.
                if not await extractor.test_password(
                    main_archive, pwd_candidate.value,
                    first_file=first_file, timeout_seconds=30,
                ):
                    pwd_candidate.times_failed += 1
                    logger.debug("Password candidate %d failed pre-screen", pwd_candidate.id)
                    continue

                result = await extractor.extract(
                    main_archive,
                    output_dir,
                    target_extensions=target_exts,
                    password=pwd_candidate.value,
                    timeout_seconds=_extraction_timeout(ctx, main_archive),
                )

                if result.success:
                    job.used_password_id = pwd_candidate.id
                    pwd_candidate.times_succeeded += 1
                    logger.info("Password succeeded for %s", main_archive.name)
                    break
                elif result.wrong_password:
                    pwd_candidate.times_failed += 1
                else:
                    # Other error, stop trying passwords
                    break

        return result

    async def _get_password_candidates(
        self,
        ctx: PipelineContext,
        group: ArchiveGroup,
    ) -> list[PasswordCandidate]:
        """Get password candidates for an archive group."""
        # Get conversation ID from first part's message
        first_part = group.parts[0] if group.parts else None
        if not first_part or not first_part.artifact.attachment:
            return []

        message = first_part.artifact.attachment.message
        if not message:
            return []

        # Extract passwords from message context
        attachment_filename = first_part.artifact.attachment.filename

        new_candidates = await extract_passwords_from_context(
            ctx.session,
            message,
            ctx.config,
            archive_name=group.base_name,
            attachment_filename=attachment_filename,
        )

        # Get existing candidates for this conversation
        existing = (
            ctx.session.execute(
                select(PasswordCandidate)
                .where(
                    PasswordCandidate.conversation_id == message.conversation_id,
                    PasswordCandidate.times_failed < MAX_FAILED_ATTEMPTS,
                )
                .order_by(
                    PasswordCandidate.times_succeeded.desc(),
                    PasswordCandidate.confidence.desc(),
                )
            )
            .scalars()
            .all()
        )

        # Combine, deduplicate, and rank
        all_candidates = deduplicate_candidates(list(existing) + new_candidates)
        ranked = rank_passwords(all_candidates)

        return [r.candidate for r in ranked]

    def _has_sufficient_disk(self, ctx: PipelineContext) -> bool:
        """Check if there is enough free disk space for extraction."""
        try:
            usage = shutil.disk_usage(ctx.config.extracted_dir)
            free_mb = usage.free / (1024 * 1024)
            return free_mb >= ctx.config.extraction.min_free_disk_mb
        except Exception as e:
            # Unknown disk state (unmounted dir, stat failure) is NOT "enough
            # disk" — proceed only when we can verify headroom.
            logger.warning(
                "Disk usage check failed (%s) — treating as insufficient space",
                e,
            )
            return False

    _RECORD_BATCH = 500

    async def _record_outputs(
        self,
        ctx: PipelineContext,
        job: ExtractionJob,
        group: ArchiveGroup,
        extracted_files: list[Path],
        output_dir: Path,
    ) -> None:
        """Record extracted output files.

        Flushes every _RECORD_BATCH rows to keep session memory bounded and
        yields to the asyncio event loop so prefetch downloads can progress
        during large (60K+) archives.
        """
        # Get provenance info
        first_part = group.parts[0] if group.parts else None
        source_conv_id = None
        source_msg_id = None

        if first_part and first_part.artifact.attachment:
            msg = first_part.artifact.attachment.message
            if msg:
                source_conv_id = msg.conversation_id
                source_msg_id = msg.id

        for i, file_path in enumerate(extracted_files):
            sha256 = hashlib.sha256()  # noqa: S324
            file_size = 0
            with open(file_path, "rb") as f:
                while chunk := f.read(65536):
                    file_size += len(chunk)
                    sha256.update(chunk)
            file_hash = sha256.hexdigest()

            output = ExtractedOutput(
                job_id=job.id,
                output_path=str(file_path),
                output_filename=file_path.name,
                output_type=file_path.suffix.lower(),
                output_size=file_size,
                output_hash=file_hash,
                source_conversation_id=source_conv_id,
                source_message_id=source_msg_id,
            )
            ctx.session.add(output)

            if (i + 1) % self._RECORD_BATCH == 0:
                ctx.session.flush()
                ctx.session.expire_all()
                await asyncio.sleep(0)

            logger.debug("Recorded output: %s (%s)", file_path.name, file_hash[:8])
