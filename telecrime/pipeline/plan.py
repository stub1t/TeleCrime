"""Stage 3: Plan - create download jobs and group multi-part archives."""

import hashlib
import logging
from collections import defaultdict
from collections.abc import Sequence

from sqlalchemy import case, func, select
from sqlalchemy.orm import joinedload

from telecrime.grouping.patterns import GroupingResult, group_by_pattern
from telecrime.models import (
    ArchiveGroup,
    ArchiveGroupPart,
    DownloadArtifact,
    FileAttachment,
)
from telecrime.pipeline.orchestrator import PipelineContext, PipelineStage
from telecrime.states import DownloadStatus, GroupStatus

logger = logging.getLogger(__name__)


class PlanStage(PipelineStage):
    """Create download jobs and group multi-part archives."""

    name = "plan"

    async def run(self, ctx: PipelineContext) -> bool:
        """Run the plan stage."""
        logger.info("Starting download planning")

        # Fix any bad groups from old code before planning new downloads
        await self._fix_bad_groups(ctx)

        # Find archive candidates without download artifacts.
        # Eagerly load .message so conversation_id is available after the commit below.
        candidates = ctx.session.execute(
            select(FileAttachment)
            .where(
                FileAttachment.is_archive_candidate == True,
                ~FileAttachment.download_artifact.has(),
            )
            .options(joinedload(FileAttachment.message))
        ).scalars().all()
        # Release the read snapshot before doing any further work.
        ctx.session.commit()

        if not candidates:
            logger.info("No new archive candidates to plan")
            return True

        # Create download artifacts for each candidate; keep an in-memory map
        # so _create_or_update_group can look them up without N+1 DB queries.
        artifact_map: dict[int, DownloadArtifact] = {}
        for attachment in candidates:
            artifact = DownloadArtifact(
                attachment_id=attachment.id,
                status=DownloadStatus.PENDING,
            )
            ctx.session.add(artifact)
            artifact_map[attachment.id] = artifact

        ctx.session.flush()
        logger.info("Created %d download jobs", len(artifact_map))

        # Group multi-part archives
        await self._group_archives(ctx, candidates, artifact_map)

        ctx.session.commit()
        return True

    async def _fix_bad_groups(self, ctx: PipelineContext) -> None:
        """Split INCOMPLETE groups that incorrectly merged standalone archives.

        Old code grouped all same-named uploads from the same conversation
        into one multi-part group. This prevents them from ever being READY
        (they wait for all N parts to download). Fix: split each such group
        into individual standalone groups — one per attachment.

        A group is "bad" if it has >3 parts and none of its file attachments
        have an explicit part number (detected_part_number IS NULL for all).
        """
        # Find bad groups: INCOMPLETE, >3 parts, no explicit part numbers
        bad_group_ids = (
            ctx.session.execute(
                select(ArchiveGroup.id)
                .join(ArchiveGroupPart, ArchiveGroupPart.group_id == ArchiveGroup.id)
                .join(DownloadArtifact, DownloadArtifact.id == ArchiveGroupPart.artifact_id)
                .join(FileAttachment, FileAttachment.id == DownloadArtifact.attachment_id)
                .where(ArchiveGroup.status == GroupStatus.INCOMPLETE)
                .group_by(ArchiveGroup.id)
                .having(
                    func.count(ArchiveGroupPart.id) > 3,
                    func.sum(
                        case((FileAttachment.detected_part_number.isnot(None), 1), else_=0)
                    ) == 0,
                )
            )
            .scalars()
            .all()
        )
        # Release the read snapshot immediately — we only need the IDs going forward.
        ctx.session.commit()

        if not bad_group_ids:
            return

        logger.info(
            "Found %d bad INCOMPLETE groups to split into standalone archives",
            len(bad_group_ids),
        )

        fixed = 0
        for group_id in bad_group_ids:
            group = (
                ctx.session.execute(
                    select(ArchiveGroup)
                    .where(ArchiveGroup.id == group_id)
                    .options(
                        joinedload(ArchiveGroup.parts)
                        .joinedload(ArchiveGroupPart.artifact)
                        .joinedload(DownloadArtifact.attachment)
                    )
                )
                .unique()
                .scalar_one_or_none()
            )
            if not group:
                continue

            for part in list(group.parts):
                artifact = part.artifact
                attachment = artifact.attachment if artifact else None
                if not attachment:
                    ctx.session.delete(part)
                    continue

                filename = attachment.filename or f"file_{attachment.id}"
                stable_key = attachment.platform_file_unique_id or f"{attachment.platform_file_id}:{attachment.size}"
                fingerprint = hashlib.sha256(stable_key.encode()).hexdigest()[:16]

                # Skip if a standalone group already exists for this attachment
                existing = ctx.session.execute(
                    select(ArchiveGroup).where(ArchiveGroup.fingerprint == fingerprint)
                ).scalar_one_or_none()

                if not existing:
                    is_ready = artifact.status == DownloadStatus.COMPLETED
                    new_group = ArchiveGroup(
                        fingerprint=fingerprint,
                        base_name=filename,
                        expected_part_count=1,
                        detected_part_count=1,
                        status=GroupStatus.READY if is_ready else GroupStatus.INCOMPLETE,
                    )
                    ctx.session.add(new_group)
                    ctx.session.flush()
                    target_group_id = new_group.id
                else:
                    target_group_id = existing.id

                # Re-link part to the new standalone group
                part.group_id = target_group_id
                part.part_index = 0

            # Mark old bad group as cleaned so it's ignored
            group.status = GroupStatus.CLEANED
            fixed += 1
            ctx.session.flush()

        ctx.session.commit()
        if fixed:
            logger.info(
                "Split %d bad archive groups into standalone archives", fixed
            )

    async def _group_archives(
        self,
        ctx: PipelineContext,
        candidates: Sequence[FileAttachment],
        artifact_map: dict[int, DownloadArtifact],
    ) -> None:
        """Group multi-part archives together."""
        # Group by conversation and base name
        by_conversation: dict[int, list[FileAttachment]] = defaultdict(list)
        for attachment in candidates:
            by_conversation[attachment.message.conversation_id].append(attachment)

        groups_created = 0

        for conv_id, attachments in by_conversation.items():
            # Use pattern matching to group parts
            grouping_results = group_by_pattern(attachments)

            for result in grouping_results:
                group = await self._create_or_update_group(ctx, result, artifact_map)
                if group:
                    groups_created += 1

        logger.info("Created/updated %d archive groups", groups_created)

    async def _create_or_update_group(
        self,
        ctx: PipelineContext,
        result: GroupingResult,
        artifact_map: dict[int, DownloadArtifact],
    ) -> ArchiveGroup | None:
        """Create or update an archive group from grouping result."""
        if not result.attachments:
            return None

        # Compute group fingerprint from Telegram file identity so that the
        # same archive posted multiple times creates only one group.
        # Use platform_file_unique_id (= "<doc_id>_<access_hash>"), falling
        # back to "<platform_file_id>:<size>" when that is unavailable.
        def _stable_key(a) -> str:
            if a.platform_file_unique_id:
                return a.platform_file_unique_id
            return f"{a.platform_file_id}:{a.size}"

        # Deduplicate attachments by Telegram file identity before fingerprinting.
        # group_by_pattern may include the same physical file multiple times when a
        # multi-part archive is re-posted across messages (e.g., part1.rar appears
        # in message A and message B with the same platform_file_unique_id).
        seen_keys: set[str] = set()
        unique_attachments: list = []
        for a in result.attachments:
            k = _stable_key(a)
            if k not in seen_keys:
                seen_keys.add(k)
                unique_attachments.append(a)

        fingerprint_parts = sorted(_stable_key(a) for a in unique_attachments)
        fingerprint_data = "|".join(fingerprint_parts)
        fingerprint = hashlib.sha256(fingerprint_data.encode()).hexdigest()[:16]

        # Check if group already exists
        existing = ctx.session.execute(
            select(ArchiveGroup).where(ArchiveGroup.fingerprint == fingerprint)
        ).scalar_one_or_none()

        if existing:
            # The same physical file(s) were posted again (repost across
            # channels). Leaving the new artifacts unlinked would orphan them:
            # _next_pending_artifact only selects artifacts WITH a group, so
            # they'd never download — even when this group's original copy is
            # gone (deleted message → FAILED_TERMINAL) and the new post is
            # the only available source.
            linked_any = False
            for idx, attachment in enumerate(unique_attachments):
                artifact = artifact_map.get(attachment.id)
                if artifact is None:
                    continue
                already_linked = ctx.session.execute(
                    select(ArchiveGroupPart).where(
                        ArchiveGroupPart.artifact_id == artifact.id
                    )
                ).scalar_one_or_none()
                if already_linked:
                    continue
                part = ArchiveGroupPart(
                    group_id=existing.id,
                    artifact_id=artifact.id,
                    part_index=result.part_numbers.get(attachment.id, idx)
                    if result.part_numbers
                    else idx,
                    role="part" if len(unique_attachments) > 1 else "main",
                )
                ctx.session.add(part)
                linked_any = True
            # A new source for a permanently-failed group can rescue it:
            # revert to INCOMPLETE so the acquire stage downloads the new
            # artifact and (re-)evaluates the group.
            if linked_any and existing.status == GroupStatus.FAILED_TERMINAL:
                logger.info(
                    "Reviving FAILED_TERMINAL group %s — reposted source linked",
                    existing.base_name,
                )
                existing.status = GroupStatus.INCOMPLETE
            return existing

        # Create new group
        group = ArchiveGroup(
            fingerprint=fingerprint,
            base_name=result.base_name,
            expected_part_count=result.expected_parts or len(unique_attachments),
            detected_part_count=len(unique_attachments),
            status=GroupStatus.INCOMPLETE,
        )
        ctx.session.add(group)
        ctx.session.flush()

        # Link artifacts to group
        for idx, attachment in enumerate(unique_attachments):
            artifact = artifact_map.get(attachment.id)

            if artifact:
                part = ArchiveGroupPart(
                    group_id=group.id,
                    artifact_id=artifact.id,
                    part_index=result.part_numbers.get(attachment.id, idx) if result.part_numbers else idx,
                    role="part" if len(unique_attachments) > 1 else "main",
                )
                ctx.session.add(part)

        logger.debug(
            "Created archive group: %s with %d parts",
            result.base_name,
            len(unique_attachments),
        )

        return group
