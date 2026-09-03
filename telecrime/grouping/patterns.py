"""Multi-part archive grouping by filename patterns."""

import re
from collections import defaultdict
from dataclasses import dataclass, field

from telecrime.grouping.normalize import normalize_group_key
from telecrime.models import FileAttachment


@dataclass
class GroupingResult:
    """Result of grouping attachments into an archive group."""

    base_name: str
    attachments: list[FileAttachment]
    expected_parts: int | None = None
    part_numbers: dict[int, int] = field(default_factory=dict)  # attachment_id -> part_number
    confidence: float = 1.0


# Patterns for detecting split archives
# Each pattern: (regex, group_index_for_base, group_index_for_part_num)
SPLIT_PATTERNS = [
    # .part1.rar, .part1.zip, .part01.7z — the dominant split format for
    # Telegram reposts. Previously only .rar was matched, so .partN.zip/.7z
    # archives were planned as N standalone groups and every part failed
    # extraction (incomplete archive) — permanent data loss.
    (re.compile(r"^(.+?)\.part(\d+)\.(zip|rar|7z)$", re.IGNORECASE), 1, 2),
    # .r00, .r01, .r02 (RAR old style)
    (re.compile(r"^(.+?)\.r(\d{2,})$", re.IGNORECASE), 1, 2),
    # archive_1of3.zip, archive_2of3.zip — must come BEFORE the generic
    # .rar/.zip match below, otherwise part numbers are never extracted.
    (re.compile(r"^(.+?)[-_]?(\d+)of(\d+)\.(zip|rar|7z)$", re.IGNORECASE), 1, 2),
    # archive.rar (main) - matches with .r00 series
    (re.compile(r"^(.+?)\.rar$", re.IGNORECASE), 1, None),
    # .7z.001, .7z.002
    (re.compile(r"^(.+?)\.7z\.(\d{3})$", re.IGNORECASE), 1, 2),
    # .zip.001, .zip.002
    (re.compile(r"^(.+?)\.zip\.(\d{3})$", re.IGNORECASE), 1, 2),
    # .z01, .z02 (ZIP split)
    (re.compile(r"^(.+?)\.z(\d{2})$", re.IGNORECASE), 1, 2),
    # Generic .001, .002, .003
    (re.compile(r"^(.+?)\.(\d{3})$"), 1, 2),
    # volume1.zip, volume2.zip
    (re.compile(r"^(.+?)[-_]?(?:vol|volume)[-_]?(\d+)\.(zip|rar|7z)$", re.IGNORECASE), 1, 2),
]


def extract_base_and_part(filename: str) -> tuple[str | None, int | None, int | None]:
    """Extract base name and part number from filename.

    Returns:
        Tuple of (base_name, part_number, expected_total) or (None, None, None)
    """
    if not filename:
        return None, None, None

    for pattern, base_group, part_group in SPLIT_PATTERNS:
        match = pattern.match(filename)
        if match:
            base_name = match.group(base_group)
            part_number = None
            expected_total = None

            if part_group is not None:
                try:
                    part_number = int(match.group(part_group))
                except (IndexError, ValueError):
                    pass

            # Check for "XofY" pattern
            if "of" in filename.lower():
                of_match = re.search(r"(\d+)of(\d+)", filename, re.IGNORECASE)
                if of_match:
                    expected_total = int(of_match.group(2))

            return base_name, part_number, expected_total

    return None, None, None


def group_by_pattern(attachments: list[FileAttachment]) -> list[GroupingResult]:
    """Group attachments by filename patterns.

    Only files with explicit split-archive part numbers (e.g. .part1.rar,
    .7z.001) are grouped as multi-part.  Files that share a base name but
    have **no** part indicator are treated as independent archives — this
    handles the common Telegram pattern of the same archive name being
    posted in many separate messages.

    When multiple attachments come from the **same message** and none have
    explicit part numbers, they are grouped together (the uploader likely
    intended them as a set).

    Args:
        attachments: List of FileAttachment objects to group

    Returns:
        List of GroupingResult objects
    """
    # Group by base name
    groups: dict[str, list[tuple[FileAttachment, int | None]]] = defaultdict(list)
    standalone: list[FileAttachment] = []

    for attachment in attachments:
        filename = attachment.filename or ""
        base_name, part_number, _ = extract_base_and_part(filename)

        if base_name:
            # Normalize base name for grouping
            normalized_base = normalize_group_key(base_name)
            groups[normalized_base].append((attachment, part_number))

            # Store detected info on attachment if not already set
            if attachment.detected_base_name is None:
                attachment.detected_base_name = base_name
            if attachment.detected_part_number is None and part_number is not None:
                attachment.detected_part_number = part_number
        else:
            standalone.append(attachment)

    results: list[GroupingResult] = []

    # Process groups
    for base_name, parts in groups.items():
        if len(parts) <= 1:
            # Single file that matched a pattern - treat as standalone
            standalone.append(parts[0][0])
            continue

        # Check if any file in the group has an explicit part number
        has_explicit_parts = any(p[1] is not None for p in parts)

        if has_explicit_parts:
            # Genuine multi-part archive (e.g. .part1.rar, .part2.rar)
            explicit_parts = [p for p in parts if p[1] is not None]
            no_part_files = [p[0] for p in parts if p[1] is None]

            # If the split uses .partN format, companion bare .rar files are
            # standalone archives that happen to share the base name.
            # Only include no-part-number files when the split uses the OLD
            # RAR format (.r00, .r01, ...) where archive.rar is the companion.
            uses_part_n = any(
                re.search(r"\.part\d+\b", p[0].filename or "", re.IGNORECASE)
                for p in explicit_parts
            )
            if uses_part_n:
                # Bare .rar files (e.g. Archive.rar mixed with Archive.part1.rar)
                # are standalone uploads, not part of this multi-part set.
                standalone.extend(no_part_files)
            else:
                # Old-style RAR: companion .rar accompanies .r00/.r01 series.
                explicit_parts = parts  # include all

            attachments_list = [p[0] for p in explicit_parts]
            if uses_part_n:
                part_numbers = {
                    p[0].id: p[1] if p[1] is not None else idx
                    for idx, p in enumerate(sorted(explicit_parts, key=lambda x: x[1] or 0))
                }
            else:
                # Old-style RAR: the bare .rar is the header-bearing first
                # volume (index 0); .r00/.r01/.r02... follow as 1,2,3.
                # Sorting by `x[1] or 0` would tie the bare .rar with .r00,
                # and assigning the .rar an enumerate index could collide with
                # .r00's explicit 0 → two parts with part_index 0, breaking
                # 7z's volume order and failing extraction.
                part_numbers = {}
                for p in sorted(
                    explicit_parts,
                    key=lambda x: x[1] if x[1] is not None else -1,
                ):
                    part_numbers[p[0].id] = 0 if p[1] is None else p[1] + 1

            # Infer expected parts from the range of part numbers.
            # Most formats are 1-indexed (.part1, .001, .z01) so the count
            # is max-min+1.  0-indexed formats (.r00) also work correctly.
            part_nums = [p[1] for p in explicit_parts if p[1] is not None]
            expected_parts = max(part_nums) - min(part_nums) + 1
            # Old-style RAR includes the bare .rar as an extra part before .r00.
            if not uses_part_n and any(p[1] is None for p in explicit_parts):
                expected_parts += 1

            results.append(GroupingResult(
                base_name=attachments_list[0].detected_base_name or attachments_list[0].filename or base_name,
                attachments=attachments_list,
                expected_parts=expected_parts,
                part_numbers=part_numbers,
                confidence=0.9,
            ))
        else:
            # No explicit part numbers — group by message instead.
            # Files in the same message belong together; files in
            # different messages are independent archives.
            by_message: dict[int | None, list[FileAttachment]] = defaultdict(list)
            for att, _ in parts:
                msg_id = att.message_id if hasattr(att, "message_id") else None
                by_message[msg_id].append(att)

            for msg_id, msg_parts in by_message.items():
                if len(msg_parts) > 1:
                    # Multiple files in the same message → group them
                    results.append(GroupingResult(
                        base_name=msg_parts[0].detected_base_name or msg_parts[0].filename or base_name,
                        attachments=msg_parts,
                        expected_parts=len(msg_parts),
                        part_numbers={a.id: idx for idx, a in enumerate(msg_parts)},
                        confidence=0.8,
                    ))
                else:
                    # Single file per message → standalone
                    standalone.append(msg_parts[0])

    # Add standalone files as single-file groups
    for attachment in standalone:
        # Split-series end volume: a bare "file.zip" is the FINAL volume of a
        # "file.z01/z02/..." series (and "file.7z" ends a ".001/.002"
        # series). Left standalone, 7z CANNOT_OPEN it and the group is
        # deleted. Attach it to the matching series when one exists — the
        # series itself must have been found in the explicit-parts branch.
        _linked = False
        _series_key: str | None = None
        _filename = attachment.filename or ""
        _suffix = _filename.lower()
        if _suffix.endswith(".zip"):
            _series_key = _filename[:-4]
        elif _suffix.endswith(".7z"):
            _series_key = _filename[:-3]
        if _series_key:
            for existing in results:
                if (
                    existing.attachments
                    and (existing.attachments[0].filename or "").lower().startswith(
                        _series_key.lower()
                    )
                    and any(
                        (a.filename or "").lower().endswith((".z01", ".z02", ".001", ".002"))
                        for a in existing.attachments
                    )
                ):
                    existing.attachments.append(attachment)
                    existing.expected_parts = (existing.expected_parts or 0) + 1
                    existing.part_numbers[attachment.id] = len(existing.attachments) - 1
                    _linked = True
                    break
        if _linked:
            continue

        base_name = attachment.filename or f"file_{attachment.id}"
        results.append(GroupingResult(
            base_name=base_name,
            attachments=[attachment],
            expected_parts=1,
            part_numbers={attachment.id: 0},
            confidence=1.0,
        ))

    return results


