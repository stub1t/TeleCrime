"""Stage 2: Discover - identify archive candidates from file attachments."""

import logging
import os.path
import re

from sqlalchemy import select

from telecrime.models import FileAttachment
from telecrime.pipeline.orchestrator import PipelineContext, PipelineStage
from telecrime.stealer.patterns import is_credential_file

logger = logging.getLogger(__name__)

# Archive extensions and their types
ARCHIVE_EXTENSIONS = {
    # Standard archives
    ".zip": "zip",
    ".7z": "7z",
    ".rar": "rar",
    ".tar": "tar",
    ".gz": "gzip",
    ".tgz": "tar",
    ".bz2": "bzip2",
    ".xz": "xz",
    ".lzma": "lzma",
    # Split archive patterns
    ".z01": "zip",
    ".z02": "zip",
    ".001": "7z",
    ".002": "7z",
    ".r00": "rar",
    ".r01": "rar",
    ".part1.rar": "rar",
    ".part01.rar": "rar",
    ".part001.rar": "rar",
}

# MIME types that indicate archives
ARCHIVE_MIME_TYPES = {
    "application/zip",
    "application/x-zip-compressed",
    "application/x-7z-compressed",
    "application/x-rar-compressed",
    "application/vnd.rar",
    "application/x-tar",
    "application/gzip",
    "application/x-gzip",
    "application/x-bzip2",
    "application/x-xz",
    "application/x-lzma",
    "application/octet-stream",  # Often used for archives
}

# Regex patterns for split archive detection
SPLIT_PATTERNS = [
    # .part1.rar, .part1.zip, .part01.7z
    (r"^(.+?)\.part(\d+)\.(zip|rar|7z)$", "split"),
    # .r00, .r01, .r02
    (r"^(.+?)\.r(\d{2,})$", "rar"),
    # .7z.001, .7z.002
    (r"^(.+?\.7z)\.(\d{3})$", "7z"),
    # .zip.001, .zip.002
    (r"^(.+?\.zip)\.(\d{3})$", "zip"),
    # .z01, .z02
    (r"^(.+?)\.z(\d{2})$", "zip"),
    # Generic .001, .002
    (r"^(.+?)\.(\d{3})$", "split"),
]

# Extensions to REJECT - executables, images, videos, etc.
# We only want archives and text-based credential files
REJECTED_EXTENSIONS = {
    # Executables
    ".exe", ".msi", ".dll", ".bat", ".cmd", ".com", ".scr", ".pif",
    ".vbs", ".vbe", ".js", ".jse", ".wsf", ".wsh", ".ps1", ".psm1",
    ".apk", ".app", ".dmg", ".pkg", ".deb", ".rpm", ".bin", ".run",
    # Images
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".ico", ".svg", ".webp",
    ".tif", ".tiff", ".psd", ".raw", ".heif", ".heic",
    # Videos
    ".mp4", ".avi", ".mkv", ".mov", ".wmv", ".flv", ".webm", ".m4v",
    ".mpg", ".mpeg", ".3gp", ".3g2",
    # Audio
    ".mp3", ".wav", ".flac", ".aac", ".ogg", ".wma", ".m4a",
    # Documents we don't need
    ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".odt",
    # Other binary files
    ".iso", ".img", ".vmdk", ".vhd", ".qcow2",
    ".torrent", ".nfo",
}

# MIME types to reject
REJECTED_MIME_TYPES = {
    # Executables
    "application/x-msdownload",
    "application/x-executable",
    "application/x-dosexec",
    "application/vnd.android.package-archive",
    # Images
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/webp",
    "image/bmp",
    "image/svg+xml",
    # Videos
    "video/mp4",
    "video/x-msvideo",
    "video/x-matroska",
    "video/quicktime",
    "video/webm",
    # Audio
    "audio/mpeg",
    "audio/wav",
    "audio/flac",
    "audio/ogg",
}


class DiscoverStage(PipelineStage):
    """Identify archive candidates from file attachments."""

    name = "discover"

    async def run(self, ctx: PipelineContext) -> bool:
        """Run the discover stage."""
        logger.info("Starting archive discovery")

        # Find all unprocessed attachments
        attachments = ctx.session.execute(
            select(FileAttachment).where(
                FileAttachment.is_archive_candidate == False,
                FileAttachment.archive_type == None,
            )
        ).scalars().all()

        candidates_found = 0

        for attachment in attachments:
            is_archive, archive_type, part_info = self._classify_attachment(attachment)

            if is_archive:
                attachment.is_archive_candidate = True
                attachment.archive_type = archive_type

                if part_info:
                    attachment.detected_base_name = part_info[0]
                    attachment.detected_part_number = part_info[1]

                candidates_found += 1
                logger.debug(
                    "Found archive candidate: %s (type=%s, part=%s)",
                    attachment.filename,
                    archive_type,
                    part_info[1] if part_info else None,
                )
            else:
                # Mark as checked (non-archive) so it isn't re-scanned next run.
                # Empty string distinguishes "checked, not an archive" from None
                # ("not yet checked"), which the query filters on.
                attachment.archive_type = ""

        ctx.session.commit()
        logger.info("Discovered %d archive candidates from %d attachments",
                   candidates_found, len(attachments))

        return True

    def _classify_attachment(
        self, attachment: FileAttachment
    ) -> tuple[bool, str | None, tuple[str, int] | None]:
        """Classify an attachment as archive or not.

        Returns:
            Tuple of (is_archive, archive_type, (base_name, part_number) or None)
        """
        filename = attachment.filename or ""
        mime_type = attachment.mime_type or ""
        filename_lower = filename.lower()

        # FIRST: Reject unwanted file types (executables, images, videos, etc.)
        # We only want archives that likely contain stealer logs
        _, file_ext = os.path.splitext(filename_lower)
        if file_ext in REJECTED_EXTENSIONS:
            logger.debug("Rejecting file with unwanted extension: %s", filename)
            return False, None, None

        if mime_type in REJECTED_MIME_TYPES:
            logger.debug("Rejecting file with unwanted MIME type: %s (%s)", filename, mime_type)
            return False, None, None

        # Check for split archive patterns first
        for pattern, archive_type in SPLIT_PATTERNS:
            match = re.match(pattern, filename_lower)
            if match:
                base_name = match.group(1)
                part_number = int(match.group(2))
                # For the generic .partN pattern the real type is in the
                # filename extension (zip/rar/7z), not the fixed type tag.
                if archive_type == "split":
                    _m = re.search(r"\.(zip|rar|7z)$", filename_lower)
                    archive_type = _m.group(1) if _m else "unknown"
                return True, archive_type, (base_name, part_number)

        # Check extension
        for ext, archive_type in ARCHIVE_EXTENSIONS.items():
            if filename_lower.endswith(ext):
                return True, archive_type, None

        # Direct credential .txt files (ULP/combo lists, password dumps)
        # Processed by extract stage via hardlink — no 7z needed.
        if filename_lower.endswith(".txt") and is_credential_file(filename):
            return True, "txt", None

        # Check MIME type
        if mime_type in ARCHIVE_MIME_TYPES:
            # Try to infer type from extension anyway
            for ext, archive_type in ARCHIVE_EXTENSIONS.items():
                if filename_lower.endswith(ext):
                    return True, archive_type, None
            # Generic archive based on MIME. application/octet-stream is the
            # catch-all MIME type — only trust it for reasonably sized files
            # (1MB-500MB); small octet-stream files are usually not archives.
            if mime_type == "application/octet-stream":
                if attachment.size and 1 * 1024 * 1024 < attachment.size < 500 * 1024 * 1024:
                    return True, "unknown", None
                return False, None, None
            return True, "unknown", None

        # Size heuristic: large files without extension might be archives
        # But be more conservative - only if it's reasonably sized (not huge media files)
        if attachment.size and 1 * 1024 * 1024 < attachment.size < 500 * 1024 * 1024:  # 1MB-500MB
            if mime_type == "application/octet-stream":
                return True, "unknown", None

        return False, None, None
