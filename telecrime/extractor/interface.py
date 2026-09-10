"""Abstract interface for archive extractors."""

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

# Bounded default for archive listing. Listing a huge archive can legitimately
# take minutes, but an unbounded listing (wedged 7z/unrar) hangs the pipeline.
DEFAULT_LIST_TIMEOUT_SECONDS: float = 300.0

# How long to wait for a killed child to be reaped before abandoning it. The
# OS still reaps the SIGKILLed process; this only bounds the await.
_KILL_REAP_TIMEOUT_SECONDS: float = 5.0


@dataclass
class ExtractionResult:
    """Result of an extraction attempt."""

    success: bool
    extracted_files: list[Path] = field(default_factory=list)
    error_code: str | None = None
    error_message: str | None = None
    needs_password: bool = False
    wrong_password: bool = False

    @property
    def requires_password(self) -> bool:
        """True when extraction failed because a password is needed or was wrong."""
        return self.needs_password or self.wrong_password


class ArchiveExtractor(ABC):
    """Abstract base class for archive extractors."""

    @staticmethod
    async def _kill_and_reap(
        process: asyncio.subprocess.Process,
        timeout: float = _KILL_REAP_TIMEOUT_SECONDS,
    ) -> None:
        """Kill a still-running child and reap it, bounded.

        Safe to call on every exit path (success, timeout, cancellation): if
        the process already exited this is a no-op. The bounded wait keeps a
        stuck child from hanging the caller while still making sure SIGKILL
        was delivered, so no orphan keeps writing into output_dir after the
        caller rmtree()s and re-extracts.
        """
        if process.returncode is not None:
            return
        try:
            process.kill()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(process.wait(), timeout=timeout)
        except TimeoutError:
            logger.warning(
                "Extractor child pid=%s did not exit within %.1fs of kill; abandoning reap",
                process.pid,
                timeout,
            )

    @abstractmethod
    async def extract(
        self,
        archive_path: Path,
        output_dir: Path,
        password: str | None = None,
        target_extensions: list[str] | None = None,
        timeout_seconds: int | None = None,
    ) -> ExtractionResult:
        """Extract contents from an archive.

        Args:
            archive_path: Path to the archive file (or first part for split archives)
            output_dir: Directory to extract files into
            password: Optional password for encrypted archives
            target_extensions: If provided, only extract files with these extensions

        Returns:
            ExtractionResult with status and extracted file paths
        """
        ...

    @abstractmethod
    async def list_contents(
        self,
        archive_path: Path,
        password: str | None = None,
        timeout_seconds: float | None = DEFAULT_LIST_TIMEOUT_SECONDS,
    ) -> list[str]:
        """List contents of an archive without extracting.

        Args:
            archive_path: Path to the archive file
            password: Optional password for encrypted archives
            timeout_seconds: Max seconds to wait for the listing. None disables
                the bound (legacy behavior); the default is a safe constant.

        Returns:
            List of file paths within the archive
        """
        ...

    async def test_password(
        self,
        archive_path: Path,
        password: str,
        first_file: str | None = None,
        timeout_seconds: int = 30,
    ) -> bool:
        """Quickly verify a password without full extraction.

        Default implementation falls through to full extraction (always returns True).
        Override in subclasses that support fast password testing.
        """
        return True
