"""Abstract interface for archive extractors."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path


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
    ) -> list[str]:
        """List contents of an archive without extracting.

        Args:
            archive_path: Path to the archive file
            password: Optional password for encrypted archives

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
