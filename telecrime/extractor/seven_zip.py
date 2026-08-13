"""7-Zip command-line extractor wrapper."""

import asyncio
import logging
import re
from pathlib import Path

from telecrime.extractor.interface import ArchiveExtractor, ExtractionResult

logger = logging.getLogger(__name__)


class SevenZipExtractor(ArchiveExtractor):
    """Wrapper for 7-Zip command-line tool."""

    def __init__(self, executable: str = "7z"):
        """Initialize extractor.

        Args:
            executable: Path to 7z executable (default: "7z" in PATH)
        """
        self.executable = executable

    async def extract(
        self,
        archive_path: Path,
        output_dir: Path,
        password: str | None = None,
        target_extensions: list[str] | None = None,
        timeout_seconds: int | None = None,
    ) -> ExtractionResult:
        """Extract contents from an archive using 7z."""
        if not archive_path.exists():
            return ExtractionResult(
                success=False,
                error_code="FILE_NOT_FOUND",
                error_message=f"Archive not found: {archive_path}",
            )

        output_dir.mkdir(parents=True, exist_ok=True)

        # Build command
        cmd = [
            self.executable,
            "x",  # Extract with full paths
            "-y",  # Yes to all prompts
            "-mmt=on",  # Multi-threaded decompression (LZMA/LZMA2/7z formats)
            f"-o{output_dir}",  # Output directory
        ]

        if password:
            cmd.append(f"-p{password}")
        else:
            cmd.append("-p-")  # No password (will fail if needed)

        # Add file filters for target extensions
        if target_extensions:
            for ext in target_extensions:
                ext_clean = ext.lstrip(".")
                cmd.append(f"-ir!*.{ext_clean}")

        cmd.append(str(archive_path))

        # Run extraction
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            try:
                if timeout_seconds:
                    stdout, stderr = await asyncio.wait_for(
                        process.communicate(), timeout=timeout_seconds
                    )
                else:
                    stdout, stderr = await process.communicate()
            except TimeoutError:
                process.kill()
                return ExtractionResult(
                    success=False,
                    error_code="TIMEOUT",
                    error_message=f"Extraction timed out after {timeout_seconds}s",
                )
            stdout_text = stdout.decode("utf-8", errors="replace")
            stderr_text = stderr.decode("utf-8", errors="replace")

            return self._parse_result(
                process.returncode or 0,
                stdout_text,
                stderr_text,
                output_dir,
                target_extensions,
            )

        except FileNotFoundError:
            return ExtractionResult(
                success=False,
                error_code="EXTRACTOR_NOT_FOUND",
                error_message=f"7z executable not found: {self.executable}",
            )
        except Exception as e:
            return ExtractionResult(
                success=False,
                error_code="EXTRACTION_ERROR",
                error_message=str(e),
            )

    def _parse_result(
        self,
        return_code: int,
        stdout: str,
        stderr: str,
        output_dir: Path,
        target_extensions: list[str] | None,
    ) -> ExtractionResult:
        """Parse 7z output and determine result."""
        combined_output = stdout + stderr

        # Check for password errors
        if return_code != 0:
            if "Wrong password" in combined_output:
                return ExtractionResult(
                    success=False,
                    error_code="WRONG_PASSWORD",
                    error_message="Wrong password",
                    wrong_password=True,
                )

            # "Data Error" in encrypted files means wrong password;
            # without encryption context it means corruption
            if "Data Error" in combined_output:
                is_encrypted = (
                    "encrypted" in combined_output.lower() or "password" in combined_output.lower()
                )
                if is_encrypted:
                    return ExtractionResult(
                        success=False,
                        error_code="WRONG_PASSWORD",
                        error_message="Wrong password (data error in encrypted file)",
                        wrong_password=True,
                    )
                else:
                    return ExtractionResult(
                        success=False,
                        error_code="CORRUPTED",
                        error_message="Archive is corrupted (data error)",
                    )

            if "Enter password" in combined_output or "password" in combined_output.lower():
                return ExtractionResult(
                    success=False,
                    error_code="PASSWORD_REQUIRED",
                    error_message="Archive requires password",
                    needs_password=True,
                )

            if "Cannot open" in combined_output:
                return ExtractionResult(
                    success=False,
                    error_code="CANNOT_OPEN",
                    error_message="Cannot open archive file",
                )

            if "Unsupported" in combined_output:
                return ExtractionResult(
                    success=False,
                    error_code="UNSUPPORTED_FORMAT",
                    error_message="Unsupported archive format",
                )

            return ExtractionResult(
                success=False,
                error_code=f"EXIT_{return_code}",
                error_message=combined_output[:500],
            )

        # Success - find extracted files
        extracted_files = self._find_extracted_files(output_dir, target_extensions)

        if not extracted_files:
            # Check if extraction succeeded but no matching files
            if target_extensions:
                return ExtractionResult(
                    success=True,
                    extracted_files=[],
                    error_message="No files matching target extensions",
                )

        return ExtractionResult(
            success=True,
            extracted_files=extracted_files,
        )

    @staticmethod
    def _normalize_extensions(extensions: list[str]) -> frozenset[str]:
        """Return a frozenset of lowercase, dot-stripped extension strings."""
        return frozenset(e.lower().lstrip(".") for e in extensions)

    def _find_extracted_files(
        self,
        output_dir: Path,
        target_extensions: list[str] | None,
    ) -> list[Path]:
        """Find all extracted files in output directory."""
        if target_extensions:
            files: list[Path] = []
            for ext in self._normalize_extensions(target_extensions):
                files.extend(f for f in output_dir.rglob(f"*.{ext}") if f.is_file() and f.stat().st_size > 0)
            return files
        return [f for f in output_dir.rglob("*") if f.is_file() and f.stat().st_size > 0]

    async def list_contents(
        self,
        archive_path: Path,
        password: str | None = None,
    ) -> list[str]:
        """List contents of an archive."""
        cmd = [self.executable, "l", "-slt"]  # List with technical info

        if password:
            cmd.append(f"-p{password}")
        else:
            cmd.append("-p-")

        cmd.append(str(archive_path))

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                return []

            # Parse file list from output.
            # 7z -slt output begins with the archive's own metadata block:
            #   --
            #   Path = /full/path/to/archive.zip   ← archive itself, NOT a member
            #   Type = zip
            #   ...
            # Then each member is in its own section separated by "----------".
            # We skip any Path entry that equals the archive's own path to avoid
            # treating it as an unsafe member.
            files = []
            stdout_text = stdout.decode("utf-8", errors="replace")
            archive_path_str = str(archive_path)

            for match in re.finditer(r"^Path = (.+)$", stdout_text, re.MULTILINE):
                path = match.group(1).strip()
                if path and path != archive_path_str and not path.endswith("/"):
                    files.append(path)

            return files

        except Exception as e:
            logger.warning("Failed to list archive contents: %s", e)
            return []

    async def test_password(
        self,
        archive_path: Path,
        password: str,
        first_file: str | None = None,
        timeout_seconds: int = 30,
    ) -> bool:
        """Quickly verify a password by testing a single member of the archive.

        For ZipCrypto and AES-encrypted archives, testing just one member is
        enough to confirm whether the password is correct. This avoids the
        cost of a full extraction attempt (which writes corrupt files to disk
        for wrong passwords before detecting the CRC mismatch at the end).

        Args:
            archive_path: Path to the archive.
            password: Password candidate to test.
            first_file: A known member filename to test against. If None,
                        the first file in the archive is tested.
            timeout_seconds: Max seconds to wait for the test.

        Returns True if the password appears valid, False otherwise.
        """
        # Use 7z t (test) on a specific file if we know one, otherwise the whole archive.
        # Limiting to one member keeps this fast: wrong passwords are detected on the
        # very first CRC/header check (milliseconds for 7z format, a few seconds for ZIP).
        cmd = [self.executable, "t", f"-p{password}", str(archive_path)]
        if first_file:
            # Use the full relative path so 7z tests exactly one member.
            # A basename-only or wildcard filter (e.g. "*/file.txt") can match
            # zero files when the archive has nested directories, causing 7z to
            # report success for any password.
            cmd.append(first_file)
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                _, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout_seconds)
            except TimeoutError:
                process.kill()
                await asyncio.wait({asyncio.create_task(process.wait())}, timeout=5.0)
                # Timed out but no immediate failure — password likely valid
                return True
            stderr_text = stderr.decode("utf-8", errors="replace")
            if process.returncode != 0:
                if "Wrong password" in stderr_text or "Data Error" in stderr_text:
                    return False
                # Other errors (format, missing file filter match, etc.) — don't block
                return True
            return True
        except Exception:
            return True  # On error, fall through to full extraction

    async def find_matching_files(
        self,
        archive_path: Path,
        target_extensions: list[str],
        password: str | None = None,
    ) -> list[str]:
        """Find files matching target extensions in archive (without extracting).

        This allows checking if an archive contains files worth extracting
        before actually extracting them - useful for nested folder structures.

        Args:
            archive_path: Path to archive
            target_extensions: Extensions to look for (e.g., [".epub", ".pdf"])
            password: Optional password

        Returns:
            List of matching file paths inside the archive
        """
        all_files = await self.list_contents(archive_path, password)
        normalized_exts = self._normalize_extensions(target_extensions)
        return [f for f in all_files if Path(f).suffix.lower().lstrip(".") in normalized_exts]

    async def has_matching_files(
        self,
        archive_path: Path,
        target_extensions: list[str],
        password: str | None = None,
    ) -> bool:
        """Check if archive contains any files matching target extensions.

        Quick check to determine if extraction is worthwhile.
        """
        matching = await self.find_matching_files(archive_path, target_extensions, password)
        return len(matching) > 0
