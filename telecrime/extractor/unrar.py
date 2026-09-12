"""UnRAR command-line extractor wrapper.

Fallback for RAR5 archives that 7z cannot handle (Unsupported Method).
Uses the unrar binary from Alexander Roshal which supports all RAR formats.
"""

import asyncio
import itertools
import logging
import shutil
from pathlib import Path

from telecrime.extractor.interface import (
    DEFAULT_LIST_TIMEOUT_SECONDS,
    ArchiveExtractor,
    ExtractionResult,
)

logger = logging.getLogger(__name__)


def _extension_case_variants(extension: str) -> list[str]:
    """Return every case permutation of an extension, without its leading dot.

    unrar file masks are case-sensitive on Linux, so ``*.txt`` silently skips
    an uppercase member like ``PASSWORDS.TXT``: extraction succeeds with zero
    files, the group is marked EXTRACTED and finalize deletes the source.
    Enumerating the variants keeps the command-line pre-filter (avoids
    extracting the whole archive) while matching any suffix casing. Suffixes
    longer than 8 characters fall back to lower/upper to bound 2**n masks.
    """
    ext = extension.lstrip(".")
    if not ext:
        return []
    if len(ext) > 8:
        return sorted({ext.lower(), ext.upper()})
    return sorted(
        {"".join(chars) for chars in itertools.product(*((c.lower(), c.upper()) for c in ext))}
    )


class UnrarExtractor(ArchiveExtractor):
    """Wrapper for the unrar command-line tool."""

    def __init__(self, executable: str = "unrar"):
        self.executable = executable

    @staticmethod
    def available() -> bool:
        """Return True if unrar is installed."""
        return shutil.which("unrar") is not None

    async def extract(
        self,
        archive_path: Path,
        output_dir: Path,
        password: str | None = None,
        target_extensions: list[str] | None = None,
        timeout_seconds: int | None = None,
    ) -> ExtractionResult:
        if not archive_path.exists():
            return ExtractionResult(
                success=False,
                error_code="FILE_NOT_FOUND",
                error_message=f"Archive not found: {archive_path}",
            )

        output_dir.mkdir(parents=True, exist_ok=True)

        # unrar x: extract with full paths, -o+: overwrite, -y: yes to all
        cmd = [self.executable, "x", "-o+", "-y"]

        if password:
            cmd.append(f"-p{password}")
        else:
            cmd.append("-p-")

        cmd.append(str(archive_path))
        # File masks BEFORE the output dir: without them unrar extracts every
        # member (jpg/db/exe included) and only post-filters — for 650-900 MB
        # RAR5 dumps that is massively wasted I/O, disk and wall time.
        # Masks are case-sensitive on Linux: pass every extension case
        # permutation so an uppercase member like PASSWORDS.TXT still matches.
        if target_extensions:
            for ext in target_extensions:
                for variant in _extension_case_variants(ext):
                    cmd.append(f"*.{variant}")
        # unrar needs trailing slash on output dir
        cmd.append(str(output_dir) + "/")

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.DEVNULL,
            )
        except FileNotFoundError:
            return ExtractionResult(
                success=False,
                error_code="EXTRACTOR_NOT_FOUND",
                error_message=f"unrar executable not found: {self.executable}",
            )
        except Exception as e:
            return ExtractionResult(
                success=False,
                error_code="EXTRACTION_ERROR",
                error_message=str(e),
            )

        # The finally kills and reaps the child on ANY exit path, including
        # asyncio.CancelledError (BaseException — the `except Exception` below
        # does not intercept it, but the finally still runs). Otherwise a
        # cancelled extraction kept writing into an output_dir the caller had
        # already rmtree()d and started re-extracting into.
        try:
            try:
                if timeout_seconds:
                    stdout, stderr = await asyncio.wait_for(
                        process.communicate(), timeout=timeout_seconds
                    )
                else:
                    stdout, stderr = await process.communicate()
            except TimeoutError:
                return ExtractionResult(
                    success=False,
                    error_code="TIMEOUT",
                    error_message=f"Extraction timed out after {timeout_seconds}s",
                )

            stdout_text = stdout.decode("utf-8", errors="replace")
            stderr_text = stderr.decode("utf-8", errors="replace")

            return await self._parse_result(
                process.returncode or 0,
                stdout_text,
                stderr_text,
                output_dir,
                target_extensions,
                password=password,
            )

        except Exception as e:
            return ExtractionResult(
                success=False,
                error_code="EXTRACTION_ERROR",
                error_message=str(e),
            )
        finally:
            await self._kill_and_reap(process)

    async def _parse_result(
        self,
        return_code: int,
        stdout: str,
        stderr: str,
        output_dir: Path,
        target_extensions: list[str] | None,
        password: str | None = None,
    ) -> ExtractionResult:
        combined = stdout + stderr
        combined_lower = combined.lower()

        # Integrity errors mean the recovered data is incomplete/corrupt and
        # must never be accepted as (partial) success: a CRC-failed file can
        # still be non-empty on disk, which previously marked the group
        # EXTRACTED and let finalize delete the source archives.
        integrity_error = (
            "crc failed" in combined_lower
            or "bad archive" in combined_lower
            or "checksum error" in combined_lower
        )

        if return_code != 0:
            # Signal death (OOM killer) is never an archive problem: check it
            # before parsing output so a killed run whose partial output
            # mentions "password" is not misclassified as needing one.
            if return_code < 0:
                # ALWAYS transient, even if some files landed: accepting
                # partial data and letting finalize delete the archive loses
                # the unextracted remainder.
                return ExtractionResult(
                    success=False,
                    error_code="KILLED",
                    error_message=f"extractor killed by signal {-return_code}",
                )

            if "Incorrect password" in combined:
                return ExtractionResult(
                    success=False,
                    error_code="WRONG_PASSWORD",
                    error_message="Wrong password",
                    wrong_password=True,
                )

            # Volume chain broken (missing/renamed parts) is retryable, so it
            # is classified before generic integrity errors: a partial-extract
            # "Bad archive" must not be terminalized as corruption.
            if "Unexpected end of archive" in combined or "Cannot find volume" in combined:
                return ExtractionResult(
                    success=False,
                    error_code="VOLUME_MISSING",
                    error_message="Archive incomplete — missing or renamed volume",
                )

            # A header-encrypted archive asks for a password before any member
            # is touched; keep this recoverable classification ahead of the
            # terminal integrity one.
            if "password" in combined_lower and "enter password" in combined_lower:
                return ExtractionResult(
                    success=False,
                    error_code="PASSWORD_REQUIRED",
                    error_message="Archive requires password",
                    needs_password=True,
                )

            # Non-header-encrypted RARs with wrong passwords produce CRC errors.
            # Without a password attempt an integrity failure is corruption.
            if integrity_error:
                if password is not None:
                    return ExtractionResult(
                        success=False,
                        error_code="WRONG_PASSWORD",
                        error_message="Wrong password (integrity failure in encrypted file)",
                        wrong_password=True,
                    )
                return ExtractionResult(
                    success=False,
                    error_code="CORRUPTED",
                    error_message="Archive is corrupted (integrity failure)",
                )
            if "No files to extract" in combined:
                return ExtractionResult(
                    success=True,
                    extracted_files=[],
                    error_message="No files to extract",
                )
            if "Cannot open" in combined or "is not RAR archive" in combined:
                return ExtractionResult(
                    success=False,
                    error_code="UNSUPPORTED_FORMAT",
                    error_message="Not a RAR archive or unsupported format",
                )

            # Partial success: some files extracted with errors (e.g. corrupt headers
            # in multi-volume archives missing later volumes). Treat as success if
            # we got any files. Integrity errors already returned above.
            extracted = await asyncio.to_thread(
                self._find_extracted_files, output_dir, target_extensions
            )
            if extracted:
                logger.warning(
                    "unrar exited with code %d but extracted %d files",
                    return_code,
                    len(extracted),
                )
                return ExtractionResult(success=True, extracted_files=extracted)

            return ExtractionResult(
                success=False,
                error_code=f"EXIT_{return_code}",
                error_message=combined[:500],
            )

        # Even a zero exit code must not mask a member integrity failure.
        if integrity_error:
            if password is not None:
                return ExtractionResult(
                    success=False,
                    error_code="WRONG_PASSWORD",
                    error_message="Wrong password (integrity failure in encrypted file)",
                    wrong_password=True,
                )
            return ExtractionResult(
                success=False,
                error_code="CORRUPTED",
                error_message="Archive is corrupted (integrity failure)",
            )

        extracted = await asyncio.to_thread(
            self._find_extracted_files, output_dir, target_extensions
        )
        return ExtractionResult(success=True, extracted_files=extracted)

    @staticmethod
    def _normalize_extensions(extensions: list[str]) -> frozenset[str]:
        return frozenset(e.lower().lstrip(".") for e in extensions)

    def _find_extracted_files(
        self,
        output_dir: Path,
        target_extensions: list[str] | None,
    ) -> list[Path]:
        exts = self._normalize_extensions(target_extensions) if target_extensions else None
        # Single directory walk, one stat per file (previously one full rglob
        # walk per extension + f.is_file() and f.stat() per file).
        files: list[Path] = []
        for f in output_dir.rglob("*"):
            if not f.is_file():
                continue
            if exts is not None and f.suffix.lower().lstrip(".") not in exts:
                continue
            try:
                if f.stat().st_size > 0:
                    files.append(f)
            except OSError:
                continue
        return files

    async def list_contents(
        self,
        archive_path: Path,
        password: str | None = None,
        timeout_seconds: float | None = DEFAULT_LIST_TIMEOUT_SECONDS,
    ) -> list[str]:
        """List contents of an archive.

        Returns [] on failure/timeout (matching the previous behavior); the
        child is always killed and reaped before returning, including when the
        caller cancels us.
        """
        cmd = [self.executable, "lb"]  # bare list (filenames only)

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
                stdin=asyncio.subprocess.DEVNULL,
            )
        except Exception as e:
            logger.warning("Failed to start archive listing with unrar: %s", e)
            return []

        async def _read_listing() -> list[str]:
            stdout, _ = await process.communicate()

            if process.returncode != 0:
                return []

            return [
                line
                for line in stdout.decode("utf-8", errors="replace").splitlines()
                if line.strip() and not line.strip().endswith("/")
            ]

        # None disables the bound (legacy behavior); the default stops a
        # wedged unrar from hanging the pipeline forever.
        try:
            if timeout_seconds:
                return await asyncio.wait_for(_read_listing(), timeout=timeout_seconds)
            return await _read_listing()
        except TimeoutError:
            logger.warning(
                "Listing archive contents with unrar timed out after %ss: %s",
                timeout_seconds,
                archive_path,
            )
            return []
        except Exception as e:
            logger.warning("Failed to list archive contents with unrar: %s", e)
            return []
        finally:
            await self._kill_and_reap(process)

    async def test_password(
        self,
        archive_path: Path,
        password: str,
        first_file: str | None = None,
        timeout_seconds: int = 30,
    ) -> bool:
        """Quickly test a password by running `unrar t` on a single member."""
        cmd = [self.executable, "t", f"-p{password}", str(archive_path)]
        if first_file:
            cmd.append(first_file)
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.DEVNULL,
            )
            try:
                _, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout_seconds)
            except TimeoutError:
                process.kill()
                await asyncio.wait({asyncio.create_task(process.wait())}, timeout=5.0)
                return True  # timeout → assume valid, fall through to full extract
            stderr_text = stderr.decode("utf-8", errors="replace")
            if process.returncode != 0:
                if "Incorrect password" in stderr_text or "CRC failed" in stderr_text:
                    return False
            return True
        except Exception:
            return True  # on error, fall through to full extract
