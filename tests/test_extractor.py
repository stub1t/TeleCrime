"""Tests for archive extractor."""

import asyncio
import shutil
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from telecrime.extractor.interface import ExtractionResult
from telecrime.extractor.seven_zip import SevenZipExtractor
from telecrime.extractor.unrar import UnrarExtractor


class _AsyncLineReader:
    """Async iterable over bytes lines, mimicking subprocess stdout."""

    def __init__(self, data: bytes):
        lines = data.splitlines()
        self._lines = iter(lines if lines else [b""])
        self._buf = data

    async def readline(self) -> bytes:
        try:
            return next(self._lines) + b"\n"
        except StopIteration:
            return b""

    async def read(self, n: int = -1) -> bytes:
        out = self._buf[:n] if n >= 0 else self._buf
        self._buf = self._buf[n:] if n >= 0 else b""
        return out


class _BlockingReader:
    """Reader that never returns, so the parent blocks until cancelled/timed out."""

    def __init__(self):
        self.entered = asyncio.Event()

    async def read(self, n: int = -1) -> bytes:
        self.entered.set()
        await asyncio.Event().wait()
        return b""

    async def readline(self) -> bytes:
        self.entered.set()
        await asyncio.Event().wait()
        return b""



class TestExtractionResult:
    """Tests for ExtractionResult dataclass."""

    def test_success_result(self):
        """Test creating a success result."""
        result = ExtractionResult(
            success=True,
            extracted_files=[Path("/tmp/file1.txt"), Path("/tmp/file2.txt")],
        )

        assert result.success is True
        assert len(result.extracted_files) == 2
        assert result.error_code is None
        assert result.needs_password is False

    def test_failure_result(self):
        """Test creating a failure result."""
        result = ExtractionResult(
            success=False,
            error_code="WRONG_PASSWORD",
            error_message="Invalid password",
            wrong_password=True,
        )

        assert result.success is False
        assert result.error_code == "WRONG_PASSWORD"
        assert result.wrong_password is True

    def test_password_needed_result(self):
        """Test creating a password-needed result."""
        result = ExtractionResult(
            success=False,
            error_code="PASSWORD_REQUIRED",
            needs_password=True,
        )

        assert result.success is False
        assert result.needs_password is True
        assert result.wrong_password is False


class TestSevenZipExtractor:
    """Tests for SevenZipExtractor class."""

    def test_init_default_executable(self):
        """Test default executable path."""
        extractor = SevenZipExtractor()
        assert extractor.executable == "7z"

    def test_init_custom_executable(self):
        """Test custom executable path."""
        extractor = SevenZipExtractor("/usr/bin/7za")
        assert extractor.executable == "/usr/bin/7za"

    @pytest.mark.asyncio
    async def test_extract_file_not_found(self, tmp_path):
        """Test extraction of non-existent file."""
        extractor = SevenZipExtractor()
        result = await extractor.extract(
            Path("/nonexistent/archive.zip"),
            tmp_path / "output",
        )

        assert result.success is False
        assert result.error_code == "FILE_NOT_FOUND"

    @pytest.mark.asyncio
    async def test_extract_creates_output_dir(self, tmp_path):
        """Test that output directory is created."""
        extractor = SevenZipExtractor()
        archive = tmp_path / "test.zip"
        archive.touch()  # Create empty file
        output_dir = tmp_path / "output" / "nested"

        # Will fail because it's not a real archive, but should create dir
        mock_parse = AsyncMock(return_value=ExtractionResult(success=False, error_code="TEST"))
        with patch.object(extractor, "_parse_result", new=mock_parse):

            with patch("asyncio.create_subprocess_exec") as mock_exec:
                mock_process = AsyncMock()
                mock_process.stdout = _AsyncLineReader(b"")
                mock_process.stderr = _AsyncLineReader(b"")
                mock_process.returncode = 1
                mock_exec.return_value = mock_process

                await extractor.extract(archive, output_dir)

        assert output_dir.exists()

    @pytest.mark.asyncio
    async def test_extract_awaits_wait_before_reading_returncode(self, tmp_path):
        """Regression: the child-watcher can lag after pipe EOF, leaving
        returncode None. Reading it before `await process.wait()` misclassified
        failed 7z runs (wrong password, data error) as exit 0 → groups marked
        EXTRACTED → archives deleted by finalize → permanent data loss.
        """
        extractor = SevenZipExtractor()
        archive = tmp_path / "protected.zip"
        archive.touch()
        output_dir = tmp_path / "out"

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_process = AsyncMock()
            mock_process.stdout = _AsyncLineReader(b"")
            mock_process.stderr = _AsyncLineReader(b"Wrong password")
            # Simulate the watcher lag: returncode stays None until wait() is
            # awaited, then becomes 2 (wrong password).
            mock_process.returncode = None
            mock_process.wait = AsyncMock(side_effect=lambda: setattr(mock_process, "returncode", 2) or 0)
            mock_exec.return_value = mock_process

            result = await extractor.extract(archive, output_dir)

        assert result.success is False
        assert result.wrong_password is True
        assert mock_process.wait.await_count == 1

    @pytest.mark.parametrize(
        "rc, stdout, stderr, success, error_code, wrong_password, needs_password",
        [
            (2, "", "Wrong password", False, "WRONG_PASSWORD", True, False),
            (2, "Data Error in encrypted file", "", False, "WRONG_PASSWORD", True, False),
            (2, "Data Error : some_file.txt", "", False, "CORRUPTED", False, False),
            (2, "Enter password", "", False, "PASSWORD_REQUIRED", False, True),
            (2, "ERROR: Cannot open encrypted archive. Wrong password?", "", False, "WRONG_PASSWORD", True, False),
            (2, "Cannot open the file", "", False, "CANNOT_OPEN", False, False),
            (2, "Unsupported archive type", "", False, "UNSUPPORTED_FORMAT", False, False),
            # Exit 1 is only a failure when nothing usable landed on disk; the
            # partial-success path is covered by the dedicated test below.
            (1, "", "some other failure", False, "EXIT_1", False, False),
        ],
    )
    @pytest.mark.asyncio
    async def test_parse_result_error_cases(
        self, rc, stdout, stderr, success, error_code, wrong_password, needs_password, tmp_path
    ):
        """Table-driven _parse_result error classification."""
        extractor = SevenZipExtractor()
        # Empty output dir: an exit-1 parse must not pick up unrelated files.
        result = await extractor._parse_result(
            return_code=rc,
            stdout=stdout,
            stderr=stderr,
            output_dir=tmp_path,
            target_extensions=None,
        )
        assert result.success is success
        assert result.error_code == error_code
        assert result.wrong_password is wrong_password
        assert result.needs_password is needs_password

    @pytest.mark.asyncio
    async def test_parse_result_killed_beats_password_filename(self, tmp_path):
        """Regression: a signal-killed (OOM) extraction whose partial output
        mentions a file named e.g. passwords.txt must classify as KILLED, not
        PASSWORD_REQUIRED — otherwise the pipeline burns password candidates.
        """
        extractor = SevenZipExtractor()
        result = await extractor._parse_result(
            return_code=-9,
            stdout="Extracting  passwords.txt\nEnter password",
            stderr="",
            output_dir=tmp_path,
            target_extensions=None,
        )
        assert result.error_code == "KILLED"
        assert result.needs_password is False
        assert result.wrong_password is False

    @pytest.mark.asyncio
    async def test_parse_result_bare_password_substring_not_password_required(self, tmp_path):
        """A non-zero exit whose output only mentions a password-looking
        filename is not a password prompt and must not be classified as one.
        """
        extractor = SevenZipExtractor()
        result = await extractor._parse_result(
            return_code=2,
            stdout="Extracting passwords.txt: some unrelated error",
            stderr="",
            output_dir=tmp_path,
            target_extensions=None,
        )
        assert result.error_code == "EXIT_2"
        assert result.needs_password is False

    @pytest.mark.asyncio
    async def test_parse_result_success(self, tmp_path):
        """Test parsing successful extraction."""
        # Create some test files
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        (output_dir / "file1.txt").write_text("data")
        (output_dir / "file2.epub").write_text("data")

        extractor = SevenZipExtractor()
        result = await extractor._parse_result(
            return_code=0,
            stdout="Everything is Ok",
            stderr="",
            output_dir=output_dir,
            target_extensions=None,
        )

        assert result.success is True
        assert len(result.extracted_files) == 2

    @pytest.mark.asyncio
    async def test_parse_result_success_with_filter(self, tmp_path):
        """Test parsing successful extraction with extension filter."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        (output_dir / "file1.txt").write_text("data")
        (output_dir / "file2.epub").write_text("data")
        (output_dir / "file3.pdf").write_text("data")

        extractor = SevenZipExtractor()
        result = await extractor._parse_result(
            return_code=0,
            stdout="Everything is Ok",
            stderr="",
            output_dir=output_dir,
            target_extensions=[".epub", ".pdf"],
        )

        assert result.success is True
        assert len(result.extracted_files) == 2

        filenames = [f.name for f in result.extracted_files]
        assert "file2.epub" in filenames
        assert "file3.pdf" in filenames
        assert "file1.txt" not in filenames

    @pytest.mark.asyncio
    async def test_parse_result_exit1_with_files_is_partial_success(self, tmp_path):
        """7z exit 1 (warning) with files on disk must be partial success.

        Returning EXIT_1 retried to the attempt cap, marked the group
        FAILED_TERMINAL and let finalize delete the archive along with the
        successfully extracted files.
        """
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        (output_dir / "creds.txt").write_text("data")

        extractor = SevenZipExtractor()
        result = await extractor._parse_result(
            return_code=1,
            stdout="WARNINGS: There are some data after the end of the payload data",
            stderr="",
            output_dir=output_dir,
            target_extensions=["txt"],
        )

        assert result.success is True
        assert [f.name for f in result.extracted_files] == ["creds.txt"]

    @pytest.mark.asyncio
    async def test_parse_result_exit1_with_integrity_error_is_failure(self, tmp_path):
        """Exit 1 plus a corruption marker must never count as partial success:
        the file that was written may itself be corrupt."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        (output_dir / "creds.txt").write_text("data")

        extractor = SevenZipExtractor()
        result = await extractor._parse_result(
            return_code=1,
            stdout="CRC Failed : creds.txt",
            stderr="",
            output_dir=output_dir,
            target_extensions=["txt"],
        )

        assert result.success is False
        assert result.error_code == "EXIT_1"

    def test_find_extracted_files_recursive(self, tmp_path):
        """Test finding files in nested directories."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        (output_dir / "file1.txt").write_text("data")

        nested = output_dir / "subdir"
        nested.mkdir()
        (nested / "file2.txt").write_text("data")

        extractor = SevenZipExtractor()
        files = extractor._find_extracted_files(output_dir, None)

        assert len(files) == 2

    def test_extension_case_variants_cover_mixed_case_suffixes(self):
        """Every suffix casing must be matched, not just all-lower/all-upper."""
        from telecrime.extractor.seven_zip import _extension_case_variants

        variants = _extension_case_variants(".TXT")
        assert set(variants) == {
            "txt", "txT", "tXt", "tXT", "Txt", "TxT", "TXt", "TXT",
        }
        # Pathological long extensions fall back to two patterns, not 2**n.
        assert _extension_case_variants("longextension") == [
            "LONGEXTENSION",
            "longextension",
        ]


class TestUnrarExtractor:
    """Tests for UnrarExtractor integrity/partial-output handling."""

    @pytest.mark.asyncio
    async def test_crc_failed_with_password_attempt_is_wrong_password(self, tmp_path):
        """A CRC failure after a password attempt means the password was wrong.

        Previously a non-empty (garbage) file on disk made this a success, so
        finalize deleted the still-encrypted source archive.
        """
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        (output_dir / "secret.txt").write_text("garbage from wrong password")

        extractor = UnrarExtractor()
        result = await extractor._parse_result(
            return_code=3,
            stdout="Extracting  secret.txt\nCRC failed in secret.txt\n",
            stderr="",
            output_dir=output_dir,
            target_extensions=["txt"],
            password="guess",
        )

        assert result.success is False
        assert result.error_code == "WRONG_PASSWORD"
        assert result.wrong_password is True

    @pytest.mark.asyncio
    async def test_bad_archive_without_password_is_corrupted(self, tmp_path):
        """Without a password attempt an integrity error is pure corruption —
        leftover partial output must not be accepted as success."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        (output_dir / "secret.txt").write_text("partial bytes")

        extractor = UnrarExtractor()
        result = await extractor._parse_result(
            return_code=3,
            stdout="Bad archive\n",
            stderr="",
            output_dir=output_dir,
            target_extensions=["txt"],
        )

        assert result.success is False
        assert result.error_code == "CORRUPTED"
        assert result.wrong_password is False

    @pytest.mark.asyncio
    async def test_checksum_error_with_zero_exit_is_corrupted(self, tmp_path):
        """Even exit 0 must not mask a member integrity failure."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        (output_dir / "secret.txt").write_text("partial bytes")

        extractor = UnrarExtractor()
        result = await extractor._parse_result(
            return_code=0,
            stdout="Checksum error in secret.txt\n",
            stderr="",
            output_dir=output_dir,
            target_extensions=["txt"],
            password="guess",
        )

        assert result.success is False
        assert result.error_code == "WRONG_PASSWORD"

    @pytest.mark.asyncio
    async def test_extract_masks_include_case_variants(self, tmp_path):
        """unrar masks are case-sensitive on Linux: `*.txt` alone skipped an
        uppercase member and the archive was deleted after an empty extract."""
        extractor = UnrarExtractor()
        archive = tmp_path / "test.rar"
        archive.touch()

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_process = AsyncMock()
            mock_process.communicate = AsyncMock(return_value=(b"", b""))
            mock_process.returncode = 0
            mock_exec.return_value = mock_process

            await extractor.extract(
                archive,
                tmp_path / "output",
                target_extensions=["txt"],
            )

        call_args = mock_exec.call_args[0]
        assert "*.txt" in call_args
        assert "*.TXT" in call_args
        assert "*.Txt" in call_args


class TestSevenZipExtractorAsync:
    """Async tests for SevenZipExtractor."""

    @pytest.mark.asyncio
    async def test_list_contents_parse_output(self):
        """Test parsing list contents output."""
        extractor = SevenZipExtractor()

        # Real 7z -slt output: the archive's own path appears first in the
        # metadata block (Path = /tmp/test.7z).  It must NOT be included in
        # the returned member list.
        mock_output = b"""
7-Zip 21.07 (x64)

Listing archive: /tmp/test.7z

--
Path = /tmp/test.7z
Type = 7z

----------
Path = file1.txt
Size = 1234
Compressed = 1000

----------
Path = subdir/file2.epub
Size = 5678
Compressed = 5000

----------
Path = subdir
Folder = +
"""

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_process = AsyncMock()
            mock_process.stdout = _AsyncLineReader(mock_output)
            mock_process.returncode = 0
            mock_process.wait = AsyncMock(return_value=0)
            mock_exec.return_value = mock_process

            files = await extractor.list_contents(Path("/tmp/test.7z"))

        # Archive's own absolute path must not appear in member list
        assert "/tmp/test.7z" not in files
        assert "file1.txt" in files
        assert "subdir/file2.epub" in files
        # Folder entries (Folder = +) must be excluded
        assert "subdir" not in files

    @pytest.mark.asyncio
    async def test_list_contents_excludes_archive_path(self):
        """Regression: archive's own absolute path must not be flagged as unsafe member."""
        extractor = SevenZipExtractor()
        archive = Path("/tmp/downloads/My Cloud Logs.zip")

        mock_output = (
            b"7-Zip 22.01\n\nListing archive: /tmp/downloads/My Cloud Logs.zip\n\n"
            b"--\nPath = /tmp/downloads/My Cloud Logs.zip\nType = zip\n\n"
            b"----------\nPath = Passwords.txt\nSize = 100\n"
        )

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_process = AsyncMock()
            mock_process.stdout = _AsyncLineReader(mock_output)
            mock_process.returncode = 0
            mock_process.wait = AsyncMock(return_value=0)
            mock_exec.return_value = mock_process

            files = await extractor.list_contents(archive)

        assert str(archive) not in files
        assert "Passwords.txt" in files

    @pytest.mark.asyncio
    async def test_extract_with_password(self, tmp_path):
        """Test extraction command includes password."""
        extractor = SevenZipExtractor()
        archive = tmp_path / "test.zip"
        archive.touch()

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_process = MagicMock()
            mock_process.stdout = _AsyncLineReader(b"")
            mock_process.stderr = _AsyncLineReader(b"")
            mock_process.returncode = 0
            mock_process.wait = AsyncMock(return_value=0)
            mock_process.kill = MagicMock()
            mock_exec.return_value = mock_process

            await extractor.extract(
                archive,
                tmp_path / "output",
                password="secret123",
            )

            # Check that -p flag was included with password
            call_args = mock_exec.call_args[0]
            assert any("-psecret123" in str(arg) for arg in call_args)

    @pytest.mark.asyncio
    async def test_extract_with_target_extensions(self, tmp_path):
        """Test extraction command includes extension filters."""
        extractor = SevenZipExtractor()
        archive = tmp_path / "test.zip"
        archive.touch()

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_process = AsyncMock()
            mock_process.stdout = _AsyncLineReader(b"")
            mock_process.stderr = _AsyncLineReader(b"")
            mock_process.returncode = 0
            mock_exec.return_value = mock_process

            await extractor.extract(
                archive,
                tmp_path / "output",
                target_extensions=[".epub", ".pdf"],
            )

            # Check that -ir flags were included. Case variants are required:
            # 7z's glob is case-sensitive on Linux, so `-ir!*.epub` alone
            # silently skipped `MEMBER.EPUB` (exit 0, zero files extracted).
            call_args = mock_exec.call_args[0]
            assert any("-ir!*.epub" in str(arg) for arg in call_args)
            assert any("-ir!*.pdf" in str(arg) for arg in call_args)
            assert any("-ir!*.EPUB" in str(arg) for arg in call_args)
            assert any("-ir!*.ePub" in str(arg) for arg in call_args)

    @pytest.mark.asyncio
    async def test_extract_uppercase_extension_member_is_not_skipped(self, tmp_path):
        """Regression: a `Passwords.TXT` member must survive extraction.

        With the old case-sensitive `-ir!*.txt` filter 7z exited 0 having
        extracted nothing; the group was marked EXTRACTED and finalize deleted
        the source archive.
        """
        if shutil.which("7z") is None:
            pytest.skip("7z is not installed")

        archive = tmp_path / "logs.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("Passwords.TXT", "https://example.com;user;pass\n")

        extractor = SevenZipExtractor()
        result = await extractor.extract(
            archive,
            tmp_path / "output",
            target_extensions=["txt"],
        )

        assert result.success is True
        assert [f.name for f in result.extracted_files] == ["Passwords.TXT"]

    @pytest.mark.asyncio
    async def test_extract_timeout_returns_error(self, tmp_path):
        """Test extraction timeout handling."""
        extractor = SevenZipExtractor()
        archive = tmp_path / "test.zip"
        archive.touch()

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_process = MagicMock()
            mock_process.stdout = _AsyncLineReader(b"")
            mock_process.stderr = _AsyncLineReader(b"")
            mock_process.returncode = 0
            mock_process.wait = AsyncMock(return_value=0)
            mock_process.kill = MagicMock()
            mock_exec.return_value = mock_process

            async def fake_wait_for(*args, **kwargs):
                raise TimeoutError

            with patch("asyncio.wait_for", new=fake_wait_for):
                result = await extractor.extract(
                    archive,
                    tmp_path / "output",
                    timeout_seconds=1,
                )

        assert result.success is False
        assert result.error_code == "TIMEOUT"


class TestSubprocessLifecycle:
    """Child-process kill/reap guarantees on cancellation and list timeouts."""

    @pytest.mark.asyncio
    async def test_extract_cancellation_kills_seven_zip_child(self, tmp_path):
        extractor = SevenZipExtractor()
        archive = tmp_path / "test.zip"
        archive.touch()

        process = MagicMock()
        reader = _BlockingReader()
        process.stdout = reader
        process.stderr = reader
        process.returncode = None
        process.kill = MagicMock()
        process.wait = AsyncMock(return_value=-9)

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_exec.return_value = process
            task = asyncio.create_task(extractor.extract(archive, tmp_path / "out"))
            await asyncio.wait_for(reader.entered.wait(), timeout=2.0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        process.kill.assert_called_once()
        process.wait.assert_awaited()

    @pytest.mark.asyncio
    async def test_extract_cancellation_kills_unrar_child(self, tmp_path):
        extractor = UnrarExtractor()
        archive = tmp_path / "test.rar"
        archive.touch()

        entered = asyncio.Event()

        async def _blocked_communicate(*args, **kwargs):
            entered.set()
            await asyncio.Event().wait()
            return b"", b""

        process = MagicMock()
        process.returncode = None
        process.communicate = AsyncMock(side_effect=_blocked_communicate)
        process.kill = MagicMock()
        process.wait = AsyncMock(return_value=-9)

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_exec.return_value = process
            task = asyncio.create_task(extractor.extract(archive, tmp_path / "out"))
            await asyncio.wait_for(entered.wait(), timeout=2.0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        process.kill.assert_called_once()
        process.wait.assert_awaited()

    @pytest.mark.asyncio
    async def test_list_contents_timeout_kills_seven_zip_child(self):
        extractor = SevenZipExtractor()
        process = MagicMock()
        process.stdout = _BlockingReader()
        process.returncode = None
        process.kill = MagicMock()
        process.wait = AsyncMock(return_value=-9)

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_exec.return_value = process
            files = await extractor.list_contents(Path("/tmp/test.7z"), timeout_seconds=0.05)

        assert files == []
        process.kill.assert_called_once()
        process.wait.assert_awaited()

    @pytest.mark.asyncio
    async def test_list_contents_timeout_kills_unrar_child(self):
        extractor = UnrarExtractor()

        async def _blocked_communicate(*args, **kwargs):
            await asyncio.Event().wait()
            return b"", b""

        process = MagicMock()
        process.returncode = None
        process.communicate = AsyncMock(side_effect=_blocked_communicate)
        process.kill = MagicMock()
        process.wait = AsyncMock(return_value=-9)

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_exec.return_value = process
            files = await extractor.list_contents(Path("/tmp/test.rar"), timeout_seconds=0.05)

        assert files == []
        process.kill.assert_called_once()
        process.wait.assert_awaited()


class TestExtractionTimeoutHelper:
    """Tests for extract._extraction_timeout (size-proportional timeout)."""

    def test_small_archive_uses_config_floor(self, tmp_path):
        from telecrime.pipeline.extract import _extraction_timeout

        archive = tmp_path / "small.zip"
        # Sparse file — the timeout helper only needs stat().st_size, and a
        # real 100MB write multiplied the /tmp (RAM tmpfs) footprint of the
        # suite and repeatedly filled it to 100%.
        with open(archive, "wb") as f:
            f.truncate(100 * 1024 * 1024)
        ctx = MagicMock()
        ctx.config.extraction.max_extraction_seconds = 600

        assert _extraction_timeout(ctx, archive) == 600

    def test_large_archive_scales_with_size(self, tmp_path):
        from telecrime.pipeline.extract import _extraction_timeout

        archive = tmp_path / "big.zip"
        with open(archive, "wb") as f:
            f.truncate(2 * 1024 * 1024 * 1024)  # 2 GB sparse
        ctx = MagicMock()
        ctx.config.extraction.max_extraction_seconds = 600

        # 2048 MiB * 3 = 6144s, above the 600s config floor.
        assert _extraction_timeout(ctx, archive) == 6144

    def test_missing_archive_uses_floor(self, tmp_path):
        from telecrime.pipeline.extract import _extraction_timeout

        ctx = MagicMock()
        ctx.config.extraction.max_extraction_seconds = 600
        assert _extraction_timeout(ctx, tmp_path / "missing.zip") == 600
