"""Tests for archive extractor."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from telecrime.extractor.interface import ExtractionResult
from telecrime.extractor.seven_zip import SevenZipExtractor


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
        with patch.object(extractor, "_parse_result") as mock_parse:
            mock_parse.return_value = ExtractionResult(success=False, error_code="TEST")

            with patch("asyncio.create_subprocess_exec") as mock_exec:
                mock_process = AsyncMock()
                mock_process.communicate = AsyncMock(return_value=(b"", b""))
                mock_process.returncode = 1
                mock_exec.return_value = mock_process

                await extractor.extract(archive, output_dir)

        assert output_dir.exists()

    def test_parse_result_wrong_password(self):
        """Test parsing wrong password error."""
        extractor = SevenZipExtractor()
        result = extractor._parse_result(
            return_code=2,
            stdout="",
            stderr="Wrong password",
            output_dir=Path("/tmp"),
            target_extensions=None,
        )

        assert result.success is False
        assert result.error_code == "WRONG_PASSWORD"
        assert result.wrong_password is True

    def test_parse_result_data_error_encrypted(self):
        """Test parsing data error in encrypted file (wrong password)."""
        extractor = SevenZipExtractor()
        result = extractor._parse_result(
            return_code=2,
            stdout="Data Error in encrypted file",
            stderr="",
            output_dir=Path("/tmp"),
            target_extensions=None,
        )

        assert result.success is False
        assert result.wrong_password is True
        assert result.error_code == "WRONG_PASSWORD"

    def test_parse_result_data_error_corrupted(self):
        """Test parsing data error without encryption context (corruption)."""
        extractor = SevenZipExtractor()
        result = extractor._parse_result(
            return_code=2,
            stdout="Data Error : some_file.txt",
            stderr="",
            output_dir=Path("/tmp"),
            target_extensions=None,
        )

        assert result.success is False
        assert result.wrong_password is False
        assert result.error_code == "CORRUPTED"

    def test_parse_result_password_required(self):
        """Test parsing password required error."""
        extractor = SevenZipExtractor()
        result = extractor._parse_result(
            return_code=2,
            stdout="Enter password",
            stderr="",
            output_dir=Path("/tmp"),
            target_extensions=None,
        )

        assert result.success is False
        assert result.needs_password is True

    def test_parse_result_cannot_open(self):
        """Test parsing cannot open error."""
        extractor = SevenZipExtractor()
        result = extractor._parse_result(
            return_code=2,
            stdout="Cannot open the file",
            stderr="",
            output_dir=Path("/tmp"),
            target_extensions=None,
        )

        assert result.success is False
        assert result.error_code == "CANNOT_OPEN"

    def test_parse_result_unsupported(self):
        """Test parsing unsupported format error."""
        extractor = SevenZipExtractor()
        result = extractor._parse_result(
            return_code=2,
            stdout="Unsupported archive type",
            stderr="",
            output_dir=Path("/tmp"),
            target_extensions=None,
        )

        assert result.success is False
        assert result.error_code == "UNSUPPORTED_FORMAT"

    def test_parse_result_success(self, tmp_path):
        """Test parsing successful extraction."""
        # Create some test files
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        (output_dir / "file1.txt").write_text("data")
        (output_dir / "file2.epub").write_text("data")

        extractor = SevenZipExtractor()
        result = extractor._parse_result(
            return_code=0,
            stdout="Everything is Ok",
            stderr="",
            output_dir=output_dir,
            target_extensions=None,
        )

        assert result.success is True
        assert len(result.extracted_files) == 2

    def test_parse_result_success_with_filter(self, tmp_path):
        """Test parsing successful extraction with extension filter."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        (output_dir / "file1.txt").write_text("data")
        (output_dir / "file2.epub").write_text("data")
        (output_dir / "file3.pdf").write_text("data")

        extractor = SevenZipExtractor()
        result = extractor._parse_result(
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
            mock_process.communicate = AsyncMock(return_value=(mock_output, b""))
            mock_process.returncode = 0
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
            mock_process.communicate = AsyncMock(return_value=(mock_output, b""))
            mock_process.returncode = 0
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
            mock_process.communicate = MagicMock()
            mock_process.returncode = 0
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
            mock_process.communicate = AsyncMock(return_value=(b"", b""))
            mock_process.returncode = 0
            mock_exec.return_value = mock_process

            await extractor.extract(
                archive,
                tmp_path / "output",
                target_extensions=[".epub", ".pdf"],
            )

            # Check that -ir flags were included
            call_args = mock_exec.call_args[0]
            assert any("-ir!*.epub" in str(arg) for arg in call_args)
            assert any("-ir!*.pdf" in str(arg) for arg in call_args)

    @pytest.mark.asyncio
    async def test_extract_timeout_returns_error(self, tmp_path):
        """Test extraction timeout handling."""
        extractor = SevenZipExtractor()
        archive = tmp_path / "test.zip"
        archive.touch()

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_process = MagicMock()
            mock_process.communicate = MagicMock()
            mock_process.returncode = 0
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


class TestExtractionTimeoutHelper:
    """Tests for extract._extraction_timeout (size-proportional timeout)."""

    def test_small_archive_uses_config_floor(self, tmp_path):
        from telecrime.pipeline.extract import _extraction_timeout

        archive = tmp_path / "small.zip"
        archive.write_bytes(b"x" * (100 * 1024 * 1024))  # 100 MB
        ctx = MagicMock()
        ctx.config.extraction.max_extraction_seconds = 600

        assert _extraction_timeout(ctx, archive) == 600

    def test_large_archive_scales_with_size(self, tmp_path):
        from telecrime.pipeline.extract import _extraction_timeout

        archive = tmp_path / "big.zip"
        archive.write_bytes(b"x" * (2 * 1024 * 1024 * 1024))  # 2 GB
        ctx = MagicMock()
        ctx.config.extraction.max_extraction_seconds = 600

        # 2048 MiB * 3 = 6144s, above the 600s config floor.
        assert _extraction_timeout(ctx, archive) == 6144

    def test_missing_archive_uses_floor(self, tmp_path):
        from telecrime.pipeline.extract import _extraction_timeout

        ctx = MagicMock()
        ctx.config.extraction.max_extraction_seconds = 600
        assert _extraction_timeout(ctx, tmp_path / "missing.zip") == 600
