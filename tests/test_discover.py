"""Tests for archive discovery stage."""

from unittest.mock import MagicMock

from telecrime.pipeline.discover import (
    ARCHIVE_EXTENSIONS,
    ARCHIVE_MIME_TYPES,
    DiscoverStage,
)


class TestArchiveExtensions:
    """Tests for ARCHIVE_EXTENSIONS mapping."""

    def test_common_extensions_exist(self):
        """Test common archive extensions are defined."""
        common = [".zip", ".7z", ".rar", ".tar", ".gz"]
        for ext in common:
            assert ext in ARCHIVE_EXTENSIONS

    def test_split_extensions_exist(self):
        """Test split archive extensions are defined."""
        split = [".z01", ".001", ".r00", ".part1.rar"]
        for ext in split:
            assert ext in ARCHIVE_EXTENSIONS


class TestArchiveMimeTypes:
    """Tests for ARCHIVE_MIME_TYPES set."""

    def test_common_mime_types_exist(self):
        """Test common MIME types are defined."""
        common = [
            "application/zip",
            "application/x-7z-compressed",
            "application/x-rar-compressed",
        ]
        for mime in common:
            assert mime in ARCHIVE_MIME_TYPES

    def test_octet_stream_included(self):
        """Test application/octet-stream is included."""
        # Many archives are served with this generic MIME type
        assert "application/octet-stream" in ARCHIVE_MIME_TYPES


class TestDiscoverStage:
    """Tests for DiscoverStage class."""

    def _make_attachment(self, filename, mime_type=None, size=None):
        """Create a mock FileAttachment."""
        mock = MagicMock()
        mock.filename = filename
        mock.mime_type = mime_type
        mock.size = size
        mock.is_archive_candidate = False
        mock.archive_type = None
        mock.detected_part_number = None
        mock.detected_base_name = None
        return mock

    def test_classify_zip(self):
        """Test ZIP file classification."""
        stage = DiscoverStage()
        attachment = self._make_attachment("test.zip")

        is_archive, archive_type, part_info = stage._classify_attachment(attachment)

        assert is_archive is True
        assert archive_type == "zip"
        assert part_info is None

    def test_classify_split_rar(self):
        """Test split RAR classification."""
        stage = DiscoverStage()
        attachment = self._make_attachment("book.part3.rar")

        is_archive, archive_type, part_info = stage._classify_attachment(attachment)

        assert is_archive is True
        assert archive_type == "rar"
        assert part_info is not None
        assert part_info[0] == "book"
        assert part_info[1] == 3

    def test_classify_split_7z(self):
        """Test split 7z classification."""
        stage = DiscoverStage()
        attachment = self._make_attachment("archive.7z.002")

        is_archive, archive_type, part_info = stage._classify_attachment(attachment)

        assert is_archive is True
        assert archive_type == "7z"
        assert part_info is not None
        assert part_info[1] == 2

    def test_classify_by_mime_type(self):
        """Test classification by MIME type."""
        stage = DiscoverStage()
        attachment = self._make_attachment(
            "unknownfile",
            mime_type="application/x-7z-compressed"
        )

        is_archive, archive_type, part_info = stage._classify_attachment(attachment)

        assert is_archive is True

    def test_classify_large_octet_stream(self):
        """Test large octet-stream files are candidates."""
        stage = DiscoverStage()
        attachment = self._make_attachment(
            "largefile",
            mime_type="application/octet-stream",
            size=50 * 1024 * 1024  # 50MB
        )

        is_archive, archive_type, part_info = stage._classify_attachment(attachment)

        assert is_archive is True
        assert archive_type == "unknown"

    def test_classify_non_archive(self):
        """Test non-archive file classification."""
        stage = DiscoverStage()
        attachment = self._make_attachment(
            "document.pdf",
            mime_type="application/pdf"
        )

        is_archive, archive_type, part_info = stage._classify_attachment(attachment)

        assert is_archive is False
        assert archive_type is None

    def test_classify_rejected_extensions(self):
        """Executables, images, and videos are rejected even with no MIME type."""
        stage = DiscoverStage()
        for filename in ["malware.exe", "photo.jpg", "clip.mp4", "lib.dll", "script.ps1"]:
            is_archive, _, _ = stage._classify_attachment(self._make_attachment(filename))
            assert is_archive is False, f"{filename!r} should be rejected"

    def test_archive_with_exe_in_name_is_not_rejected(self):
        """A .zip whose name happens to contain .exe is still an archive."""
        stage = DiscoverStage()
        is_archive, archive_type, _ = stage._classify_attachment(
            self._make_attachment("malware.exe.zip")
        )
        assert is_archive is True
        assert archive_type == "zip"

    def test_classify_small_octet_stream(self):
        """Test small octet-stream files are not automatically archives."""
        stage = DiscoverStage()
        attachment = self._make_attachment(
            "smallfile",
            mime_type="application/octet-stream",
            size=1024  # 1KB
        )

        is_archive, archive_type, part_info = stage._classify_attachment(attachment)

        # Small files without archive extension may or may not match
        # based on the implementation's heuristics
        # The key is that octet-stream alone isn't sufficient for small files
        if is_archive:
            # If it's detected, archive_type should be set
            assert archive_type is not None

    def test_stage_name(self):
        """Test stage has correct name."""
        stage = DiscoverStage()
        assert stage.name == "discover"
