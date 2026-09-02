"""Tests for configuration management."""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from telecrime.config import (
    Config,
    DownloadConfig,
    ExtractionConfig,
    TelegramConfig,
    _apply_env_vars,
    get_default_config_path,
    get_default_data_dir,
    load_config,
)


class TestConfig:
    """Tests for Config dataclass."""

    def test_default_values(self):
        """Test default configuration values."""
        config = Config()

        assert config.telegram is not None
        assert config.extraction is not None
        assert config.download is not None

    def test_post_init_no_implicit_database_url(self):
        """Bare Config() leaves database_url empty (no SQLite fallback)."""
        config = Config(data_dir=Path("/custom/data"))

        assert config.database_url == ""

    def test_custom_data_dir_updates_derived_paths(self, tmp_path):
        config = Config(data_dir=tmp_path / "data")

        assert config.downloads_dir == tmp_path / "data" / "downloads"
        assert config.extracted_dir == tmp_path / "data" / "extracted"

    def test_ensure_directories(self, tmp_path):
        """Test ensure_directories creates all required dirs."""
        config = Config(
            data_dir=tmp_path / "data",
            downloads_dir=tmp_path / "downloads",
            extracted_dir=tmp_path / "extracted",
        )

        config.ensure_directories()

        assert config.data_dir.exists()
        assert config.downloads_dir.exists()
        assert config.extracted_dir.exists()

    def test_explicit_paths_are_preserved(self, tmp_path):
        downloads = tmp_path / "custom-downloads"
        extracted = tmp_path / "custom-extracted"
        config = Config(
            data_dir=tmp_path / "data",
            downloads_dir=downloads,
            extracted_dir=extracted,
        )

        assert config.downloads_dir == downloads
        assert config.extracted_dir == extracted


class TestTelegramConfig:
    """Tests for TelegramConfig dataclass."""

    def test_default_values(self):
        """Test default Telegram config values."""
        config = TelegramConfig()

        assert config.api_id is None
        assert config.api_hash is None
        assert config.session_name == "telecrime"
        assert config.phone is None
        assert config.aux_session_name is None

    def test_aux_session_swap(self):
        """with_aux_telegram_session swaps session_name when aux is set."""
        config = Config()
        assert config.with_aux_telegram_session() is config

        config.telegram.aux_session_name = "telecrime_aux"
        swapped = config.with_aux_telegram_session()
        assert swapped is not config
        assert swapped.telegram.session_name == "telecrime_aux"
        # original is untouched
        assert config.telegram.session_name == "telecrime"

    def test_aux_session_env_override(self):
        """TELECRIME_TELEGRAM_AUX_SESSION_NAME populates aux_session_name."""
        config = Config()
        with patch.dict(os.environ, {"TELECRIME_TELEGRAM_AUX_SESSION_NAME": "tc_aux"}):
            _apply_env_vars(config)
        assert config.telegram.aux_session_name == "tc_aux"

    def test_download_sessions_env_override(self):
        """TELECRIME_DOWNLOAD_SESSIONS populates download_session_names."""
        config = Config()
        with patch.dict(
            os.environ,
            {"TELECRIME_DOWNLOAD_SESSIONS": "dl2, dl3 ,dl4"},
        ):
            _apply_env_vars(config)
        assert config.telegram.download_session_names == ["dl2", "dl3", "dl4"]

    def test_download_session_swap(self):
        """with_download_session picks the right session; index 0 stays main."""
        config = Config()
        assert config.with_download_session(0) is config
        assert config.with_download_session(1) is config

        config.telegram.download_session_names = ["dl2", "dl3"]
        assert config.with_download_session(0).telegram.session_name == "telecrime"
        assert config.with_download_session(1).telegram.session_name == "dl2"
        assert config.with_download_session(2).telegram.session_name == "dl3"
        # out of range falls back to main
        assert config.with_download_session(3) is config
        # original is untouched
        assert config.telegram.session_name == "telecrime"

    def test_parallel_chunks_env_override(self):
        """TELECRIME_PARALLEL_CHUNKS populates download.parallel_chunks."""
        config = Config()
        with patch.dict(os.environ, {"TELECRIME_PARALLEL_CHUNKS": "4"}):
            _apply_env_vars(config)
        assert config.download.parallel_chunks == 4

    def test_parallel_chunks_default(self):
        """Parallel chunk download is enabled by default."""
        config = Config()
        assert config.download.parallel_chunks == 8
        assert config.download.parallel_min_bytes == 4 * 1024 * 1024


class TestExtractionConfig:
    """Tests for ExtractionConfig dataclass."""

    def test_default_values(self):
        """Test default extraction config values."""
        config = ExtractionConfig()

        # Default targets .txt for stealer log credential extraction
        assert ".txt" in config.target_extensions
        assert config.extractor_path == "7z"


class TestDownloadConfig:
    """Tests for DownloadConfig dataclass."""

    def test_default_values(self):
        """Test default download config values."""
        config = DownloadConfig()

        assert config.max_retries == 3
        assert config.retry_delay_seconds == 5


class TestGetDefaultPaths:
    """Tests for path helper functions."""

    def test_default_config_path(self):
        """Test default config path is in user's config directory."""
        path = get_default_config_path()

        assert "telecrime" in str(path)
        assert "config.toml" in str(path)

    def test_default_config_path_respects_xdg(self):
        """Test XDG_CONFIG_HOME is respected."""
        with patch.dict(os.environ, {"XDG_CONFIG_HOME": "/custom/config"}):
            path = get_default_config_path()
            assert str(path).startswith("/custom/config")

    def test_default_data_dir(self):
        """Test default data directory."""
        path = get_default_data_dir()

        assert path.name == "data"


class TestApplyEnvVars:
    """Tests for _apply_env_vars function."""

    def test_database_url_from_env(self):
        """Test TELECRIME_DATABASE_URL environment variable."""
        config = Config()

        with patch.dict(
            os.environ,
            {"TELECRIME_DATABASE_URL": "postgresql://user:pass@db:5432/telecrime"},
        ):
            _apply_env_vars(config)

        assert config.database_url == "postgresql://user:pass@db:5432/telecrime"

    def test_data_dir_from_env(self):
        """Test TELECRIME_DATA_DIR environment variable."""
        config = Config()

        with patch.dict(os.environ, {"TELECRIME_DATA_DIR": "/custom/data"}):
            _apply_env_vars(config)

        assert config.data_dir == Path("/custom/data")
        assert config.downloads_dir == Path("/custom/data/downloads")
        assert config.extracted_dir == Path("/custom/data/extracted")

    def test_telegram_api_id_from_env(self):
        """Test TELECRIME_TELEGRAM_API_ID environment variable."""
        config = Config()

        with patch.dict(os.environ, {"TELECRIME_TELEGRAM_API_ID": "12345"}):
            _apply_env_vars(config)

        assert config.telegram.api_id == 12345

    def test_telegram_api_hash_from_env(self):
        """Test TELECRIME_TELEGRAM_API_HASH environment variable."""
        config = Config()

        with patch.dict(os.environ, {"TELECRIME_TELEGRAM_API_HASH": "TEST_API_HASH_0123456789abcdef"}):
            _apply_env_vars(config)

        assert config.telegram.api_hash == "TEST_API_HASH_0123456789abcdef"

    def test_target_extensions_from_env(self):
        """Test TELECRIME_TARGET_EXTENSIONS environment variable."""
        config = Config()

        with patch.dict(os.environ, {"TELECRIME_TARGET_EXTENSIONS": ".mobi, .azw3, .fb2"}):
            _apply_env_vars(config)

        assert ".mobi" in config.extraction.target_extensions
        assert ".azw3" in config.extraction.target_extensions
        assert ".fb2" in config.extraction.target_extensions

    def test_env_vars_override_defaults(self):
        """Test environment variables override default values."""
        config = Config()
        original_extensions = config.extraction.target_extensions.copy()

        with patch.dict(os.environ, {"TELECRIME_TARGET_EXTENSIONS": ".custom"}):
            _apply_env_vars(config)

        assert config.extraction.target_extensions != original_extensions
        assert ".custom" in config.extraction.target_extensions


class TestLoadConfig:
    """Tests for load_config function."""

    _PG_URL = "postgresql://telecrime:telecrime@db:5432/telecrime"

    def test_load_returns_config(self):
        with patch.dict(os.environ, {"TELECRIME_DATABASE_URL": self._PG_URL}):
            config = load_config()

        assert isinstance(config, Config)

    def test_load_with_nonexistent_path(self, tmp_path):
        config_path = tmp_path / "nonexistent.toml"
        with patch.dict(os.environ, {"TELECRIME_DATABASE_URL": self._PG_URL}):
            config = load_config(config_path)

        assert isinstance(config, Config)

    def test_load_applies_env_vars(self):
        with patch.dict(
            os.environ,
            {"TELECRIME_TELEGRAM_API_ID": "99999", "TELECRIME_DATABASE_URL": self._PG_URL},
        ):
            config = load_config()

        assert config.telegram.api_id == 99999

    def test_load_from_toml_file(self, tmp_path):
        config_path = tmp_path / "config.toml"
        config_path.write_text(f"""
database_url = "{self._PG_URL}"
data_dir = "{tmp_path / 'configured-data'}"

[telegram]
api_id = 12345
api_hash = "testhash"

[extraction]
target_extensions = [".epub", ".mobi"]
""")

        config = load_config(config_path)

        assert config.telegram.api_id == 12345
        assert config.telegram.api_hash == "testhash"
        assert ".epub" in config.extraction.target_extensions
        assert ".mobi" in config.extraction.target_extensions
        assert config.data_dir == tmp_path / "configured-data"
        assert config.downloads_dir == tmp_path / "configured-data" / "downloads"
        assert config.extracted_dir == tmp_path / "configured-data" / "extracted"

    def test_load_requires_database_url(self, tmp_path, monkeypatch):
        monkeypatch.delenv("TELECRIME_DATABASE_URL", raising=False)
        with pytest.raises(RuntimeError, match="database_url"):
            load_config(tmp_path / "missing.toml")
