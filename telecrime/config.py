"""Configuration management for Telecrime."""

import dataclasses
import os

# tomllib is in stdlib since Python 3.11 (project requires-python = ">=3.11").
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ConfigDict = dict[str, Any]

try:
    import tomli_w
except ImportError:
    tomli_w = None  # type: ignore


def get_default_config_path() -> Path:
    """Get the default config file path."""
    xdg_config = os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")
    return Path(xdg_config) / "telecrime" / "config.toml"


def get_default_data_dir() -> Path:
    """Get the default data directory."""
    return Path(__file__).parent.parent / "data"


@dataclass
class TelegramConfig:
    """Telegram API configuration."""

    api_id: int | None = None
    api_hash: str | None = None
    session_name: str = "telecrime"
    phone: str | None = None
    # Separate session for housekeeping jobs so they don't wait on the pipeline.
    aux_session_name: str | None = None
    # Additional session files used for parallel downloads. Each session is a
    # separate Telegram account, so it has its own download speed budget
    # (~2 MB/s each). Configure via TELECRIME_DOWNLOAD_SESSIONS (comma list).
    download_session_names: list[str] = field(default_factory=list)


@dataclass
class ExtractionConfig:
    """Extraction settings."""

    # Only extract text files that may contain credentials
    # Stealer logs store credentials in .txt files (Passwords.txt, etc.)
    target_extensions: list[str] = field(default_factory=lambda: [".txt"])
    extractor_path: str = "7z"  # Path to 7-Zip executable
    max_extraction_seconds: int = 600  # Timeout per archive extraction
    min_free_disk_mb: int = 10240  # Minimum free disk space required (10 GB)
    scheduler_min_free_disk_gb: float = 10.0  # Scheduler skip threshold (GB)


@dataclass
class DownloadConfig:
    """Download settings."""

    verify_hash: bool = True
    max_retries: int = 3
    retry_delay_seconds: int = 5
    # Number of archives to pre-download while current archive is being processed.
    # Max 3 (higher values risk Telegram rate-limiting / temporary ban).
    prefetch_count: int = 2
    # Number of parallel upload.getFile chunk streams used per large download.
    # Telegram throttles a single sequential stream (~1-2 MB/s); parallel chunk
    # striping multiplies throughput (measured ~5-7 MB/s with 8 streams on a
    # premium account). 1 disables parallel downloads entirely.
    parallel_chunks: int = 8
    # Files smaller than this are downloaded sequentially (overhead not worth it).
    parallel_min_bytes: int = 4 * 1024 * 1024


@dataclass
class Config:
    """Main configuration for Telecrime."""

    # Paths
    database_url: str = ""
    data_dir: Path = field(default_factory=get_default_data_dir)
    downloads_dir: Path = field(default_factory=lambda: get_default_data_dir() / "downloads")
    extracted_dir: Path = field(default_factory=lambda: get_default_data_dir() / "extracted")

    # Sub-configs
    telegram: TelegramConfig = field(default_factory=TelegramConfig)
    extraction: ExtractionConfig = field(default_factory=ExtractionConfig)
    download: DownloadConfig = field(default_factory=DownloadConfig)

    def __post_init__(self) -> None:
        """Keep derived paths alongside a custom data directory.

        Explicit download/extraction paths remain untouched, while the default
        paths follow ``data_dir`` when it is supplied by a config file or test.
        """
        default_data_dir = get_default_data_dir()
        if self.data_dir != default_data_dir:
            if self.downloads_dir == default_data_dir / "downloads":
                self.downloads_dir = self.data_dir / "downloads"
            if self.extracted_dir == default_data_dir / "extracted":
                self.extracted_dir = self.data_dir / "extracted"

    def ensure_directories(self) -> None:
        """Create all required directories."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.downloads_dir.mkdir(parents=True, exist_ok=True)
        self.extracted_dir.mkdir(parents=True, exist_ok=True)

    def with_aux_telegram_session(self) -> "Config":
        """Return a copy that points at the aux Telegram session, if one is set."""
        if not self.telegram.aux_session_name:
            return self
        new_telegram = dataclasses.replace(
            self.telegram, session_name=self.telegram.aux_session_name
        )
        return dataclasses.replace(self, telegram=new_telegram)

    def with_download_session(self, index: int) -> "Config":
        """Return a copy pointing at the (index)-th parallel download session.

        index 0 is the main session; 1..N map to download_session_names.
        """
        names = self.telegram.download_session_names
        if index == 0 or not names or index > len(names):
            return self
        new_telegram = dataclasses.replace(
            self.telegram, session_name=names[index - 1]
        )
        return dataclasses.replace(self, telegram=new_telegram)


def load_config(config_path: Path | None = None) -> Config:
    """Load configuration from file and environment.

    Priority (highest to lowest):
    1. Environment variables (TELECRIME_*)
    2. Config file
    3. Defaults
    """
    config = Config()

    # Load from file if exists
    if config_path is None:
        config_path = get_default_config_path()

    if config_path.exists():
        with open(config_path, "rb") as f:
            data = tomllib.load(f)
            _apply_config_dict(config, data)

    # Override with environment variables
    _apply_env_vars(config)

    if not config.database_url:
        raise RuntimeError(
            "database_url must be set (env var TELECRIME_DATABASE_URL or "
            "config file). SQLite is no longer supported; use a "
            "postgresql:// URL."
        )

    return config


def _apply_config_dict(config: Config, data: ConfigDict) -> None:
    """Apply dictionary values to config."""
    if "database_url" in data:
        config.database_url = data["database_url"]
    if "data_dir" in data:
        _set_data_dir(config, Path(data["data_dir"]))

    if "telegram" in data:
        tg = data["telegram"]
        if "api_id" in tg:
            config.telegram.api_id = tg["api_id"]
        if "api_hash" in tg:
            config.telegram.api_hash = tg["api_hash"]
        if "session_name" in tg:
            config.telegram.session_name = tg["session_name"]
        if "phone" in tg:
            config.telegram.phone = tg["phone"]
        if "aux_session_name" in tg:
            config.telegram.aux_session_name = tg["aux_session_name"]

    if "extraction" in data:
        ext = data["extraction"]
        if "target_extensions" in ext:
            config.extraction.target_extensions = ext["target_extensions"]
        if "extractor_path" in ext:
            config.extraction.extractor_path = ext["extractor_path"]
        if "max_extraction_seconds" in ext:
            config.extraction.max_extraction_seconds = ext["max_extraction_seconds"]
        if "min_free_disk_mb" in ext:
            config.extraction.min_free_disk_mb = ext["min_free_disk_mb"]
        if "scheduler_min_free_disk_gb" in ext:
            config.extraction.scheduler_min_free_disk_gb = ext["scheduler_min_free_disk_gb"]

    if "download" in data:
        dl = data["download"]
        if "max_retries" in dl:
            config.download.max_retries = dl["max_retries"]


def _apply_env_vars(config: Config) -> None:
    """Override config with environment variables."""
    if url := os.environ.get("TELECRIME_DATABASE_URL"):
        config.database_url = url
    if data_dir := os.environ.get("TELECRIME_DATA_DIR"):
        _set_data_dir(config, Path(data_dir))
    if api_id := os.environ.get("TELECRIME_TELEGRAM_API_ID"):
        config.telegram.api_id = int(api_id)
    if api_hash := os.environ.get("TELECRIME_TELEGRAM_API_HASH"):
        config.telegram.api_hash = api_hash
    if aux := os.environ.get("TELECRIME_TELEGRAM_AUX_SESSION_NAME"):
        config.telegram.aux_session_name = aux
    if sessions := os.environ.get("TELECRIME_DOWNLOAD_SESSIONS"):
        config.telegram.download_session_names = [
            s.strip() for s in sessions.split(",") if s.strip()
        ]
    if extensions := os.environ.get("TELECRIME_TARGET_EXTENSIONS"):
        config.extraction.target_extensions = [e.strip() for e in extensions.split(",")]
    if max_seconds := os.environ.get("TELECRIME_MAX_EXTRACTION_SECONDS"):
        config.extraction.max_extraction_seconds = int(max_seconds)
    if min_free := os.environ.get("TELECRIME_MIN_FREE_DISK_MB"):
        config.extraction.min_free_disk_mb = int(min_free)
    if scheduler_min_free := os.environ.get("TELECRIME_SCHEDULER_MIN_FREE_DISK_GB"):
        config.extraction.scheduler_min_free_disk_gb = float(scheduler_min_free)
    if parallel_chunks := os.environ.get("TELECRIME_PARALLEL_CHUNKS"):
        config.download.parallel_chunks = int(parallel_chunks)
    if parallel_min := os.environ.get("TELECRIME_PARALLEL_MIN_BYTES"):
        config.download.parallel_min_bytes = int(parallel_min)


def _set_data_dir(config: Config, data_dir: Path) -> None:
    """Set data_dir and update only paths that still use its derived defaults."""
    if config.downloads_dir == config.data_dir / "downloads":
        config.downloads_dir = data_dir / "downloads"
    if config.extracted_dir == config.data_dir / "extracted":
        config.extracted_dir = data_dir / "extracted"
    config.data_dir = data_dir


def _remove_none_values(d: ConfigDict) -> ConfigDict:
    """Recursively remove None values from a dictionary."""
    result = {}
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, dict):
            v = _remove_none_values(v)
            if v:  # Only include non-empty dicts
                result[k] = v
        else:
            result[k] = v
    return result


def save_config(config: Config, config_path: Path | None = None) -> None:
    """Save configuration to file."""
    if tomli_w is None:
        raise RuntimeError("tomli_w not installed, cannot save config")

    if config_path is None:
        config_path = get_default_config_path()

    config_path.parent.mkdir(parents=True, exist_ok=True)

    data = {
        "database_url": config.database_url,
        "data_dir": str(config.data_dir),
        "telegram": {
            "api_id": config.telegram.api_id,
            "api_hash": config.telegram.api_hash,
            "session_name": config.telegram.session_name,
            "phone": config.telegram.phone,
            "aux_session_name": config.telegram.aux_session_name,
            "download_session_names": config.telegram.download_session_names,
        },
        "extraction": {
            "target_extensions": config.extraction.target_extensions,
            "extractor_path": config.extraction.extractor_path,
            "max_extraction_seconds": config.extraction.max_extraction_seconds,
            "min_free_disk_mb": config.extraction.min_free_disk_mb,
            "scheduler_min_free_disk_gb": config.extraction.scheduler_min_free_disk_gb,
        },
        "download": {
            "max_retries": config.download.max_retries,
            "parallel_chunks": config.download.parallel_chunks,
            "parallel_min_bytes": config.download.parallel_min_bytes,
        },
    }

    # Remove None values (not serializable to TOML)
    data = _remove_none_values(data)

    with open(config_path, "wb") as f:
        tomli_w.dump(data, f)
