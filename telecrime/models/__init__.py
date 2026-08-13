"""Database models for Telecrime.

This module uses lazy attribute loading so importing one model from
`telecrime.models` does not depend on eagerly importing every other model.
That avoids partial-initialization issues during package import.
"""

from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Static view for type checkers — mypy cannot see through the lazy
    # __getattr__ loader below. These must mirror _MODEL_IMPORTS.
    from telecrime.models.archive_group import ArchiveGroup, ArchiveGroupPart
    from telecrime.models.artifact import DownloadArtifact
    from telecrime.models.attachment import FileAttachment
    from telecrime.models.base import Base
    from telecrime.models.channel import TelegramChannel
    from telecrime.models.conversation import Conversation
    from telecrime.models.credential import ParsedCredential
    from telecrime.models.extraction import ExtractedOutput, ExtractionJob
    from telecrime.models.first_seen import FirstSeenIndex
    from telecrime.models.message import Message
    from telecrime.models.password import PasswordCandidate
    from telecrime.models.pipeline_run import PipelineRun
    from telecrime.models.pipeline_state import PipelineState
    from telecrime.models.system_info import SystemInfoRecord
    from telecrime.models.watchlist import WatchlistItem

_MODEL_IMPORTS = {
    "Base": ("telecrime.models.base", "Base"),
    "Conversation": ("telecrime.models.conversation", "Conversation"),
    "Message": ("telecrime.models.message", "Message"),
    "FileAttachment": ("telecrime.models.attachment", "FileAttachment"),
    "DownloadArtifact": ("telecrime.models.artifact", "DownloadArtifact"),
    "ArchiveGroup": ("telecrime.models.archive_group", "ArchiveGroup"),
    "ArchiveGroupPart": ("telecrime.models.archive_group", "ArchiveGroupPart"),
    "PasswordCandidate": ("telecrime.models.password", "PasswordCandidate"),
    "ExtractionJob": ("telecrime.models.extraction", "ExtractionJob"),
    "ExtractedOutput": ("telecrime.models.extraction", "ExtractedOutput"),
    "FirstSeenIndex": ("telecrime.models.first_seen", "FirstSeenIndex"),
    "ParsedCredential": ("telecrime.models.credential", "ParsedCredential"),
    "TelegramChannel": ("telecrime.models.channel", "TelegramChannel"),
    "PipelineState": ("telecrime.models.pipeline_state", "PipelineState"),
    "PipelineRun": ("telecrime.models.pipeline_run", "PipelineRun"),
    "WatchlistItem": ("telecrime.models.watchlist", "WatchlistItem"),
    "SystemInfoRecord": ("telecrime.models.system_info", "SystemInfoRecord"),
}

__all__ = list(_MODEL_IMPORTS)


def _load_all_models() -> None:
    """Import all model modules once so SQLAlchemy relationships can resolve."""
    if globals().get("_ALL_MODELS_LOADED"):
        return

    for export_name, (module_name, attr_name) in _MODEL_IMPORTS.items():
        value = getattr(import_module(module_name), attr_name)
        globals()[export_name] = value

    globals()["_ALL_MODELS_LOADED"] = True


def __getattr__(name: str) -> object:
    """Lazily import model classes on first access."""
    try:
        module_name, attr_name = _MODEL_IMPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc

    _load_all_models()
    # _load_all_models() should have populated globals() — fall back to a direct
    # import if (due to reentrancy or a partial load) the name is still missing.
    value = globals().get(name)
    if value is None:
        value = getattr(import_module(module_name), attr_name)
        globals()[name] = value
    return value
