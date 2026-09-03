"""Pipeline stages for Telecrime."""

from telecrime.pipeline.acquire import AcquireStage
from telecrime.pipeline.discover import DiscoverStage
from telecrime.pipeline.enrich import EnrichStage
from telecrime.pipeline.extract import ExtractStage
from telecrime.pipeline.finalize import FinalizeStage
from telecrime.pipeline.ingest import IngestStage
from telecrime.pipeline.orchestrator import (
    Pipeline,
    PipelineContext,
    PipelineStage,
    create_default_pipeline,
)
from telecrime.pipeline.parse import ParseStage
from telecrime.pipeline.plan import PlanStage

__all__ = [
    "AcquireStage",
    "DiscoverStage",
    "EnrichStage",
    "ExtractStage",
    "FinalizeStage",
    "IngestStage",
    "ParseStage",
    "Pipeline",
    "PipelineContext",
    "PipelineStage",
    "PlanStage",
    "create_default_pipeline",
]
