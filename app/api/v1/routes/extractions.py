"""Extraction routes."""

from fastapi import APIRouter

from app.api.v1.controllers.extraction_controller import ExtractionController
from app.schemas.extraction import (
    BulkRetryResponse,
    ExtractionRunListResponse,
    ExtractionRunRead,
    MetricsSummary,
    SegmentRetryResponse,
    SegmentRetryStatusResponse,
)

router = APIRouter(tags=["extractions"])

# Extraction runs
extraction_runs_router = APIRouter(prefix="/extraction-runs")
extraction_runs_router.get("", response_model=ExtractionRunListResponse)(ExtractionController.list_extraction_runs)
extraction_runs_router.get("/{run_id}", response_model=ExtractionRunRead)(ExtractionController.get_extraction_run)

# Metrics
metrics_router = APIRouter(prefix="/metrics")
metrics_router.get("", response_model=MetricsSummary)(ExtractionController.get_metrics)

# Segment retry operations
segments_router = APIRouter(prefix="/segments")
segments_router.post("/{segment_id}/retry", response_model=SegmentRetryResponse)(ExtractionController.retry_segment)
segments_router.get("/{segment_id}/retry-status", response_model=SegmentRetryStatusResponse)(ExtractionController.get_segment_retry_status)

# Document retry operations
documents_retry_router = APIRouter(prefix="/documents")
documents_retry_router.post("/{document_id}/retry-failed-segments", response_model=BulkRetryResponse)(ExtractionController.retry_failed_segments_for_document)

# Include all routers
router.include_router(extraction_runs_router)
router.include_router(metrics_router)
router.include_router(segments_router)
router.include_router(documents_retry_router)
