from datetime import datetime

from pydantic import BaseModel

from app.models.core import ExtractionRunStatus, SegmentStatus, SegmentType


class ExtractionSegmentRead(BaseModel):
    id: int
    segment_type: SegmentType
    page_start: int
    page_end: int
    status: SegmentStatus
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ExtractionRunRead(BaseModel):
    id: int
    document_id: int
    status: ExtractionRunStatus
    started_at: datetime | None
    finished_at: datetime | None
    error_message: str | None
    segments: list[ExtractionSegmentRead]

    class Config:
        from_attributes = True


class ExtractionRunListResponse(BaseModel):
    """Response for GET /extraction-runs list endpoint."""

    items: list[ExtractionRunRead]
    total: int
    page: int
    page_size: int

    class Config:
        from_attributes = True


class MetricsSummary(BaseModel):
    """Metrics summary for Phase 7 observability."""

    total_documents: int
    total_extraction_runs: int
    completed_runs: int
    partial_runs: int
    failed_runs: int
    total_voters: int
    avg_voters_per_document: float
    total_segments: int
    failed_segments: int
    gemini_error_rate: float  # Percentage of failed segments
    avg_extraction_time_seconds: float | None  # Average time for completed runs
    total_extraction_time_seconds: float | None

    class Config:
        from_attributes = True


class SegmentRetryResponse(BaseModel):
    """Response for segment retry operations."""
    
    message: str
    segment_id: int
    document_id: int
    extraction_run_id: int


class NonRetryableSegment(BaseModel):
    """Information about a segment that cannot be retried."""
    
    segment_id: int
    reason: str


class BulkRetryResponse(BaseModel):
    """Response for bulk segment retry operations."""
    
    message: str
    document_id: int
    failed_segments_count: int
    retryable_segments_count: int
    non_retryable_segments_count: int
    retryable_segment_ids: list[int] | None = None
    non_retryable_reasons: list[NonRetryableSegment] | None = None


class SegmentRetryStatusResponse(BaseModel):
    """Response for segment retry status check."""
    
    segment_id: int
    status: SegmentStatus
    can_retry: bool
    reason: str
    last_updated: str
    hours_since_failure: float
    hours_remaining_for_retry: float
    retry_deadline: str


