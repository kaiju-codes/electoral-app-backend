"""Extraction controller for handling extraction-related API endpoints."""

from typing import Optional

from fastapi import Depends, HTTPException
from sqlalchemy.orm import Session

from app.core.extraction_service import ExtractionService
from app.db import get_db
from app.models.core import ExtractionRunStatus, ExtractionSegment
from app.schemas.extraction import (
    BulkRetryResponse,
    ExtractionRunListResponse,
    ExtractionRunRead,
    MetricsSummary,
    SegmentRetryResponse,
    SegmentRetryStatusResponse,
)


class ExtractionController:
    """Controller for extraction operations."""
    
    @staticmethod
    def list_extraction_runs(
        document_id: Optional[int] = None,
        status: Optional[ExtractionRunStatus] = None,
        page: int = 1,
        page_size: int = 20,
        db: Session = Depends(get_db),
    ) -> ExtractionRunListResponse:
        """List extraction runs with pagination and optional filters."""
        service = ExtractionService(db)
        return service.list_extraction_runs(
            document_id=document_id,
            status=status,
            page=page,
            page_size=page_size,
        )
    
    @staticmethod
    def get_extraction_run(run_id: int, db: Session = Depends(get_db)) -> ExtractionRunRead:
        """Get a single extraction run."""
        service = ExtractionService(db)
        run = service.get_extraction_run(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Extraction run not found.")
        return run
    
    @staticmethod
    def get_metrics(db: Session = Depends(get_db)) -> MetricsSummary:
        """Get metrics summary."""
        service = ExtractionService(db)
        return service.get_metrics()
    
    @staticmethod
    async def retry_segment(
        segment_id: int,
        db: Session = Depends(get_db)
    ) -> SegmentRetryResponse:
        """Retry processing of a single failed segment."""
        service = ExtractionService(db)
        
        try:
            return await service.retry_segment(segment_id)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
    
    @staticmethod
    async def retry_failed_segments_for_document(
        document_id: int,
        db: Session = Depends(get_db)
    ) -> BulkRetryResponse:
        """Retry all failed segments for a specific document."""
        service = ExtractionService(db)
        
        try:
            return await service.retry_failed_segments_for_document(document_id)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
    
    @staticmethod
    def get_segment_retry_status(segment_id: int, db: Session = Depends(get_db)) -> SegmentRetryStatusResponse:
        """Check if a segment can be retried and get retry status information."""
        service = ExtractionService(db)
        
        try:
            return service.get_segment_retry_status(segment_id)
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
