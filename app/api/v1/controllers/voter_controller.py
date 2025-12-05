"""Voter controller for handling voter-related API endpoints."""

from typing import Optional

from fastapi import Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.core.voter_service import VoterService
from app.db import get_db
from app.schemas.voters import VoterListResponse


class VoterController:
    """Controller for voter operations."""
    
    @staticmethod
    def list_voters(
        state: str,
        assembly_constituency_number: str,
        # Search parameters
        search: Optional[str] = None,
        search_type: Optional[str] = None,
        # Filter parameters
        part_number: Optional[str] = None,
        polling_station: Optional[str] = None,
        gender: Optional[str] = None,
        # Sort parameters
        sort_by: Optional[str] = None,
        sort_order: str = "asc",
        # Pagination
        page: int = 1,
        page_size: int = 50,
        db: Session = Depends(get_db),
    ) -> VoterListResponse:
        """List voters with filtering and pagination."""
        service = VoterService(db)
        return service.list_voters(
            state=state,
            assembly_constituency_number=assembly_constituency_number,
            search=search,
            search_type=search_type,
            part_number=part_number,
            polling_station=polling_station,
            gender=gender,
            sort_by=sort_by,
            sort_order=sort_order,
            page=page,
            page_size=page_size,
        )
    
    @staticmethod
    def export_voters(
        state: str,
        assembly_constituency_number: str,
        # Search parameters
        search: Optional[str] = None,
        search_type: Optional[str] = None,
        # Filter parameters
        part_number: Optional[str] = None,
        polling_station: Optional[str] = None,
        gender: Optional[str] = None,
        # Sort parameters
        sort_by: Optional[str] = None,
        sort_order: str = "asc",
        db: Session = Depends(get_db),
    ) -> StreamingResponse:
        """Export voters to CSV format."""
        service = VoterService(db)
        
        try:
            csv_bytes, filename = service.export_voters_csv(
                state=state,
                assembly_constituency_number=assembly_constituency_number,
                search=search,
                search_type=search_type,
                part_number=part_number,
                polling_station=polling_station,
                gender=gender,
                sort_by=sort_by,
                sort_order=sort_order,
            )
            
            return StreamingResponse(
                csv_bytes,
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")
