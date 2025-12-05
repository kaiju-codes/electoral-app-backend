"""Document controller for handling document-related API endpoints."""

from typing import Optional

from fastapi import BackgroundTasks, Depends, File, HTTPException, UploadFile
from sqlalchemy.orm import Session

from app.core.document_service import DocumentService
from app.db import get_db
from app.schemas.documents import DocumentDetail, DocumentListResponse, DocumentRead


class DocumentController:
    """Controller for document operations."""
    
    @staticmethod
    async def upload_document(
        background_tasks: BackgroundTasks,
        file: UploadFile = File(...),
        db: Session = Depends(get_db),
    ) -> DocumentRead:
        """Upload a new document."""
        if file.content_type != "application/pdf":
            raise HTTPException(status_code=400, detail="Only PDF files are supported.")
        
        contents = await file.read()
        if not contents:
            raise HTTPException(status_code=400, detail="Empty file uploaded.")
        
        service = DocumentService(db)
        
        try:
            document = service.create_document(file.filename, contents)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        
        # Process document in background
        background_tasks.add_task(
            service.process_document,
            document.id,
            contents,
            file.filename,
            file.content_type or "application/pdf",
        )
        
        return DocumentRead.model_validate(document)
    
    @staticmethod
    def list_documents(
        # Filter parameters
        state: Optional[str] = None,
        assembly_constituency: Optional[str] = None,
        part_number: Optional[str] = None,
        # Search parameters
        search: Optional[str] = None,
        # Sort parameters
        sort_by: Optional[str] = None,
        sort_order: str = "desc",
        # Pagination
        page: int = 1,
        page_size: int = 20,
        db: Session = Depends(get_db),
    ) -> DocumentListResponse:
        """List documents with filtering and pagination."""
        service = DocumentService(db)
        return service.list_documents(
            state=state,
            assembly_constituency=assembly_constituency,
            part_number=part_number,
            search=search,
            sort_by=sort_by,
            sort_order=sort_order,
            page=page,
            page_size=page_size,
        )
    
    @staticmethod
    def get_document(document_id: int, db: Session = Depends(get_db)) -> DocumentDetail:
        """Get a single document by ID."""
        service = DocumentService(db)
        document = service.get_document_detail(document_id)
        if not document:
            raise HTTPException(status_code=404, detail="Document not found.")
        return document
