"""Document service for handling document-related business logic."""

import io
import logging
from datetime import datetime
from typing import List, Optional

from pypdf import PdfReader
from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from app.config import get_settings
from app.models.core import Document, DocumentHeader, DocumentSection, ExtractionRun, ExtractionRunStatus, Voter
from app.schemas.documents import DocumentDetail, DocumentListResponse, DocumentRead, DocumentSectionRead
from app.services.gemini_client import upload_file
from app.core.extraction_service import ExtractionService

settings = get_settings()
logger = logging.getLogger(__name__)


class DocumentService:
    """Service for document operations."""
    
    def __init__(self, db: Session):
        self.db = db
        self.extraction_service = ExtractionService(db)
    
    def count_pdf_pages(self, pdf_bytes: bytes) -> int:
        """Count the number of pages in a PDF from bytes."""
        try:
            pdf_stream = io.BytesIO(pdf_bytes)
            reader = PdfReader(pdf_stream)
            page_count = len(reader.pages)
            logger.info(f"PDF page count detected: {page_count} pages")
            return page_count
        except Exception as exc:
            logger.error(
                f"Failed to count PDF pages: error={type(exc).__name__}: {str(exc)}",
                exc_info=True,
            )
            return 0
    
    def create_document(self, filename: str, file_bytes: bytes) -> Document:
        """Create a new document record."""
        page_size_kb = max(len(file_bytes) // 1024, 1)
        page_count = self.count_pdf_pages(file_bytes)
        
        if page_count == 0:
            raise ValueError("Failed to read PDF or PDF is empty/invalid.")
        
        document = Document(
            original_filename=filename,
            page_size_kb=page_size_kb,
            page_count=page_count,
        )
        self.db.add(document)
        self.db.commit()
        self.db.refresh(document)
        
        return document
    
    def get_document(self, document_id: int) -> Optional[Document]:
        """Get a document by ID."""
        return self.db.get(Document, document_id)
    
    def get_document_detail(self, document_id: int) -> Optional[DocumentDetail]:
        """Get detailed document information."""
        document = self.db.get(Document, document_id)
        if not document:
            return None
        
        header = document.header
        voter_count = self.db.query(Voter).filter(Voter.document_id == document.id).count()
        
        # Get latest extraction run information
        latest_run = self.db.query(ExtractionRun).filter(
            ExtractionRun.document_id == document.id
        ).order_by(ExtractionRun.created_at.desc()).first()
        
        latest_run_status = latest_run.status.value if latest_run else None
        latest_run_error_message = latest_run.error_message if latest_run else None
        
        header_summary = None
        if header:
            header_summary = {
                "state": header.state,
                "part_number": header.part_number,
                "language": header.language,
                "assembly_constituency_number_local": header.assembly_constituency_number_local,
                "assembly_constituency_number_english": header.assembly_constituency_number_english,
                "assembly_constituency_name_local": header.assembly_constituency_name_local,
                "assembly_constituency_name_english": header.assembly_constituency_name_english,
                "polling_station_number_local": header.polling_station_number_local,
                "polling_station_number_english": header.polling_station_number_english,
                "polling_station_name_local": header.polling_station_name_local,
                "polling_station_name_english": header.polling_station_name_english,
                "polling_station_building_and_address_local": header.polling_station_building_and_address_local,
                "polling_station_building_and_address_english": header.polling_station_building_and_address_english,
            }
        
        # Get sections for this document, ordered by start_serial_number (ascending), then by section_id
        # This ensures sections are displayed in serial number order, showing all occurrences
        # Only include sections with start_serial_number (no NULL values)
        sections = self.db.query(DocumentSection).filter(
            DocumentSection.document_id == document_id,
            DocumentSection.start_serial_number.isnot(None)
        ).order_by(
            DocumentSection.start_serial_number.asc(),
            DocumentSection.section_id
        ).all()
        sections_list = [DocumentSectionRead.model_validate(sec) for sec in sections] if sections else None
        
        base = DocumentRead.model_validate(document)
        return DocumentDetail(
            **base.model_dump(),
            header=header_summary,
            sections=sections_list,
            voter_count=voter_count,
            latest_run_status=latest_run_status,
            latest_run_error_message=latest_run_error_message,
        )
    
    def list_documents(
        self,
        state: Optional[str] = None,
        assembly_constituency: Optional[str] = None,
        part_number: Optional[str] = None,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "desc",
        page: int = 1,
        page_size: int = 20,
    ) -> DocumentListResponse:
        """List documents with filtering and pagination."""
        query = self.db.query(Document).options(joinedload(Document.header))
        
        # Apply filters
        if state:
            query = query.join(DocumentHeader, Document.id == DocumentHeader.document_id).filter(
                DocumentHeader.state == state
            )
        
        if assembly_constituency:
            if not state:  # Join if not already joined
                query = query.join(DocumentHeader, Document.id == DocumentHeader.document_id)
            query = query.filter(
                (DocumentHeader.assembly_constituency_name_english.ilike(f"%{assembly_constituency}%"))
                | (DocumentHeader.assembly_constituency_name_local.ilike(f"%{assembly_constituency}%"))
                | (DocumentHeader.assembly_constituency_number_english == assembly_constituency)
                | (DocumentHeader.assembly_constituency_number_local == assembly_constituency)
            )
        
        if part_number:
            if not state and not assembly_constituency:  # Join if not already joined
                query = query.join(DocumentHeader, Document.id == DocumentHeader.document_id)
            query = query.filter(DocumentHeader.part_number == part_number)
        
        # Apply search
        if search and search.strip():
            search_term = f"%{search.strip()}%"
            query = query.filter(Document.original_filename.ilike(search_term))
        
        # Get total count before pagination
        total = query.count()
        
        # Apply sorting
        if sort_by == "created_at":
            order_field = Document.created_at
        else:
            order_field = Document.created_at  # default
        
        if sort_order == "asc":
            order_field = order_field.asc()
        else:
            order_field = order_field.desc()
        
        # Apply pagination
        offset = (page - 1) * page_size
        documents = query.order_by(order_field).offset(offset).limit(page_size).all()
        
        # Create DocumentDetail objects with header information
        items = []
        for doc in documents:
            header_summary = None
            if doc.header:
                header_summary = {
                    "state": doc.header.state,
                    "part_number": doc.header.part_number,
                    "language": doc.header.language,
                    "assembly_constituency_number_local": doc.header.assembly_constituency_number_local,
                    "assembly_constituency_number_english": doc.header.assembly_constituency_number_english,
                    "assembly_constituency_name_local": doc.header.assembly_constituency_name_local,
                    "assembly_constituency_name_english": doc.header.assembly_constituency_name_english,
                    "polling_station_number_local": doc.header.polling_station_number_local,
                    "polling_station_number_english": doc.header.polling_station_number_english,
                    "polling_station_name_local": doc.header.polling_station_name_local,
                    "polling_station_name_english": doc.header.polling_station_name_english,
                    "polling_station_building_and_address_local": doc.header.polling_station_building_and_address_local,
                    "polling_station_building_and_address_english": doc.header.polling_station_building_and_address_english,
                }
            
            # Get voter count for this document
            voter_count = self.db.query(Voter).filter(Voter.document_id == doc.id).count()
            
            # Get latest extraction run information
            latest_run = self.db.query(ExtractionRun).filter(
                ExtractionRun.document_id == doc.id
            ).order_by(ExtractionRun.created_at.desc()).first()
            
            latest_run_status = latest_run.status.value if latest_run else None
            latest_run_error_message = latest_run.error_message if latest_run else None
            
            base = DocumentRead.model_validate(doc)
            detail = DocumentDetail(
                **base.model_dump(),
                header=header_summary,
                voter_count=voter_count,
                latest_run_status=latest_run_status,
                latest_run_error_message=latest_run_error_message,
            )
            items.append(detail)
        
        logger.info(
            f"Documents list: page={page}, page_size={page_size}, "
            f"total={total}, returned={len(items)}"
        )
        
        return DocumentListResponse(
            items=items,
            total=total,
            page=page,
            page_size=page_size,
        )
    
    async def process_document(self, document_id: int, file_bytes: bytes, filename: str, mime_type: str) -> None:
        """Process document for extraction in background."""
        await self.extraction_service.run_extraction_for_document(
            document_id, file_bytes, filename, mime_type
        )
