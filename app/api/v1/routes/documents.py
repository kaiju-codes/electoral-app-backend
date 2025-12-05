"""Document routes."""

from fastapi import APIRouter

from app.api.v1.controllers.document_controller import DocumentController
from app.schemas.documents import DocumentDetail, DocumentListResponse, DocumentRead

router = APIRouter(prefix="/documents", tags=["documents"])

# Upload document
router.post("/upload", response_model=DocumentRead)(DocumentController.upload_document)

# List documents
router.get("", response_model=DocumentListResponse)(DocumentController.list_documents)

# Get single document
router.get("/{document_id}", response_model=DocumentDetail)(DocumentController.get_document)
