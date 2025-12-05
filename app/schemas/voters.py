from datetime import datetime

from pydantic import BaseModel

from app.schemas.documents import DocumentSectionRead


class VoterRead(BaseModel):
    """Voter read schema with flattened fields."""

    id: int
    document_id: int
    serial_number: int | None
    house_number: str | None
    voter_name_local: str | None
    voter_name_english: str | None
    relation_type: str | None
    relation_name_local: str | None
    relation_name_english: str | None
    gender: str | None
    age: int | None
    photo_id: str | None
    # Header context for filtering/search
    state: str | None
    assembly_constituency_number_english: int | None
    assembly_constituency_name_english: str | None
    part_number: int | None
    # Document section information
    document_section: DocumentSectionRead | None = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class VoterListResponse(BaseModel):
    """Response for GET /voters list endpoint."""

    items: list[VoterRead]
    total: int
    page: int
    page_size: int

    class Config:
        from_attributes = True

