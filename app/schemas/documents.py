from datetime import datetime

from pydantic import BaseModel


class DocumentCreate(BaseModel):
    original_filename: str


class DocumentRead(BaseModel):
    id: int
    original_filename: str
    upload_file_uri: str | None
    mime_type: str | None
    page_count: int | None
    page_size_kb: int | None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class DocumentHeaderSummary(BaseModel):
    state: str | None = None
    part_number: int | None = None
    language: str | None = None
    assembly_constituency_number_local: str | None = None
    assembly_constituency_number_english: int | None = None
    assembly_constituency_name_local: str | None = None
    assembly_constituency_name_english: str | None = None
    polling_station_number_local: str | None = None
    polling_station_number_english: int | None = None
    polling_station_name_local: str | None = None
    polling_station_name_english: str | None = None
    polling_station_building_and_address_local: str | None = None
    polling_station_building_and_address_english: str | None = None


class DocumentSectionRead(BaseModel):
    id: int
    document_id: int
    section_id: int
    section_name_local: str | None
    section_name_english: str | None
    start_serial_number: int | None

    class Config:
        from_attributes = True


class DocumentDetail(DocumentRead):
    header: DocumentHeaderSummary | None = None
    sections: list[DocumentSectionRead] | None = None
    voter_count: int
    # Latest extraction run information
    latest_run_status: str | None = None
    latest_run_error_message: str | None = None


class DocumentListResponse(BaseModel):
    """Response for GET /documents list endpoint."""

    items: list[DocumentDetail]
    total: int
    page: int
    page_size: int

