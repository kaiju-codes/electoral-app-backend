import enum
from datetime import datetime

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


class ExtractionRunStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"
    SKIPPED = "SKIPPED"


class SegmentType(str, enum.Enum):
    HEADER = "HEADER"
    LIST_CHUNK = "LIST_CHUNK"


class SegmentStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )


class Document(Base, TimestampMixin):
    __tablename__ = "documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    original_filename: Mapped[str] = mapped_column(String(length=512))
    upload_file_uri: Mapped[str | None] = mapped_column(String(length=1024), nullable=True)
    mime_type: Mapped[str | None] = mapped_column(String(length=255), nullable=True)
    page_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    page_size_kb: Mapped[int | None] = mapped_column(Integer, nullable=True)

    extraction_runs: Mapped[list["ExtractionRun"]] = relationship(
        back_populates="document", cascade="all, delete-orphan"
    )
    header: Mapped["DocumentHeader"] = relationship(
        back_populates="document", uselist=False, cascade="all, delete-orphan"
    )
    voters: Mapped[list["Voter"]] = relationship(
        back_populates="document", cascade="all, delete-orphan"
    )
    sections: Mapped[list["DocumentSection"]] = relationship(
        back_populates="document", cascade="all, delete-orphan"
    )


class ExtractionRun(Base, TimestampMixin):
    __tablename__ = "extraction_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    document_id: Mapped[int] = mapped_column(ForeignKey("documents.id"), nullable=False)
    status: Mapped[ExtractionRunStatus] = mapped_column(
        Enum(ExtractionRunStatus),
        default=ExtractionRunStatus.PENDING,
        nullable=False,
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_message: Mapped[str | None] = mapped_column(String(length=2000), nullable=True)

    document: Mapped[Document] = relationship(back_populates="extraction_runs")
    segments: Mapped[list["ExtractionSegment"]] = relationship(
        back_populates="extraction_run", cascade="all, delete-orphan"
    )


class ExtractionSegment(Base, TimestampMixin):
    __tablename__ = "extraction_segments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    extraction_run_id: Mapped[int] = mapped_column(
        ForeignKey("extraction_runs.id"), nullable=False
    )
    segment_type: Mapped[SegmentType] = mapped_column(Enum(SegmentType), nullable=False)
    page_start: Mapped[int] = mapped_column(Integer, nullable=False)
    page_end: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[SegmentStatus] = mapped_column(
        Enum(SegmentStatus), default=SegmentStatus.PENDING, nullable=False
    )
    raw_response_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    parsed_header_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    parsed_list_json: Mapped[list[dict] | None] = mapped_column(JSON, nullable=True)

    extraction_run: Mapped[ExtractionRun] = relationship(back_populates="segments")


class DocumentHeader(Base, TimestampMixin):
    __tablename__ = "document_header"

    document_id: Mapped[int] = mapped_column(
        ForeignKey("documents.id"), primary_key=True
    )
    state: Mapped[str | None] = mapped_column(String(length=255))
    part_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    language: Mapped[str | None] = mapped_column(String(length=50))
    assembly_constituency_number_local: Mapped[str | None] = mapped_column(
        String(length=255)
    )
    assembly_constituency_number_english: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    assembly_constituency_name_local: Mapped[str | None] = mapped_column(
        String(length=255)
    )
    assembly_constituency_name_english: Mapped[str | None] = mapped_column(
        String(length=255)
    )
    polling_station_number_local: Mapped[str | None] = mapped_column(
        String(length=255)
    )
    polling_station_number_english: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    polling_station_name_local: Mapped[str | None] = mapped_column(
        String(length=255)
    )
    polling_station_name_english: Mapped[str | None] = mapped_column(
        String(length=255)
    )
    polling_station_building_and_address_local: Mapped[str | None] = mapped_column(
        String(length=1024)
    )
    polling_station_building_and_address_english: Mapped[str | None] = mapped_column(
        String(length=1024)
    )
    raw_header_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    document: Mapped[Document] = relationship(back_populates="header")


class Voter(Base, TimestampMixin):
    __tablename__ = "voters"
    __table_args__ = (
        UniqueConstraint("document_id", "serial_number", name="uq_voter_doc_serial"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    document_id: Mapped[int] = mapped_column(ForeignKey("documents.id"), nullable=False)
    serial_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    house_number: Mapped[str | None] = mapped_column(String(length=255))
    voter_name_local: Mapped[str | None] = mapped_column(String(length=255))
    voter_name_english: Mapped[str | None] = mapped_column(String(length=255))
    relation_type: Mapped[str | None] = mapped_column(String(length=50))
    relation_name_local: Mapped[str | None] = mapped_column(String(length=255))
    relation_name_english: Mapped[str | None] = mapped_column(String(length=255))
    gender: Mapped[str | None] = mapped_column(String(length=20))
    age: Mapped[int | None] = mapped_column(Integer, nullable=True)
    photo_id: Mapped[str | None] = mapped_column(String(length=255))
    raw_row_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    document: Mapped[Document] = relationship(back_populates="voters")


class DocumentSection(Base, TimestampMixin):
    __tablename__ = "document_sections"
    __table_args__ = (
        UniqueConstraint("document_id", "section_id", "start_serial_number", name="uq_document_section_occurrence"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    document_id: Mapped[int] = mapped_column(ForeignKey("documents.id"), nullable=False)
    section_id: Mapped[int] = mapped_column(Integer, nullable=False)
    section_name_local: Mapped[str | None] = mapped_column(String(length=255), nullable=True)
    section_name_english: Mapped[str | None] = mapped_column(String(length=255), nullable=True)
    start_serial_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Note: Composite index on (document_id, start_serial_number) is created in migration
    # Partial unique index for NULL handling is also created in migration

    document: Mapped[Document] = relationship(back_populates="sections")


class ApiKeyProvider(str, enum.Enum):
    """Enum for API key provider types."""
    GEMINI = "GEMINI"
    GPT = "GPT"


class ApiKeySettings(Base, TimestampMixin):
    __tablename__ = "api_key_settings"
    __table_args__ = (
        UniqueConstraint("provider_type", name="uq_api_key_provider"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    provider_type: Mapped[ApiKeyProvider] = mapped_column(
        Enum(ApiKeyProvider), nullable=False, unique=True
    )
    encrypted_api_key: Mapped[str] = mapped_column(String(length=2000), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)


