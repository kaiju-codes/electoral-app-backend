"""Extraction service for handling document extraction and processing."""

import asyncio
import logging
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

from sqlalchemy.orm import Session

from app.config import get_settings
from app.models.core import (
    Document,
    DocumentHeader,
    DocumentSection,
    ExtractionRun,
    ExtractionRunStatus,
    ExtractionSegment,
    SegmentStatus,
    SegmentType,
    Voter,
)
from app.schemas.extraction import (
    BulkRetryResponse,
    ExtractionRunListResponse,
    ExtractionRunRead,
    ExtractionSegmentRead,
    MetricsSummary,
    SegmentRetryResponse,
    SegmentRetryStatusResponse,
)
from app.services.gemini_client import extract_segment, upload_file
from app.db import SessionLocal

settings = get_settings()
logger = logging.getLogger(__name__)


class ExtractionService:
    """Service for extraction operations."""
    
    def __init__(self, db: Session):
        self.db = db
    
    @staticmethod
    def _to_int(value: str | int | None) -> int | None:
        """
        Convert a value to integer safely.
        
        Args:
            value: String, integer, or None value to convert
            
        Returns:
            Integer value if conversion successful, None otherwise
        """
        if value is None:
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            # Remove whitespace and try to convert
            cleaned = value.strip()
            if not cleaned or cleaned == "":
                return None
            try:
                # Remove any non-numeric characters except minus sign
                cleaned = cleaned.replace(",", "").replace(" ", "")
                return int(cleaned)
            except (ValueError, TypeError):
                return None
        return None
    
    def _deduplicate_and_validate_voters(self, all_rows: list[dict], document_id: int) -> list[dict]:
        """
        Intelligently deduplicate and validate voter rows based on sequential serial numbers.
        
        Strategy:
        1. Extract and validate serial numbers
        2. Check sequential nature using first 2, last 2, and middle samples
        3. Detect duplicates and handle them intelligently
        4. Optionally renumber if sequence is broken
        
        Args:
            all_rows: List of voter row dictionaries from segments
            document_id: Document ID for logging
            
        Returns:
            Deduplicated and validated list of voter rows
        """
        if not all_rows:
            return []
        
        # Step 1: Extract serial numbers and create working copy
        rows_with_serial: list[tuple[int | None, dict, int]] = []  # (serial_num, row, original_index)
        for idx, row in enumerate(all_rows):
            serial_num = self._to_int(row.get("serial_number"))
            rows_with_serial.append((serial_num, row, idx))
        
        # Filter out rows with None serial numbers (will be handled separately)
        rows_with_valid_serial = [(s, r, i) for s, r, i in rows_with_serial if s is not None]
        rows_without_serial = [(s, r, i) for s, r, i in rows_with_serial if s is None]
        
        if not rows_with_valid_serial:
            logger.warning(f"No valid serial numbers found in rows for document_id={document_id}")
            return [r for _, r, _ in rows_without_serial]
        
        # Step 2: Validate sequential nature using samples
        serial_numbers = [s for s, _, _ in rows_with_valid_serial]
        total_count = len(serial_numbers)
        
        # Check first 2 rows
        first_2_serials = sorted(set(serial_numbers))[:2]
        first_2_valid = len(first_2_serials) >= 2 and (first_2_serials[1] - first_2_serials[0]) == 1
        
        # Check last 2 rows
        last_2_serials = sorted(set(serial_numbers))[-2:]
        last_2_valid = len(last_2_serials) >= 2 and (last_2_serials[1] - last_2_serials[0]) == 1
        
        # Check middle sample (every 10% of data)
        middle_samples = []
        if total_count > 4:
            sample_indices = [total_count // 4, total_count // 2, 3 * total_count // 4]
            sorted_serials = sorted(set(serial_numbers))
            for idx in sample_indices:
                if idx < len(sorted_serials):
                    middle_samples.append(sorted_serials[idx])
        
        middle_valid = True
        if len(middle_samples) >= 2:
            for i in range(1, len(middle_samples)):
                # Check if increment is consistent (usually 1, but allow small variations)
                diff = middle_samples[i] - middle_samples[i-1]
                if diff < 1 or diff > 10:  # Allow gaps up to 10 (reasonable for missing voters)
                    middle_valid = False
                    break
        
        # Detect expected increment (usually 1)
        expected_increment = 1
        if first_2_valid:
            expected_increment = first_2_serials[1] - first_2_serials[0]
        elif len(serial_numbers) >= 2:
            # Try to detect from sorted list
            sorted_unique = sorted(set(serial_numbers))
            increments = [sorted_unique[i+1] - sorted_unique[i] for i in range(min(10, len(sorted_unique)-1))]
            if increments:
                # Use most common increment
                most_common = Counter(increments).most_common(1)[0]
                if most_common[1] >= len(increments) * 0.7:  # At least 70% consistency
                    expected_increment = most_common[0]
        
        logger.info(
            f"Serial number validation for document_id={document_id}: "
            f"total_rows={total_count}, first_2_valid={first_2_valid}, "
            f"last_2_valid={last_2_valid}, middle_valid={middle_valid}, "
            f"expected_increment={expected_increment}"
        )
        
        # Step 3: Detect duplicates
        serial_to_rows: dict[int, list[tuple[dict, int]]] = {}  # serial_num -> [(row, original_index)]
        for serial_num, row, orig_idx in rows_with_valid_serial:
            if serial_num not in serial_to_rows:
                serial_to_rows[serial_num] = []
            serial_to_rows[serial_num].append((row, orig_idx))
        
        duplicates = {s: rows for s, rows in serial_to_rows.items() if len(rows) > 1}
        
        if duplicates:
            logger.warning(
                f"Found {len(duplicates)} duplicate serial_numbers in document_id={document_id}: "
                f"duplicates={list(duplicates.keys())[:10]}"  # Log first 10
            )
        
        # Step 4: Deduplicate - keep first occurrence of each serial number
        # For duplicates, prefer the row with more complete data
        deduplicated_rows: dict[int, dict] = {}  # serial_num -> best_row
        
        for serial_num, row_list in serial_to_rows.items():
            if len(row_list) == 1:
                # No duplicate, keep as is
                deduplicated_rows[serial_num] = row_list[0][0]
            else:
                # Duplicate found - choose the best row
                # Prefer row with more non-empty fields
                best_row = None
                best_score = -1
                
                for row, _ in row_list:
                    score = sum([
                        1 if row.get("voter_name", {}).get("english") or row.get("voter_name", {}).get("local") else 0,
                        1 if row.get("house_number") else 0,
                        1 if row.get("gender") else 0,
                        1 if row.get("age") else 0,
                        1 if row.get("photo_id") else 0,
                    ])
                    if score > best_score:
                        best_score = score
                        best_row = row
                
                deduplicated_rows[serial_num] = best_row
                logger.debug(
                    f"Duplicate serial_number={serial_num} in document_id={document_id}: "
                    f"kept best row with score={best_score} out of {len(row_list)} duplicates"
                )
        
        # Step 5: Sort by serial number and detect gaps
        sorted_serials = sorted(deduplicated_rows.keys())
        if not sorted_serials:
            return []
        
        min_serial = sorted_serials[0]
        max_serial = sorted_serials[-1]
        expected_count = (max_serial - min_serial) // expected_increment + 1
        actual_count = len(sorted_serials)
        
        # Detect gaps
        gaps = []
        for i in range(len(sorted_serials) - 1):
            current = sorted_serials[i]
            next_serial = sorted_serials[i + 1]
            gap = next_serial - current
            if gap > expected_increment:
                gaps.append((current, next_serial, gap - expected_increment))
        
        if gaps:
            logger.warning(
                f"Found {len(gaps)} gaps in serial numbers for document_id={document_id}: "
                f"gaps={gaps[:5]}"  # Log first 5 gaps
            )
        
        if expected_count != actual_count:
            logger.info(
                f"Serial number range for document_id={document_id}: "
                f"min={min_serial}, max={max_serial}, expected_count={expected_count}, "
                f"actual_count={actual_count}, missing={expected_count - actual_count}"
            )
        
        # Step 6: Return deduplicated rows in serial number order
        result = []
        for serial_num in sorted_serials:
            result.append(deduplicated_rows[serial_num])
        
        # Add rows without serial numbers at the end (if any)
        for _, row, _ in rows_without_serial:
            result.append(row)
        
        logger.info(
            f"Deduplication complete for document_id={document_id}: "
            f"original_rows={len(all_rows)}, deduplicated_rows={len(result)}, "
            f"duplicates_removed={len(all_rows) - len(result)}"
        )
        
        return result
    
    def _upsert_document_sections(self, document_id: int, section_names: list[dict] | None) -> None:
        """Store section names from header JSON for later use when creating sections with start_serial_number.
        
        Note: Sections are NOT created here. They are only created when we have start_serial_number
        from list chunks via _update_section_start_serial_numbers.
        This method is kept for backward compatibility but no longer creates sections.
        """
        if not section_names or not isinstance(section_names, list):
            return
        
        logger.info(
            f"Section names from header stored (not creating sections yet): document_id={document_id}, "
            f"section_names_count={len(section_names)}"
        )
        # Sections will be created later when we have start_serial_number from list chunks
    
    def _get_document_sections(self, document_id: int) -> list[DocumentSection]:
        """Get all sections for a document, ordered by start_serial_number (ascending), then by section_id.
        
        This ordering is important for range-based section matching in voter service.
        All sections must have a start_serial_number (no NULL values).
        """
        db = SessionLocal()
        try:
            sections = db.query(DocumentSection).filter(
                DocumentSection.document_id == document_id,
                DocumentSection.start_serial_number.isnot(None)
            ).order_by(
                DocumentSection.start_serial_number.asc(),
                DocumentSection.section_id
            ).all()
            return sections
        finally:
            db.close()
    
    def _update_section_start_serial_numbers(
        self, document_id: int, section_start_positions: list[dict], section_names: list[dict] | None = None
    ) -> None:
        """Create section occurrences based on section_start_positions from list chunks.
        
        For each section_start_position, creates a new section occurrence if it doesn't exist
        with that exact (document_id, section_id, start_serial_number) combination.
        Section names are obtained from section_names (from header JSON) or from existing sections.
        All sections must have a start_serial_number (no NULL values allowed).
        """
        if not section_start_positions or not isinstance(section_start_positions, list):
            return
        
        db = SessionLocal()
        try:
            inserted_count = 0
            skipped_count = 0
            
            # Build a lookup map for section names from header
            section_names_map: dict[int, dict] = {}
            if section_names and isinstance(section_names, list):
                for section in section_names:
                    if isinstance(section, dict):
                        section_id = section.get("number")
                        if isinstance(section_id, int):
                            section_names_map[section_id] = {
                                "local": section.get("local"),
                                "english": section.get("english")
                            }
            
            for pos in section_start_positions:
                section_id = pos.get("section_number")
                start_serial = pos.get("start_serial_number")
                
                if not isinstance(section_id, int) or not isinstance(start_serial, int):
                    skipped_count += 1
                    continue
                
                # Check if section occurrence with this exact combination exists
                existing = db.query(DocumentSection).filter(
                    DocumentSection.document_id == document_id,
                    DocumentSection.section_id == section_id,
                    DocumentSection.start_serial_number == start_serial
                ).first()
                
                if existing:
                    # Section occurrence already exists, skip
                    skipped_count += 1
                    continue
                
                # Get section name from header map or existing section
                section_name_local = None
                section_name_english = None
                
                if section_id in section_names_map:
                    section_name_local = section_names_map[section_id].get("local")
                    section_name_english = section_names_map[section_id].get("english")
                else:
                    # Fallback: get from any existing occurrence of this section_id
                    name_section = db.query(DocumentSection).filter(
                        DocumentSection.document_id == document_id,
                        DocumentSection.section_id == section_id
                    ).first()
                    if name_section:
                        section_name_local = name_section.section_name_local
                        section_name_english = name_section.section_name_english
                
                # Insert new section occurrence with start_serial_number
                new_section = DocumentSection(
                    document_id=document_id,
                    section_id=section_id,
                    section_name_local=section_name_local,
                    section_name_english=section_name_english,
                    start_serial_number=start_serial
                )
                db.add(new_section)
                inserted_count += 1
                logger.debug(
                    f"Created section: document_id={document_id}, section_id={section_id}, "
                    f"start_serial={start_serial}"
                )
            
            db.commit()
            logger.info(
                f"Section start serial numbers processed: document_id={document_id}, "
                f"sections_inserted={inserted_count}, skipped={skipped_count}"
            )
        finally:
            db.close()
    
    def _check_duplicate_document(self, header_json: dict, document_id: int) -> Tuple[bool, str | None]:
        """Check if document is duplicate based on header data. Returns (is_duplicate, error_message)."""
        state = header_json.get("state")
        part_number = header_json.get("part_number")
        ac_number = header_json.get("assembly_constituency_number") or {}
        ac_number_english = ac_number.get("english")
        
        if not (state and part_number is not None and ac_number_english is not None):
            return False, None
        
        db = SessionLocal()
        try:
            existing = db.query(DocumentHeader).filter(
                DocumentHeader.state == state,
                DocumentHeader.assembly_constituency_number_english == ac_number_english,
                DocumentHeader.part_number == part_number,
                DocumentHeader.document_id != document_id
            ).first()
            
            if existing:
                error_msg = (
                    f"Document already processed: state={state}, "
                    f"constituency={ac_number_english}, part_number={part_number}"
                )
                return True, error_msg
            return False, None
        finally:
            db.close()
    
    def _skip_duplicate_runs(self, header_json: dict, current_document_id: int, error_msg: str) -> None:
        """Skip all extraction runs for documents with the same state + constituency + part_number combination."""
        state = header_json.get("state")
        part_number = header_json.get("part_number")
        ac_number = header_json.get("assembly_constituency_number") or {}
        ac_number_english = ac_number.get("english")
        
        if not (state and part_number is not None and ac_number_english is not None):
            return
        
        db = SessionLocal()
        try:
            # Find all documents with the same combination (including current document)
            duplicate_documents = db.query(DocumentHeader).filter(
                DocumentHeader.state == state,
                DocumentHeader.assembly_constituency_number_english == ac_number_english,
                DocumentHeader.part_number == part_number
            ).all()
            
            # Get all document IDs with this combination
            all_doc_ids = [doc.document_id for doc in duplicate_documents]
            
            # Also ensure current document is included (in case header not yet saved)
            if current_document_id not in all_doc_ids:
                all_doc_ids.append(current_document_id)
            
            # Find all extraction runs for these documents that are not already completed/skipped
            runs_to_skip = db.query(ExtractionRun).filter(
                ExtractionRun.document_id.in_(all_doc_ids),
                ExtractionRun.status.in_([
                    ExtractionRunStatus.PENDING,
                    ExtractionRunStatus.RUNNING
                ])
            ).all()
            
            # Mark all runs as skipped and skip their remaining segments
            skipped_count = 0
            skipped_segments_count = 0
            for run in runs_to_skip:
                if run.status not in (ExtractionRunStatus.COMPLETED, ExtractionRunStatus.SKIPPED):
                    run.status = ExtractionRunStatus.SKIPPED
                    run.error_message = error_msg
                    run.finished_at = run.finished_at or datetime.utcnow()
                    skipped_count += 1
                    
                    # Skip all remaining segments for this run
                    remaining_segments = db.query(ExtractionSegment).filter(
                        ExtractionSegment.extraction_run_id == run.id,
                        ExtractionSegment.status.in_([
                            SegmentStatus.PENDING,
                            SegmentStatus.RUNNING
                        ])
                    ).all()
                    
                    for segment in remaining_segments:
                        segment.status = SegmentStatus.SKIPPED
                        segment.raw_response_json = {"skipped": True, "reason": error_msg}
                        skipped_segments_count += 1
            
            if skipped_count > 0:
                db.commit()
                logger.info(
                    f"Skipped {skipped_count} extraction runs and {skipped_segments_count} segments for duplicate documents: "
                    f"state={state}, constituency={ac_number_english}, part_number={part_number}, "
                    f"document_ids={all_doc_ids}"
                )
        finally:
            db.close()
    
    def _process_segment_response(
        self, 
        segment_id: int, 
        seg_type: SegmentType, 
        parsed: dict, 
        extraction_run_id: int
    ) -> None:
        """Centralized handler for segment response processing."""
        db = SessionLocal()
        try:
            segment = db.get(ExtractionSegment, segment_id)
            if not segment:
                return
            
            # Check if run is already skipped
            run = db.get(ExtractionRun, extraction_run_id)
            if run and run.status == ExtractionRunStatus.SKIPPED:
                segment.status = SegmentStatus.SKIPPED
                segment.raw_response_json = {"skipped": True, "reason": run.error_message or "Run was skipped"}
                db.commit()
                return
            
            # Persist segment results
            segment.status = SegmentStatus.DONE
            segment.raw_response_json = parsed
            if seg_type == SegmentType.HEADER:
                segment.parsed_header_json = parsed.get("header")  # type: ignore[assignment]
            if "list" in parsed:
                segment.parsed_list_json = parsed.get("list")  # type: ignore[assignment]
            db.commit()
            
            # Process header and sections for HEADER segments
            if seg_type == SegmentType.HEADER and segment.parsed_header_json:
                header_json = segment.parsed_header_json
                if run:
                    # Check for duplicates BEFORE upserting header
                    is_duplicate, error_msg = self._check_duplicate_document(header_json, run.document_id)
                    
                    if is_duplicate:
                        # Skip all runs for documents with the same combination
                        self._skip_duplicate_runs(header_json, run.document_id, error_msg)
                        # Mark this segment as skipped
                        segment.status = SegmentStatus.SKIPPED
                        segment.raw_response_json = {"skipped": True, "reason": error_msg}
                        db.commit()
                        logger.info(
                            f"Duplicate document detected in segment processing, skipping: "
                            f"extraction_run_id={extraction_run_id}, document_id={run.document_id}, {error_msg}"
                        )
                        # Don't upsert header for duplicate documents
                        return
                    
                    # Only upsert if not a duplicate
                    self._upsert_document_header(run.document_id, header_json)
                    section_names = header_json.get("section_names")
                    if isinstance(section_names, list):
                        self._upsert_document_sections(run.document_id, section_names)
            
            # Update extraction run status
            self._update_extraction_run_status(extraction_run_id)
        finally:
            db.close()
    
    def _process_section_start_positions(self, extraction_run_id: int) -> None:
        """Collect section start positions from all LIST_CHUNK segments and create sections.
        
        Also retrieves section names from the header segment to populate section names.
        """
        db = SessionLocal()
        try:
            run = db.get(ExtractionRun, extraction_run_id)
            if not run:
                return
            
            # Get section names from header segment
            header_seg = db.query(ExtractionSegment).filter(
                ExtractionSegment.extraction_run_id == extraction_run_id,
                ExtractionSegment.segment_type == SegmentType.HEADER,
                ExtractionSegment.status == SegmentStatus.DONE
            ).first()
            
            section_names = None
            if header_seg and header_seg.parsed_header_json:
                header_json = header_seg.parsed_header_json
                section_names = header_json.get("section_names")
            
            # Collect section_start_positions from all list chunks
            all_section_start_positions: list[dict] = []
            list_segs_done = db.query(ExtractionSegment).filter(
                ExtractionSegment.extraction_run_id == extraction_run_id,
                ExtractionSegment.segment_type == SegmentType.LIST_CHUNK,
                ExtractionSegment.status == SegmentStatus.DONE
            ).all()
            
            for seg in list_segs_done:
                seg_response = seg.raw_response_json or {}
                section_starts = seg_response.get("section_start_positions")
                if isinstance(section_starts, list):
                    all_section_start_positions.extend(section_starts)
            
            # Create sections with start_serial_numbers
            if all_section_start_positions:
                self._update_section_start_serial_numbers(
                    run.document_id, all_section_start_positions, section_names
                )
        finally:
            db.close()
    
    def _build_segments(self, page_count: Optional[int], max_pages_per_call: int) -> List[Tuple[SegmentType, int, int]]:
        """Build header + list segments according to guideline-step-1."""
        segments: List[Tuple[SegmentType, int, int]] = []
        
        # Header segment: page 1 only (always included if document exists)
        segments.append((SegmentType.HEADER, 1, 1))
        
        # List segments: pages 2..N (only if we have actual pages)
        if page_count and page_count > 1:
            start_page = 2
            end_page = page_count
            
            current = start_page
            while current <= end_page:
                # Chunk size is max_pages_per_call - 1 (because we typically exclude page 1 from chunks)
                chunk_end = min(current + max_pages_per_call - 1, end_page)
                segments.append((SegmentType.LIST_CHUNK, current, chunk_end))
                current = chunk_end + 1
        else:
            # No pages beyond header, or invalid page count
            logger.warning(
                f"Invalid or missing page_count={page_count}, only creating header segment"
            )
        
        logger.info(
            f"Built segments: page_count={page_count}, total_segments={len(segments)}, "
            f"max_pages_per_call={max_pages_per_call}"
        )
        return segments
    
    async def _run_extraction_segments_async(self, extraction_run_id: int, file_uri: str, mime_type: str) -> None:
        """Asynchronously process all segments for an extraction run using Gemini.
        
        Processes HEADER segment first, then checks for duplicates. Only proceeds with
        LIST_CHUNK segments if HEADER succeeds and no duplicate is found.
        """
        segment_start_time = time.time()
        semaphore = asyncio.Semaphore(3)
        success_count = 0
        failure_count = 0
        
        async def process_segment(seg_id: int, seg_type: SegmentType, page_start: int, page_end: int, sections: list[DocumentSection] | None = None) -> None:
            nonlocal success_count, failure_count
            async with semaphore:
                # Check if run is already skipped before processing
                db_check = SessionLocal()
                try:
                    run_check = db_check.get(ExtractionRun, extraction_run_id)
                    if run_check and run_check.status == ExtractionRunStatus.SKIPPED:
                        # Mark segment as skipped
                        segment_check = db_check.get(ExtractionSegment, seg_id)
                        if segment_check and segment_check.status in (SegmentStatus.PENDING, SegmentStatus.RUNNING):
                            segment_check.status = SegmentStatus.SKIPPED
                            segment_check.raw_response_json = {"skipped": True, "reason": run_check.error_message or "Run was skipped"}
                            db_check.commit()
                            logger.info(
                                f"Segment skipped due to run being skipped: extraction_run_id={extraction_run_id}, "
                                f"segment_id={seg_id}"
                            )
                        return
                finally:
                    db_check.close()
                
                # Mark segment as RUNNING before processing so UI shows progress
                db_mark = SessionLocal()
                try:
                    seg = db_mark.get(ExtractionSegment, seg_id)
                    if seg and seg.status == SegmentStatus.PENDING:
                        seg.status = SegmentStatus.RUNNING
                        db_mark.commit()
                        logger.debug(
                            f"Segment marked as RUNNING: extraction_run_id={extraction_run_id}, "
                            f"segment_id={seg_id}"
                        )
                finally:
                    db_mark.close()
                
                segment_process_start = time.time()
                try:
                    parsed = await asyncio.to_thread(
                        extract_segment,
                        file_uri,
                        mime_type,
                        seg_type.name,
                        page_start,
                        page_end,
                        sections,
                    )
                    # Use centralized response processing
                    self._process_segment_response(seg_id, seg_type, parsed, extraction_run_id)
                    success_count += 1
                    duration = time.time() - segment_process_start
                    logger.info(
                        f"Segment completed: extraction_run_id={extraction_run_id}, "
                        f"segment_id={seg_id}, segment_type={seg_type.name}, "
                        f"pages={page_start}-{page_end}, duration={duration:.2f}s"
                    )
                except Exception as exc:  # pragma: no cover - defensive logging
                    failure_count += 1
                    duration = time.time() - segment_process_start
                    logger.error(
                        f"Segment failed: extraction_run_id={extraction_run_id}, "
                        f"segment_id={seg_id}, segment_type={seg_type.name}, "
                        f"pages={page_start}-{page_end}, duration={duration:.2f}s, "
                        f"error={type(exc).__name__}: {str(exc)}",
                        exc_info=True,
                    )
                    db = SessionLocal()
                    try:
                        segment = db.get(ExtractionSegment, seg_id)
                        if segment:
                            segment.status = SegmentStatus.FAILED
                            segment.raw_response_json = {"error": str(exc)}
                            db.commit()
                    finally:
                        db.close()
                    
                    # Update run status when a segment fails
                    self._update_extraction_run_status(extraction_run_id)
        
        # Fetch all segments for the run.
        db = SessionLocal()
        try:
            run: ExtractionRun | None = db.get(ExtractionRun, extraction_run_id)
            if not run:
                return
            run.status = ExtractionRunStatus.RUNNING
            run.started_at = run.started_at or datetime.utcnow()
            db.commit()
            db.refresh(run)
            
            segments = list(run.segments)
            total_segments = len(segments)
            logger.info(
                f"Starting segment extraction: extraction_run_id={extraction_run_id}, "
                f"document_id={run.document_id}, total_segments={total_segments}"
            )
        finally:
            db.close()
        
        # Step 1: Process HEADER segment first (chronologically)
        header_segment = next((seg for seg in segments if seg.segment_type == SegmentType.HEADER), None)
        
        if header_segment:
            logger.info(
                f"Processing HEADER segment first: extraction_run_id={extraction_run_id}, "
                f"segment_id={header_segment.id}"
            )
            await process_segment(header_segment.id, header_segment.segment_type, header_segment.page_start, header_segment.page_end)
        
        # Step 2: Check if run was skipped due to duplicate (duplicate check happens in _process_segment_response)
        if header_segment:
            db = SessionLocal()
            try:
                run_check = db.get(ExtractionRun, extraction_run_id)
                if run_check and run_check.status == ExtractionRunStatus.SKIPPED:
                    logger.info(
                        f"Extraction run was skipped due to duplicate: extraction_run_id={extraction_run_id}, "
                        f"document_id={run_check.document_id}"
                    )
                    return  # Stop processing, don't process LIST chunks
            finally:
                db.close()
        
        # Step 4: Process LIST_CHUNK segments (can be parallel)
        list_segments = [seg for seg in segments if seg.segment_type == SegmentType.LIST_CHUNK]
        if list_segments:
            logger.info(
                f"Processing {len(list_segments)} LIST_CHUNK segments: extraction_run_id={extraction_run_id}"
            )
            
            # Get sections from database for LIST_CHUNK processing
            db = SessionLocal()
            try:
                run_for_sections = db.get(ExtractionRun, extraction_run_id)
                sections_list: list[DocumentSection] = []
                if run_for_sections:
                    sections_list = self._get_document_sections(run_for_sections.document_id)
            finally:
                db.close()
            
            tasks = [
                process_segment(seg.id, seg.segment_type, seg.page_start, seg.page_end, sections_list if sections_list else None)
                for seg in list_segments
            ]
            if tasks:
                await asyncio.gather(*tasks)
            
            # After all list chunks complete, update section start_serial_numbers
            self._process_section_start_positions(extraction_run_id)
        
        # Update extraction run status after all segments complete
        self._update_extraction_run_status(extraction_run_id)
        
        total_duration = time.time() - segment_start_time
        logger.info(
            f"Segment extraction completed: extraction_run_id={extraction_run_id}, "
            f"total_segments={total_segments}, success={success_count}, "
            f"failures={failure_count}, total_duration={total_duration:.2f}s, "
            f"avg_duration_per_segment={total_duration/max(total_segments, 1):.2f}s"
        )
    
    def _upsert_document_header(self, document_id: int, header_json: dict) -> None:
        """Upsert document header."""
        db = SessionLocal()
        try:
            header_model = (
                db.get(DocumentHeader, document_id) or DocumentHeader(document_id=document_id)
            )
            header_model.state = header_json.get("state")
            part_num = header_json.get("part_number")
            header_model.part_number = self._to_int(part_num)
            header_model.language = header_json.get("language")
            
            ac_number = header_json.get("assembly_constituency_number") or {}
            header_model.assembly_constituency_number_local = ac_number.get("local")
            ac_num_eng = ac_number.get("english")
            header_model.assembly_constituency_number_english = self._to_int(ac_num_eng)
            
            ac_name = header_json.get("assembly_constituency_name") or {}
            header_model.assembly_constituency_name_local = ac_name.get("local")
            header_model.assembly_constituency_name_english = ac_name.get("english")
            
            ps_number = header_json.get("polling_station_number") or {}
            header_model.polling_station_number_local = ps_number.get("local")
            ps_num_eng = ps_number.get("english")
            header_model.polling_station_number_english = self._to_int(ps_num_eng)
            
            ps_name = header_json.get("polling_station_name") or {}
            header_model.polling_station_name_local = ps_name.get("local")
            header_model.polling_station_name_english = ps_name.get("english")
            
            ps_address = header_json.get("polling_station_building_and_address") or {}
            header_model.polling_station_building_and_address_local = ps_address.get("local")
            header_model.polling_station_building_and_address_english = ps_address.get("english")
            
            header_model.raw_header_json = header_json
            db.merge(header_model)
            db.commit()
            logger.info(f"Document header upserted: document_id={document_id}")
        finally:
            db.close()
    
    def _update_extraction_run_status(self, extraction_run_id: int) -> None:
        """Update extraction run status based on current segment states."""
        db = SessionLocal()
        try:
            run: ExtractionRun | None = db.get(ExtractionRun, extraction_run_id)
            if not run:
                return
            
            # Don't update if already skipped or completed
            if run.status in (ExtractionRunStatus.SKIPPED, ExtractionRunStatus.COMPLETED):
                return
            
            segments: list[ExtractionSegment] = list(run.segments)
            
            # Check if any segments are still in progress (PENDING or RUNNING)
            unfinished = any(s.status in (SegmentStatus.PENDING, SegmentStatus.RUNNING) for s in segments)
            if unfinished:
                # Keep run as RUNNING and don't set finished_at while segments are in progress
                run.status = ExtractionRunStatus.RUNNING
                run.finished_at = None
                db.commit()
                logger.debug(
                    f"Extraction run kept as RUNNING: extraction_run_id={extraction_run_id}, "
                    f"unfinished_segments={sum(1 for s in segments if s.status in (SegmentStatus.PENDING, SegmentStatus.RUNNING))}"
                )
                return
            
            # All segments are terminal (DONE, FAILED, or SKIPPED), determine final status
            any_failed = any(s.status == SegmentStatus.FAILED for s in segments)
            failed_segment_count = sum(1 for s in segments if s.status == SegmentStatus.FAILED)
            all_done = all(s.status == SegmentStatus.DONE for s in segments)
            any_done = any(s.status == SegmentStatus.DONE for s in segments)
            
            header_segment = next(
                (s for s in segments if s.segment_type == SegmentType.HEADER and s.status == SegmentStatus.DONE),
                None,
            )
            list_segments_done = [
                s for s in segments 
                if s.segment_type == SegmentType.LIST_CHUNK and s.status == SegmentStatus.DONE
            ]
            
            # Check if we have header and list data
            has_header = header_segment is not None
            has_list_data = len(list_segments_done) > 0
            
            # Determine final status
            if all_done and has_header and has_list_data and not any_failed:
                run.status = ExtractionRunStatus.COMPLETED
                run.finished_at = run.finished_at or datetime.utcnow()
            elif any_done and (has_header or has_list_data):
                run.status = ExtractionRunStatus.PARTIAL
                if not run.finished_at:
                    run.finished_at = datetime.utcnow()
            elif any_failed and not any_done:
                run.status = ExtractionRunStatus.FAILED
                run.error_message = f"All segments failed. Failed segments: {failed_segment_count}"
                if not run.finished_at:
                    run.finished_at = datetime.utcnow()
            
            db.commit()
            logger.debug(
                f"Extraction run status updated: extraction_run_id={extraction_run_id}, "
                f"status={run.status}, has_header={has_header}, "
                f"list_segments_done={len(list_segments_done)}, failed={failed_segment_count}"
            )
        finally:
            db.close()
    
    def _merge_segments_and_persist(self, extraction_run_id: int) -> None:
        """Merge header + list segments and persist into document_header and voters."""
        db = SessionLocal()
        try:
            run: ExtractionRun | None = db.get(ExtractionRun, extraction_run_id)
            if not run:
                logger.warning(f"ExtractionRun not found for merge: extraction_run_id={extraction_run_id}")
                return
            
            # Skip merging if run was skipped (e.g., duplicate document)
            if run.status == ExtractionRunStatus.SKIPPED:
                logger.info(f"Skipping merge for skipped extraction run: extraction_run_id={extraction_run_id}")
                return
            
            document: Document = run.document
            segments: list[ExtractionSegment] = list(run.segments)
            
            header_segment = next(
                (s for s in segments if s.segment_type == SegmentType.HEADER and s.status == SegmentStatus.DONE),
                None,
            )
            list_segments = [
                s
                for s in segments
                if s.segment_type == SegmentType.LIST_CHUNK and s.status == SegmentStatus.DONE
            ]
            logger.info(
                f"Merging segments: extraction_run_id={extraction_run_id}, "
                f"has_header={header_segment is not None}, "
                f"list_segments_count={len(list_segments)}"
            )
            
            # Header is already upserted incrementally, so we just need to handle voters here
            # Build voter rows
            all_rows: list[dict] = []
            
            for seg in list_segments:
                # Extract voter rows
                rows = seg.parsed_list_json or (seg.raw_response_json or {}).get("list") or []
                if isinstance(rows, list):
                    all_rows.extend(rows)
            
            # Get header_json for status determination
            header_json: dict | None = None
            if header_segment:
                header_json = header_segment.parsed_header_json or (
                    header_segment.raw_response_json or {}
                ).get("header")
            
            # Simple idempotency: remove previous voters for this document, then insert fresh.
            if all_rows:
                deleted_count = db.query(Voter).filter(Voter.document_id == document.id).delete()
                
                # Intelligently deduplicate and validate voters before inserting
                validated_rows = self._deduplicate_and_validate_voters(all_rows, document.id)
                
                voter_models: list[Voter] = []
                for row in validated_rows:
                    voter_name = row.get("voter_name") or {}
                    relation_name = row.get("relation_name") or {}
                    
                    # Convert numeric fields to integers
                    serial_num = row.get("serial_number")
                    age_val = row.get("age")
                    
                    voter_models.append(
                        Voter(
                            document_id=document.id,
                            serial_number=self._to_int(serial_num),
                            house_number=row.get("house_number") or "",
                            voter_name_local=voter_name.get("local") or "",
                            voter_name_english=voter_name.get("english") or "",
                            relation_type=row.get("relation_type") or "",
                            relation_name_local=relation_name.get("local") or "",
                            relation_name_english=relation_name.get("english") or "",
                            gender=row.get("gender") or "",
                            age=self._to_int(age_val),
                            photo_id=row.get("photo_id") or "",
                            raw_row_json=row,
                        )
                    )
                
                if voter_models:
                    db.bulk_save_objects(voter_models)
                    logger.info(
                        f"Voters persisted: extraction_run_id={extraction_run_id}, "
                        f"document_id={document.id}, deleted_old={deleted_count}, "
                        f"inserted_new={len(voter_models)}"
                    )
            
            # Final status update (status is already updated incrementally, but ensure it's correct)
            self._update_extraction_run_status(extraction_run_id)
            
            db.commit()
        finally:
            db.close()
    
    async def run_extraction_for_document(self, document_id: int, file_bytes: bytes, filename: str, mime_type: str) -> None:
        """Orchestrate extraction for a single document."""
        extraction_start_time = time.time()
        logger.info(
            f"Starting extraction for document: document_id={document_id}, "
            f"filename={filename}, size_kb={len(file_bytes)/1024:.2f}KB"
        )
        
        db = SessionLocal()
        # Variables to capture before session closes
        file_uri: str | None = None
        final_mime_type: str | None = None
        extraction_run_id: int | None = None
        
        try:
            document: Document | None = db.get(Document, document_id)
            if not document:
                logger.warning(f"Document not found: document_id={document_id}")
                db.close()
                return
            
            # Document processing status is now tracked via ExtractionRun status
            db.commit()
            db.refresh(document)
            
            # Step 2: Upload to Gemini
            upload_start = time.time()
            file_uri, detected_mime_type, metadata = upload_file(file_bytes, filename)
            upload_duration = time.time() - upload_start
            document.upload_file_uri = file_uri
            final_mime_type = detected_mime_type or mime_type or "application/pdf"
            document.mime_type = final_mime_type
            db.commit()
            db.refresh(document)
            logger.info(
                f"File uploaded to Gemini: document_id={document_id}, "
                f"page_count={document.page_count}, upload_duration={upload_duration:.2f}s"
            )
            
            # Step 3: Create ExtractionRun
            extraction_run = ExtractionRun(
                document_id=document.id,
                status=ExtractionRunStatus.PENDING,
                started_at=datetime.utcnow(),
            )
            db.add(extraction_run)
            db.commit()
            db.refresh(extraction_run)
            extraction_run_id = extraction_run.id
            logger.info(
                f"ExtractionRun created: run_id={extraction_run_id}, document_id={document_id}, "
                f"page_count={document.page_count}"
            )
            
            # Step 4: Create segments based on actual page count
            segments_spec = self._build_segments(
                page_count=document.page_count,
                max_pages_per_call=settings.gemini_max_pages_per_call,
            )
            segment_count = len(segments_spec)
            for seg_type, page_start, page_end in segments_spec:
                segment = ExtractionSegment(
                    extraction_run_id=extraction_run_id,
                    segment_type=seg_type,
                    page_start=page_start,
                    page_end=page_end,
                    status=SegmentStatus.PENDING,
                )
                db.add(segment)
            db.commit()
            db.refresh(extraction_run)
            logger.info(
                f"Segments created: extraction_run_id={extraction_run_id}, "
                f"total_segments={segment_count}"
            )
        except Exception as exc:
            # Mark document as failed if anything goes wrong early.
            logger.error(
                f"Extraction failed early: document_id={document_id}, "
                f"error={type(exc).__name__}: {str(exc)}",
                exc_info=True,
            )
            try:
                document = db.get(Document, document_id)
                if document:
                    # Find the latest extraction run and mark it as failed
                    latest_run = db.query(ExtractionRun).filter(
                        ExtractionRun.document_id == document_id
                    ).order_by(ExtractionRun.created_at.desc()).first()
                    if latest_run:
                        latest_run.status = ExtractionRunStatus.FAILED
                        latest_run.error_message = str(exc)
                        db.commit()
            except Exception:
                pass  # Ignore errors during cleanup
            finally:
                db.close()
            return
        finally:
            db.close()
        
        # Verify we have the required values before proceeding
        if not file_uri or not final_mime_type or not extraction_run_id:
            logger.error(
                f"Missing required values after extraction setup: "
                f"file_uri={file_uri}, mime_type={final_mime_type}, "
                f"extraction_run_id={extraction_run_id}"
            )
            return
        
        # Step 4: Run async extraction over all segments
        extraction_segments_start = time.time()
        await self._run_extraction_segments_async(
            extraction_run_id=extraction_run_id,
            file_uri=file_uri,
            mime_type=final_mime_type,
        )
        extraction_segments_duration = time.time() - extraction_segments_start
        logger.info(
            f"Segment extraction finished: extraction_run_id={extraction_run_id}, "
            f"duration={extraction_segments_duration:.2f}s"
        )
        
        # Step 5: Merge and persist
        merge_start = time.time()
        self._merge_segments_and_persist(extraction_run_id=extraction_run_id)
        merge_duration = time.time() - merge_start
        total_extraction_duration = time.time() - extraction_start_time
        
        logger.info(
            f"Merge and persist completed: extraction_run_id={extraction_run_id}, "
            f"merge_duration={merge_duration:.2f}s"
        )
        logger.info(
            f"Extraction completed: document_id={document_id}, "
            f"total_duration={total_extraction_duration:.2f}s"
        )
    
    def list_extraction_runs(
        self,
        document_id: Optional[int] = None,
        status: Optional[ExtractionRunStatus] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> ExtractionRunListResponse:
        """List extraction runs with pagination and optional filters."""
        query = self.db.query(ExtractionRun)
        
        if document_id:
            query = query.filter(ExtractionRun.document_id == document_id)
        if status:
            query = query.filter(ExtractionRun.status == status)
        
        # Get total count before pagination
        total = query.count()
        
        # Apply pagination
        offset = (page - 1) * page_size
        runs = query.order_by(ExtractionRun.created_at.desc()).offset(offset).limit(page_size).all()
        
        items = []
        for run in runs:
            segments = [ExtractionSegmentRead.model_validate(seg) for seg in run.segments]
            run_read = ExtractionRunRead.model_validate(run)
            run_read.segments = segments
            items.append(run_read)
        
        logger.info(
            f"Extraction runs list: page={page}, page_size={page_size}, "
            f"document_id={document_id}, status={status}, total={total}, returned={len(items)}"
        )
        
        return ExtractionRunListResponse(
            items=items,
            total=total,
            page=page,
            page_size=page_size,
        )
    
    def get_extraction_run(self, run_id: int) -> Optional[ExtractionRunRead]:
        """Get a single extraction run."""
        run: ExtractionRun | None = self.db.get(ExtractionRun, run_id)
        if not run:
            return None
        
        segments = [ExtractionSegmentRead.model_validate(seg) for seg in run.segments]
        base = ExtractionRunRead.model_validate(run)
        base.segments = segments
        return base
    
    def get_metrics(self) -> MetricsSummary:
        """Get metrics summary."""
        # Total documents
        total_documents = self.db.query(Document).count()
        
        # Total extraction runs and their statuses
        total_runs = self.db.query(ExtractionRun).count()
        completed_runs = (
            self.db.query(ExtractionRun).filter(ExtractionRun.status == ExtractionRunStatus.COMPLETED).count()
        )
        partial_runs = (
            self.db.query(ExtractionRun).filter(ExtractionRun.status == ExtractionRunStatus.PARTIAL).count()
        )
        failed_runs = (
            self.db.query(ExtractionRun).filter(ExtractionRun.status == ExtractionRunStatus.FAILED).count()
        )
        
        # Total voters and average per document
        total_voters = self.db.query(Voter).count()
        avg_voters_per_document = (
            total_voters / total_documents if total_documents > 0 else 0.0
        )
        
        # Total segments and failed segments
        total_segments = self.db.query(ExtractionSegment).count()
        failed_segments = (
            self.db.query(ExtractionSegment)
            .filter(ExtractionSegment.status == SegmentStatus.FAILED)
            .count()
        )
        gemini_error_rate = (
            (failed_segments / total_segments * 100) if total_segments > 0 else 0.0
        )
        
        # Average extraction time (from completed runs that have both started_at and finished_at)
        completed_runs_with_times = (
            self.db.query(ExtractionRun)
            .filter(
                ExtractionRun.status == ExtractionRunStatus.COMPLETED,
                ExtractionRun.started_at.isnot(None),
                ExtractionRun.finished_at.isnot(None),
            )
            .all()
        )
        
        avg_extraction_time: float | None = None
        total_extraction_time: float | None = None
        
        if completed_runs_with_times:
            extraction_times = []
            for run in completed_runs_with_times:
                if run.started_at and run.finished_at:
                    delta = run.finished_at - run.started_at
                    extraction_times.append(delta.total_seconds())
            
            if extraction_times:
                avg_extraction_time = sum(extraction_times) / len(extraction_times)
                total_extraction_time = sum(extraction_times)
        
        logger.info(
            f"Metrics summary: documents={total_documents}, runs={total_runs}, "
            f"completed={completed_runs}, partial={partial_runs}, failed={failed_runs}, "
            f"voters={total_voters}, segments={total_segments}, failed_segments={failed_segments}, "
            f"error_rate={gemini_error_rate:.2f}%"
        )
        
        return MetricsSummary(
            total_documents=total_documents,
            total_extraction_runs=total_runs,
            completed_runs=completed_runs,
            partial_runs=partial_runs,
            failed_runs=failed_runs,
            total_voters=total_voters,
            avg_voters_per_document=avg_voters_per_document,
            total_segments=total_segments,
            failed_segments=failed_segments,
            gemini_error_rate=gemini_error_rate,
            avg_extraction_time_seconds=avg_extraction_time,
            total_extraction_time_seconds=total_extraction_time,
        )
    
    def _can_retry_segment(self, segment: ExtractionSegment) -> Tuple[bool, str]:
        """Check if a segment can be retried based on status and time constraints."""
        if segment.status == SegmentStatus.SKIPPED:
            return False, "Segment is SKIPPED and cannot be retried"
        if segment.status != SegmentStatus.FAILED:
            return False, f"Segment is not in FAILED status (current: {segment.status})"
        
        # Check 48-hour time limit from last update
        now = datetime.now(timezone.utc)
        time_limit = now - timedelta(hours=48)
        
        # Ensure segment.updated_at is timezone-aware
        segment_updated = segment.updated_at
        if segment_updated.tzinfo is None:
            segment_updated = segment_updated.replace(tzinfo=timezone.utc)
        
        if segment_updated < time_limit:
            return False, "Retry time limit exceeded (48 hours)"
        
        return True, "OK"
    
    async def retry_segment(self, segment_id: int) -> SegmentRetryResponse:
        """Retry processing of a single failed segment."""
        # Get segment with its extraction run and document
        segment = self.db.get(ExtractionSegment, segment_id)
        if not segment:
            raise ValueError("Segment not found")
        
        # Check if segment can be retried
        can_retry, reason = self._can_retry_segment(segment)
        if not can_retry:
            raise ValueError(f"Cannot retry segment: {reason}")
        
        # Get document details for file URI
        extraction_run = segment.extraction_run
        document = extraction_run.document
        
        if not document.upload_file_uri:
            raise ValueError("Document file URI not available")
        
        # Start retry in background
        await self._retry_segment_async(
            segment_id,
            document.upload_file_uri,
            document.mime_type or "application/pdf"
        )
        
        logger.info(f"Segment retry initiated: segment_id={segment_id}, document_id={document.id}")
        
        return SegmentRetryResponse(
            message="Segment retry initiated",
            segment_id=segment_id,
            document_id=document.id,
            extraction_run_id=extraction_run.id
        )
    
    async def retry_failed_segments_for_document(self, document_id: int) -> BulkRetryResponse:
        """Retry all failed segments for a specific document."""
        # Get document
        document = self.db.get(Document, document_id)
        if not document:
            raise ValueError("Document not found")
        
        if not document.upload_file_uri:
            raise ValueError("Document file URI not available")
        
        # Get all failed segments for this document
        failed_segments = []
        retryable_segments = []
        non_retryable_segments = []
        
        for extraction_run in document.extraction_runs:
            for segment in extraction_run.segments:
                if segment.status == SegmentStatus.FAILED:
                    failed_segments.append(segment)
                    can_retry, reason = self._can_retry_segment(segment)
                    if can_retry:
                        retryable_segments.append(segment)
                    else:
                        non_retryable_segments.append((segment, reason))
        
        if not failed_segments:
            return BulkRetryResponse(
                message="No failed segments found for document",
                document_id=document_id,
                failed_segments_count=0,
                retryable_segments_count=0,
                non_retryable_segments_count=0
            )
        
        if not retryable_segments:
            return BulkRetryResponse(
                message="No retryable segments found (all exceed time limit or wrong status)",
                document_id=document_id,
                failed_segments_count=len(failed_segments),
                retryable_segments_count=0,
                non_retryable_segments_count=len(non_retryable_segments),
                non_retryable_reasons=[
                    {"segment_id": seg.id, "reason": reason} 
                    for seg, reason in non_retryable_segments
                ]
            )
        
        # Start retry for all retryable segments
        for segment in retryable_segments:
            await self._retry_segment_async(
                segment.id,
                document.upload_file_uri,
                document.mime_type or "application/pdf"
            )
        
        logger.info(
            f"Bulk segment retry initiated: document_id={document_id}, "
            f"retrying {len(retryable_segments)} segments"
        )
        
        return BulkRetryResponse(
            message=f"Retry initiated for {len(retryable_segments)} failed segments",
            document_id=document_id,
            failed_segments_count=len(failed_segments),
            retryable_segments_count=len(retryable_segments),
            non_retryable_segments_count=len(non_retryable_segments),
            retryable_segment_ids=[seg.id for seg in retryable_segments],
            non_retryable_reasons=[
                {"segment_id": seg.id, "reason": reason} 
                for seg, reason in non_retryable_segments
            ]
        )
    
    def get_segment_retry_status(self, segment_id: int) -> SegmentRetryStatusResponse:
        """Check if a segment can be retried and get retry status information."""
        segment = self.db.get(ExtractionSegment, segment_id)
        if not segment:
            raise ValueError("Segment not found")
        
        can_retry, reason = self._can_retry_segment(segment)
        
        # Calculate time remaining for retry
        now = datetime.now(timezone.utc)
        
        # Ensure segment.updated_at is timezone-aware
        segment_updated = segment.updated_at
        if segment_updated.tzinfo is None:
            segment_updated = segment_updated.replace(tzinfo=timezone.utc)
        
        hours_since_failure = (now - segment_updated).total_seconds() / 3600
        hours_remaining = max(0, 48 - hours_since_failure)
        
        return SegmentRetryStatusResponse(
            segment_id=segment_id,
            status=segment.status,
            can_retry=can_retry,
            reason=reason,
            last_updated=segment_updated.isoformat(),
            hours_since_failure=round(hours_since_failure, 2),
            hours_remaining_for_retry=round(hours_remaining, 2),
            retry_deadline=(segment_updated + timedelta(hours=48)).isoformat()
        )
    
    async def _retry_segment_async(self, segment_id: int, file_uri: str, mime_type: str) -> None:
        """Retry processing a single failed segment."""
        semaphore = asyncio.Semaphore(1)  # Single segment retry
        
        async def process_single_segment(seg_id: int, seg_type: SegmentType, page_start: int, page_end: int, sections: list[DocumentSection] | None = None) -> bool:
            async with semaphore:
                # Check if run is already skipped before processing
                db_check = SessionLocal()
                try:
                    segment_check = db_check.get(ExtractionSegment, seg_id)
                    if not segment_check:
                        return False
                    run_check = db_check.get(ExtractionRun, segment_check.extraction_run_id)
                    if run_check and run_check.status == ExtractionRunStatus.SKIPPED:
                        # Mark segment as skipped
                        if segment_check.status in (SegmentStatus.PENDING, SegmentStatus.RUNNING, SegmentStatus.FAILED):
                            segment_check.status = SegmentStatus.SKIPPED
                            segment_check.raw_response_json = {"skipped": True, "reason": run_check.error_message or "Run was skipped"}
                            db_check.commit()
                            logger.info(
                                f"Segment retry skipped due to run being skipped: segment_id={seg_id}"
                            )
                        return False
                finally:
                    db_check.close()
                
                segment_process_start = time.time()
                try:
                    parsed = await asyncio.to_thread(
                        extract_segment,
                        file_uri,
                        mime_type,
                        seg_type.name,
                        page_start,
                        page_end,
                        sections,
                    )
                    
                    # Get extraction_run_id for centralized processing
                    db = SessionLocal()
                    try:
                        segment = db.get(ExtractionSegment, seg_id)
                        if not segment:
                            return False
                        extraction_run_id_local = segment.extraction_run_id
                    finally:
                        db.close()
                    
                    # Use centralized response processing
                    self._process_segment_response(seg_id, seg_type, parsed, extraction_run_id_local)
                    
                    duration = time.time() - segment_process_start
                    logger.info(
                        f"Segment retry successful: segment_id={seg_id}, "
                        f"segment_type={seg_type.name}, pages={page_start}-{page_end}, "
                        f"duration={duration:.2f}s"
                    )
                    return True
                        
                except Exception as exc:
                    duration = time.time() - segment_process_start
                    logger.error(
                        f"Segment retry failed: segment_id={seg_id}, "
                        f"segment_type={seg_type.name}, pages={page_start}-{page_end}, "
                        f"duration={duration:.2f}s, error={type(exc).__name__}: {str(exc)}",
                        exc_info=True,
                    )
                    
                    # Update segment with failure
                    db = SessionLocal()
                    try:
                        segment = db.get(ExtractionSegment, seg_id)
                        if segment:
                            segment.status = SegmentStatus.FAILED
                            segment.raw_response_json = {"error": str(exc)}
                            db.commit()
                    finally:
                        db.close()
                    return False
        
        # Get segment details
        db = SessionLocal()
        try:
            segment = db.get(ExtractionSegment, segment_id)
            if not segment:
                logger.error(f"Segment not found for retry: segment_id={segment_id}")
                return
                
            # Reset segment status to RUNNING
            segment.status = SegmentStatus.RUNNING
            segment.raw_response_json = None
            segment.parsed_header_json = None
            segment.parsed_list_json = None
            db.commit()
            db.refresh(segment)
            
            logger.info(f"Starting segment retry: segment_id={segment_id}")
            
            # Get sections from database if retrying a LIST_CHUNK segment
            sections_list: list[DocumentSection] = []
            if segment.segment_type == SegmentType.LIST_CHUNK:
                extraction_run_id = segment.extraction_run_id
                run_for_sections = db.get(ExtractionRun, extraction_run_id)
                if run_for_sections:
                    sections_list = self._get_document_sections(run_for_sections.document_id)
            
            # Process the segment
            success = await process_single_segment(
                segment.id, segment.segment_type, segment.page_start, segment.page_end, sections_list if sections_list else None
            )
            
            if success:
                logger.info(f"Segment retry completed successfully: segment_id={segment_id}")
                extraction_run_id = segment.extraction_run_id
                
                # If this was a HEADER segment retry, check for duplicates
                if segment.segment_type == SegmentType.HEADER:
                    db_check = SessionLocal()
                    try:
                        header_seg = db_check.get(ExtractionSegment, segment_id)
                        if header_seg and header_seg.status == SegmentStatus.DONE and header_seg.parsed_header_json:
                            header_json = header_seg.parsed_header_json
                            run = db_check.get(ExtractionRun, extraction_run_id)
                            if run:
                                is_duplicate, error_msg = self._check_duplicate_document(header_json, run.document_id)
                                if is_duplicate:
                                    # Skip all runs for documents with the same combination
                                    self._skip_duplicate_runs(header_json, run.document_id, error_msg)
                                    logger.info(
                                        f"Duplicate document detected after header retry: extraction_run_id={extraction_run_id}, "
                                        f"document_id={run.document_id}, {error_msg}"
                                    )
                                    return  # Don't process list chunks or merge
                    finally:
                        db_check.close()
                
                # If this was a LIST_CHUNK retry, update section start_serial_numbers
                if segment.segment_type == SegmentType.LIST_CHUNK:
                    self._process_section_start_positions(extraction_run_id)
                
                # Update extraction run status after retry
                self._update_extraction_run_status(extraction_run_id)
                
                # After successful retry, merge segments to update voters and final status
                self._merge_segments_and_persist(extraction_run_id=extraction_run_id)
                logger.info(
                    f"Segments merged after retry: segment_id={segment_id}, "
                    f"extraction_run_id={extraction_run_id}"
                )
            else:
                logger.error(f"Segment retry failed: segment_id={segment_id}")
                
        finally:
            db.close()
