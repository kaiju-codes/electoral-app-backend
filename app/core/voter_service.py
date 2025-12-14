"""Voter service for handling voter-related business logic."""

import csv
import io
import logging
from typing import Optional

from sqlalchemy.orm import Session

from app.models.core import DocumentHeader, DocumentSection, Voter
from app.schemas.documents import DocumentSectionRead
from app.schemas.voters import VoterListResponse, VoterRead

logger = logging.getLogger(__name__)


class VoterService:
    """Service for voter operations."""
    
    def __init__(self, db: Session):
        self.db = db
    
    def _get_document_sections_cache(self, document_ids: list[int]) -> dict[int, list[DocumentSection]]:
        """Load and cache document sections for given document IDs."""
        if not document_ids:
            return {}
        
        sections = (
            self.db.query(DocumentSection)
            .filter(DocumentSection.document_id.in_(document_ids))
            .all()
        )
        
        # Group by document_id
        cache: dict[int, list[DocumentSection]] = {}
        for section in sections:
            if section.document_id not in cache:
                cache[section.document_id] = []
            cache[section.document_id].append(section)
        
        return cache
    
    def _find_voter_section(
        self, voter_serial: int | None, sections: list[DocumentSection]
    ) -> DocumentSection | None:
        """
        Find which section a voter belongs to based on serial_number.
        
        Returns the section where start_serial_number <= voter_serial < next_section.start_serial_number.
        If no next section exists, returns the last section with start_serial_number <= voter_serial.
        Returns None if no matching section found.
        
        This method correctly handles multiple occurrences of the same section_id at different
        serial number ranges (e.g., section_one at 1-50 and again at 101-110) by sorting
        sections by start_serial_number and finding the appropriate range.
        """
        if voter_serial is None or not sections:
            return None
        
        # Filter and sort sections by start_serial_number (ascending), then by section_id for consistency
        # This handles multiple occurrences of the same section_id correctly
        sorted_sections = sorted(
            [s for s in sections if s.start_serial_number is not None],
            key=lambda s: (s.start_serial_number, s.section_id)
        )
        
        if not sorted_sections:
            return None
        
        # Find section where start_serial_number <= voter_serial < next_section.start_serial_number
        for i, section in enumerate(sorted_sections):
            if section.start_serial_number <= voter_serial:
                # Check if there's a next section
                if i + 1 < len(sorted_sections):
                    next_start = sorted_sections[i + 1].start_serial_number
                    if voter_serial < next_start:
                        return section
                else:
                    # Last section, voter is in this section
                    return section
        
        return None
    
    def list_voters(
        self,
        state: str,
        assembly_constituency_number: str,
        search: Optional[str] = None,
        search_type: Optional[str] = None,
        part_number: Optional[str] = None,
        polling_station: Optional[str] = None,
        gender: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "asc",
        page: int = 1,
        page_size: int = 50,
    ) -> VoterListResponse:
        """List voters with filtering and pagination."""
        # Join with document_header to filter by state and assembly_constituency_number
        # Use case-insensitive comparison for state
        query = (
            self.db.query(Voter)
            .join(DocumentHeader, Voter.document_id == DocumentHeader.document_id)
            .filter(DocumentHeader.state.ilike(state))
        )
        
        # Filter by assembly_constituency_number (check both local and english fields)
        # Convert string to int for english field comparison
        try:
            ac_number_int = int(assembly_constituency_number) if assembly_constituency_number else None
        except (ValueError, TypeError):
            ac_number_int = None
        
        if ac_number_int is not None:
            query = query.filter(
                (DocumentHeader.assembly_constituency_number_english == ac_number_int)
                | (DocumentHeader.assembly_constituency_number_local == assembly_constituency_number)
            )
        else:
            # Fallback to string comparison for local field only
            query = query.filter(
                DocumentHeader.assembly_constituency_number_local == assembly_constituency_number
            )
        
        # Apply additional filters
        if part_number:
            query = query.filter(DocumentHeader.part_number == part_number)
        
        if polling_station:
            query = query.filter(
                (DocumentHeader.polling_station_name_english.ilike(f"%{polling_station}%"))
                | (DocumentHeader.polling_station_name_local.ilike(f"%{polling_station}%"))
                | (DocumentHeader.polling_station_number_english == polling_station)
                | (DocumentHeader.polling_station_number_local == polling_station)
            )
        
        if gender and gender.lower() != "all":
            query = query.filter(Voter.gender == gender)
        
        # Apply search if provided
        if search and search.strip():
            search_term = f"%{search.strip()}%"
            
            if search_type == "name":
                query = query.filter(
                    (Voter.voter_name_english.ilike(search_term))
                    | (Voter.voter_name_local.ilike(search_term))
                )
            elif search_type == "father_name":
                query = query.filter(
                    (Voter.relation_name_english.ilike(search_term))
                    | (Voter.relation_name_local.ilike(search_term))
                )
            elif search_type == "epic":
                query = query.filter(Voter.photo_id.ilike(search_term))
            elif search_type == "house_no":
                query = query.filter(Voter.house_number.ilike(search_term))
            else:
                # Default: search across all fields
                query = query.filter(
                    (Voter.voter_name_english.ilike(search_term))
                    | (Voter.voter_name_local.ilike(search_term))
                    | (Voter.relation_name_english.ilike(search_term))
                    | (Voter.relation_name_local.ilike(search_term))
                    | (Voter.house_number.ilike(search_term))
                    | (Voter.photo_id.ilike(search_term))
                    | (Voter.serial_number.ilike(search_term))
                )
        
        # Get total count before pagination
        total = query.count()
        
        # Apply sorting
        if sort_by == "created_at":
            order_field = Voter.created_at
        elif sort_by == "name":
            order_field = Voter.voter_name_english
        elif sort_by == "father_name":
            order_field = Voter.relation_name_english
        elif sort_by == "part_number":
            order_field = DocumentHeader.part_number
        else:
            order_field = Voter.serial_number  # default
        
        if sort_order == "desc":
            order_field = order_field.desc()
        
        # Apply pagination
        offset = (page - 1) * page_size
        voters = query.order_by(order_field).offset(offset).limit(page_size).all()
        
        # Load document sections cache for all unique document_ids
        unique_document_ids = list(set(voter.document_id for voter in voters))
        sections_cache = self._get_document_sections_cache(unique_document_ids)
        
        # Map to VoterRead with header context and document section
        items = []
        for voter in voters:
            header = self.db.get(DocumentHeader, voter.document_id)
            
            # Calculate document_section
            document_section = None
            sections = sections_cache.get(voter.document_id, [])
            if sections:
                found_section = self._find_voter_section(voter.serial_number, sections)
                if found_section:
                    document_section = DocumentSectionRead.model_validate(found_section)
            
            voter_read = VoterRead(
                id=voter.id,
                document_id=voter.document_id,
                serial_number=voter.serial_number,
                house_number=voter.house_number,
                voter_name_local=voter.voter_name_local,
                voter_name_english=voter.voter_name_english,
                relation_type=voter.relation_type,
                relation_name_local=voter.relation_name_local,
                relation_name_english=voter.relation_name_english,
                gender=voter.gender,
                age=voter.age,
                photo_id=voter.photo_id,
                state=header.state if header else None,
                assembly_constituency_number_english=(
                    header.assembly_constituency_number_english if header else None
                ),
                assembly_constituency_name_english=(
                    header.assembly_constituency_name_english if header else None
                ),
                part_number=header.part_number if header else None,
                document_section=document_section,
                created_at=voter.created_at,
                updated_at=voter.updated_at,
            )
            items.append(voter_read)
        
        logger.info(
            f"Voters list: page={page}, page_size={page_size}, "
            f"state={state}, assembly_constituency_number={assembly_constituency_number}, "
            f"gender={gender}, search={search}, search_type={search_type}, "
            f"total={total}, returned={len(items)}"
        )
        
        return VoterListResponse(
            items=items,
            total=total,
            page=page,
            page_size=page_size,
        )
    
    def export_voters_csv(
        self,
        state: str,
        assembly_constituency_number: str,
        search: Optional[str] = None,
        search_type: Optional[str] = None,
        part_number: Optional[str] = None,
        polling_station: Optional[str] = None,
        gender: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "asc",
    ) -> tuple[io.BytesIO, str]:
        """Export voters to CSV format."""
        # Build the same query as list_voters but without pagination
        # Use case-insensitive comparison for state
        query = (
            self.db.query(Voter)
            .join(DocumentHeader, Voter.document_id == DocumentHeader.document_id)
            .filter(DocumentHeader.state.ilike(state))
        )
        
        # Filter by assembly_constituency_number
        # Convert string to int for english field comparison
        try:
            ac_number_int = int(assembly_constituency_number) if assembly_constituency_number else None
        except (ValueError, TypeError):
            ac_number_int = None
        
        if ac_number_int is not None:
            query = query.filter(
                (DocumentHeader.assembly_constituency_number_english == ac_number_int)
                | (DocumentHeader.assembly_constituency_number_local == assembly_constituency_number)
            )
        else:
            # Fallback to string comparison for local field only
            query = query.filter(
                DocumentHeader.assembly_constituency_number_local == assembly_constituency_number
            )
        
        # Apply additional filters
        if part_number:
            query = query.filter(DocumentHeader.part_number == part_number)
        
        if polling_station:
            query = query.filter(
                (DocumentHeader.polling_station_name_english.ilike(f"%{polling_station}%"))
                | (DocumentHeader.polling_station_name_local.ilike(f"%{polling_station}%"))
                | (DocumentHeader.polling_station_number_english == polling_station)
                | (DocumentHeader.polling_station_number_local == polling_station)
            )
        
        if gender and gender.lower() != "all":
            query = query.filter(Voter.gender == gender)
        
        # Apply search if provided
        if search and search.strip():
            search_term = f"%{search.strip()}%"
            
            if search_type == "name":
                query = query.filter(
                    (Voter.voter_name_english.ilike(search_term))
                    | (Voter.voter_name_local.ilike(search_term))
                )
            elif search_type == "father_name":
                query = query.filter(
                    (Voter.relation_name_english.ilike(search_term))
                    | (Voter.relation_name_local.ilike(search_term))
                )
            elif search_type == "epic":
                query = query.filter(Voter.photo_id.ilike(search_term))
            elif search_type == "house_no":
                query = query.filter(Voter.house_number.ilike(search_term))
            else:
                # Default: search across all fields
                query = query.filter(
                    (Voter.voter_name_english.ilike(search_term))
                    | (Voter.voter_name_local.ilike(search_term))
                    | (Voter.relation_name_english.ilike(search_term))
                    | (Voter.relation_name_local.ilike(search_term))
                    | (Voter.house_number.ilike(search_term))
                    | (Voter.photo_id.ilike(search_term))
                    | (Voter.serial_number.ilike(search_term))
                )
        
        # Apply sorting
        if sort_by == "created_at":
            order_field = Voter.created_at
        elif sort_by == "name":
            order_field = Voter.voter_name_english
        elif sort_by == "father_name":
            order_field = Voter.relation_name_english
        elif sort_by == "part_number":
            order_field = DocumentHeader.part_number
        else:
            order_field = Voter.serial_number  # default
        
        if sort_order == "desc":
            order_field = order_field.desc()
        
        # Get all voters (no pagination for export)
        voters = query.order_by(order_field).all()
        
        # Load document sections cache for all unique document_ids
        unique_document_ids = list(set(voter.document_id for voter in voters))
        sections_cache = self._get_document_sections_cache(unique_document_ids)
        
        # Create CSV content
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow([
            'Serial Number', 'House Number', 'Voter Name (Local)', 'Voter Name (English)',
            'Relation Type', 'Relation Name (Local)', 'Relation Name (English)',
            'Gender', 'Age', 'Photo ID', 'State', 'Assembly Constituency',
            'Part Number', 'Polling Station', 'Section ID'
        ])
        
        # Write data
        for voter in voters:
            header = self.db.get(DocumentHeader, voter.document_id)
            
            # Calculate section_id
            section_id = ''
            sections = sections_cache.get(voter.document_id, [])
            if sections:
                found_section = self._find_voter_section(voter.serial_number, sections)
                if found_section:
                    section_id = str(found_section.section_id)
            
            writer.writerow([
                voter.serial_number or '',
                voter.house_number or '',
                voter.voter_name_local or '',
                voter.voter_name_english or '',
                voter.relation_type or '',
                voter.relation_name_local or '',
                voter.relation_name_english or '',
                voter.gender or '',
                voter.age or '',
                voter.photo_id or '',
                header.state if header else '',
                header.assembly_constituency_name_english if header else '',
                header.part_number if header else '',
                header.polling_station_name_english if header else '',
                section_id,
            ])
        
        output.seek(0)
        
        # Create filename
        filename = f"voters_{state}_{assembly_constituency_number}.csv"
        
        # Return as BytesIO
        csv_bytes = io.BytesIO(output.getvalue().encode('utf-8'))
        
        return csv_bytes, filename
