"""Location service for handling location-related business logic."""

import logging
from typing import Dict, List

from sqlalchemy.orm import Session

from app.models.core import DocumentHeader

logger = logging.getLogger(__name__)


class LocationService:
    """Service for location operations."""
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_states(self) -> Dict[str, List[str]]:
        """Get list of unique states from document headers."""
        states = (
            self.db.query(DocumentHeader.state)
            .filter(DocumentHeader.state.isnot(None))
            .distinct()
            .order_by(DocumentHeader.state)
            .all()
        )
        
        state_list = [state[0] for state in states if state[0]]
        
        logger.info(f"States list: returned {len(state_list)} states")
        
        return {"states": state_list}
    
    def get_constituencies(self, state: str) -> Dict[str, List[Dict[str, str | int]]]:
        """Get list of assembly constituencies for a given state."""
        constituencies = (
            self.db.query(
                DocumentHeader.assembly_constituency_number_english,
                DocumentHeader.assembly_constituency_number_local,
                DocumentHeader.assembly_constituency_name_english,
                DocumentHeader.assembly_constituency_name_local,
            )
            .filter(DocumentHeader.state == state)
            .filter(DocumentHeader.assembly_constituency_number_english.isnot(None))
            .distinct()
            .order_by(DocumentHeader.assembly_constituency_number_english)
            .all()
        )
        
        constituency_list = []
        for const in constituencies:
            if const[0] is not None:  # If English number exists
                constituency_list.append({
                    "number_english": const[0],  # Now an integer
                    "number_local": const[1],
                    "name_english": const[2],
                    "name_local": const[3],
                    "display_name": f"{const[0]} - {const[2] or const[3] or 'Unknown'}"
                })
        
        logger.info(f"Constituencies list: state={state}, returned {len(constituency_list)} constituencies")
        
        return {"constituencies": constituency_list}
