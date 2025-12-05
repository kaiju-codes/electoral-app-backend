"""Location controller for handling location-related API endpoints."""

from typing import Dict, List

from fastapi import Depends
from sqlalchemy.orm import Session

from app.core.location_service import LocationService
from app.db import get_db


class LocationController:
    """Controller for location operations."""
    
    @staticmethod
    def get_states(db: Session = Depends(get_db)) -> Dict[str, List[str]]:
        """Get list of unique states from document headers."""
        service = LocationService(db)
        return service.get_states()
    
    @staticmethod
    def get_constituencies(state: str, db: Session = Depends(get_db)) -> Dict[str, List[Dict[str, str | int]]]:
        """Get list of assembly constituencies for a given state."""
        service = LocationService(db)
        return service.get_constituencies(state)
