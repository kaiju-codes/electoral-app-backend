"""Location routes."""

from fastapi import APIRouter

from app.api.v1.controllers.location_controller import LocationController
from app.schemas.locations import ConstituenciesResponse, StatesResponse

router = APIRouter(tags=["locations"])

# States
router.get("/states", response_model=StatesResponse)(LocationController.get_states)

# Constituencies
router.get("/constituencies", response_model=ConstituenciesResponse)(LocationController.get_constituencies)
