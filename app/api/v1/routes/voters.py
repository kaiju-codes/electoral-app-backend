"""Voter routes."""

from fastapi import APIRouter

from app.api.v1.controllers.voter_controller import VoterController
from app.schemas.voters import VoterListResponse

router = APIRouter(prefix="/voters", tags=["voters"])

# List voters
router.get("", response_model=VoterListResponse)(VoterController.list_voters)

# Export voters
router.get("/export")(VoterController.export_voters)
