"""Main API v1 router."""

from fastapi import APIRouter

from app.api.v1.routes.documents import router as documents_router
from app.api.v1.routes.extractions import router as extractions_router
from app.api.v1.routes.locations import router as locations_router
from app.api.v1.routes.settings import router as settings_router
from app.api.v1.routes.voters import router as voters_router

api_router = APIRouter()

# Include all route modules
api_router.include_router(documents_router)
api_router.include_router(voters_router)
api_router.include_router(extractions_router)
api_router.include_router(locations_router)
api_router.include_router(settings_router)
