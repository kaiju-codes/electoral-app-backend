import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.api import api_router
from app.config import get_settings
from app.db import Base, engine

settings = get_settings()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title=settings.app_name,
    description="Electoral Data Extraction API - Clean Architecture",
    version="2.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],  # React dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def on_startup() -> None:
    """Initialize database tables on startup."""
    # For early development we let SQLAlchemy create tables from models.
    # In production, Alembic migrations should be the source of truth.
    Base.metadata.create_all(bind=engine)


@app.get("/health")
def health_check() -> dict:
    """Health check endpoint."""
    return {"status": "ok", "version": "2.0.0"}


# Include API router
app.include_router(api_router, prefix="/api/v1")

# Root endpoint
@app.get("/")
def root() -> dict:
    """Root endpoint."""
    return {
        "message": "Electoral Data Extraction API v2.0",
        "docs": "/docs",
        "health": "/health"
    }
