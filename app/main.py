import logging
import os

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

# Get allowed origins from environment variable
# Default to localhost for development, but use env var in production
cors_origins_str = os.getenv(
    "CORS_ORIGINS",
    "http://localhost:3000,http://127.0.0.1:3000"
)
allowed_origins = [origin.strip() for origin in cors_origins_str.split(",") if origin.strip()]

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def on_startup() -> None:
    """Application startup event."""
    # Database schema is managed by Alembic migrations.
    # Run migrations manually: `alembic upgrade head`
    # Or configure migrations to run automatically in CI/CD pipeline.
    # Do not use Base.metadata.create_all() in production.
    pass


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
