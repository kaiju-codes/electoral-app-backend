import os
import sys
import time
from typing import Callable

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.api import api_router
from app.config import get_settings
from app.core.logging_config import setup_logging, get_logger
from app.db import Base, check_database_connection, engine

settings = get_settings()

# Configure logging early, before other imports
setup_logging(
    log_level=settings.log_level,
    log_format=settings.log_format,
    include_timestamp=True,
)
logger = get_logger(__name__)

# Log Python and environment information at startup
logger.info("=" * 60)
logger.info(f"Python version: {sys.version}")
logger.info(f"Python executable: {sys.executable}")
logger.info(f"Application: {settings.app_name}")
logger.info(f"Log level: {settings.log_level}")
logger.info("=" * 60)

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


# Request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next: Callable) -> Response:
    """Log all incoming HTTP requests with timing information."""
    start_time = time.time()
    
    # Log request
    logger.info(
        f"Request: {request.method} {request.url.path} "
        f"| Client: {request.client.host if request.client else 'unknown'}"
    )
    
    # Process request
    response = await call_next(request)
    
    # Calculate processing time
    process_time = time.time() - start_time
    
    # Log response
    logger.info(
        f"Response: {request.method} {request.url.path} "
        f"| Status: {response.status_code} | Time: {process_time:.3f}s"
    )
    
    # Add timing header
    response.headers["X-Process-Time"] = str(process_time)
    
    return response


@app.on_event("startup")
def on_startup() -> None:
    """Application startup event."""
    logger.info("Starting FastAPI application...")
    logger.info(f"Database URL configured: {settings.database_url.split('@')[-1] if '@' in settings.database_url else '***'}")
    
    # Check database connection - this will raise an exception and fail startup if DB is not available
    logger.info("Checking database connection...")
    try:
        check_database_connection()
        logger.info("Database connection verified successfully")
    except ConnectionError as e:
        logger.error(f"Database connection check failed: {str(e)}")
        logger.error("Application startup aborted due to database connection failure")
        raise  # Re-raise to fail the deployment
    
    logger.info(f"Gemini Model: {settings.gemini_model}")
    logger.info(f"Gemini Max Pages Per Call: {settings.gemini_max_pages_per_call}")
    logger.info(f"Electoral Roll Prompt Version: {settings.electoral_roll_prompt_version}")
    # Database schema is managed by Alembic migrations.
    # Run migrations manually: `alembic upgrade head`
    # Or configure migrations to run automatically in CI/CD pipeline.
    # Do not use Base.metadata.create_all() in production.
    logger.info("FastAPI application started successfully")


@app.get("/health")
def health_check() -> dict:
    """Health check endpoint."""
    logger.debug("Health check endpoint called")
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
