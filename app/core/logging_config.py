"""
Logging configuration for the application.
Provides structured logging that works well with Azure App Service.
"""
import logging
import sys
from typing import Optional


def setup_logging(
    log_level: str = "INFO",
    log_format: Optional[str] = None,
    include_timestamp: bool = True,
) -> None:
    """
    Configure application-wide logging.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Custom log format string. If None, uses default structured format.
        include_timestamp: Whether to include timestamp in log messages.
    """
    # Convert string level to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Default format: structured for Azure App Service
    if log_format is None:
        if include_timestamp:
            log_format = "%(asctime)s [%(levelname)-8s] [%(name)s] %(message)s"
        else:
            log_format = "[%(levelname)-8s] [%(name)s] %(message)s"
    
    # Configure root logger
    logging.basicConfig(
        level=numeric_level,
        format=log_format,
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout)  # Use stdout for Azure App Service
        ],
        force=True,  # Override any existing configuration
    )
    
    # Set specific logger levels
    # Reduce noise from third-party libraries
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.error").setLevel(logging.INFO)
    logging.getLogger("gunicorn.access").setLevel(logging.WARNING)
    logging.getLogger("gunicorn.error").setLevel(logging.INFO)
    
    # SQLAlchemy logging (can be verbose)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)
    
    # HTTPX logging
    logging.getLogger("httpx").setLevel(logging.WARNING)
    
    # Get logger for this module
    logger = logging.getLogger(__name__)
    logger.info(f"Logging configured: level={log_level}, format={log_format}")


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for a module.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)

