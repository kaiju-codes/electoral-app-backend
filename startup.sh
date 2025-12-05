#!/bin/bash
# Azure App Service startup script for FastAPI application

# Use PORT environment variable if provided, otherwise default to 8000
PORT=${PORT:-8000}

# Start Gunicorn with the FastAPI app
exec gunicorn app.main:app \
    --bind 0.0.0.0:${PORT} \
    --workers 4 \
    --worker-class uvicorn.workers.UvicornWorker \
    --timeout 120 \
    --access-logfile - \
    --error-logfile -

