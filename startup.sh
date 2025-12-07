#!/bin/bash
# Azure App Service startup script for FastAPI application.
# Oryx is responsible for creating/using the virtualenv (antenv) and
# wiring up PATH/PYTHONPATH. This script should ONLY start Gunicorn.

# Use PORT environment variable if provided, otherwise default to 8000
PORT=${PORT:-8000}

echo "Starting Gunicorn for FastAPI app on port ${PORT}..."

exec gunicorn app.main:app \
    --bind "0.0.0.0:${PORT}" \
    --workers 4 \
    --worker-class uvicorn.workers.UvicornWorker \
    --timeout 120 \
    --access-logfile - \
    --error-logfile -

