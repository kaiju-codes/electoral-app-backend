#!/bin/bash
# Azure App Service startup script for FastAPI application.
# Oryx is responsible for creating/using the virtualenv (antenv) and
# wiring up PATH/PYTHONPATH. This script should ONLY start Gunicorn.

# Use PORT environment variable if provided, otherwise default to 8000
PORT=${PORT:-8000}

# Log environment information
echo "============================================================"
echo "Azure App Service Startup Script"
echo "============================================================"
echo "Python version: $(python --version 2>&1)"
echo "Python path: $(which python)"
echo "Working directory: $(pwd)"
echo "PORT: ${PORT}"
echo "PYTHONPATH: ${PYTHONPATH:-not set}"
echo "Virtual environment: ${VIRTUAL_ENV:-not set}"
if [ -d "./antenv" ]; then
    echo "antenv directory: exists"
elif [ -d "/home/site/wwwroot/antenv" ]; then
    echo "antenv directory: exists at /home/site/wwwroot/antenv"
else
    echo "antenv directory: not found (Oryx should create it)"
fi
echo "============================================================"
echo "Starting Gunicorn for FastAPI app on port ${PORT}..."

exec gunicorn app.main:app \
    --bind "0.0.0.0:${PORT}" \
    --workers 4 \
    --worker-class uvicorn.workers.UvicornWorker \
    --timeout 120 \
    --access-logfile - \
    --error-logfile -

