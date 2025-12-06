#!/bin/bash
# Azure App Service startup script for FastAPI application

# Install dependencies if not already installed (fallback if Oryx didn't run)
if [ ! -d "/home/site/wwwroot/antenv" ]; then
    echo "Virtual environment not found, installing dependencies..."
    cd /home/site/wwwroot
    python -m venv antenv
    source antenv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
fi

# Use PORT environment variable if provided, otherwise default to 8000
PORT=${PORT:-8000}

# Activate virtual environment if it exists
if [ -d "/home/site/wwwroot/antenv" ]; then
    source /home/site/wwwroot/antenv/bin/activate
fi

# Start Gunicorn with the FastAPI app
exec gunicorn app.main:app \
    --bind 0.0.0.0:${PORT} \
    --workers 4 \
    --worker-class uvicorn.workers.UvicornWorker \
    --timeout 120 \
    --access-logfile - \
    --error-logfile -

