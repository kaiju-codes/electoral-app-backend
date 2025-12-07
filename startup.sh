#!/bin/bash
# Azure App Service startup script for FastAPI application

# Use PORT environment variable if provided, otherwise default to 8000
PORT=${PORT:-8000}

# Try to activate virtual environment if it exists
# Oryx creates it in the extracted directory, which should be in PYTHONPATH
if [ -d "./antenv" ] && [ -f "./antenv/bin/activate" ]; then
    source ./antenv/bin/activate
elif [ -d "/home/site/wwwroot/antenv" ] && [ -f "/home/site/wwwroot/antenv/bin/activate" ]; then
    source /home/site/wwwroot/antenv/bin/activate
else
    # Fallback: install dependencies if venv doesn't exist
    echo "Virtual environment not found, installing dependencies..."
    python -m venv antenv
    source antenv/bin/activate
    pip install --upgrade pip
    if [ -f "requirements.txt" ]; then
        pip install -r requirements.txt
    elif [ -f "/home/site/wwwroot/requirements.txt" ]; then
        cd /home/site/wwwroot
        pip install -r requirements.txt
    fi
fi

# Start Gunicorn with the FastAPI app
exec gunicorn app.main:app \
    --bind 0.0.0.0:${PORT} \
    --workers 4 \
    --worker-class uvicorn.workers.UvicornWorker \
    --timeout 120 \
    --access-logfile - \
    --error-logfile -

