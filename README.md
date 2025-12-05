# Electoral Data Extraction API

FastAPI-based backend service for extracting and managing electoral roll data from PDF documents using Google Gemini AI.

## Architecture

This application follows a **clean architecture** pattern with clear separation of concerns:

```
app/
├── main.py              # FastAPI application entry point
├── config.py            # Configuration and settings management
├── db.py                # Database connection and session management
├── api/                 # API layer
│   └── v1/
│       ├── api.py       # Main API router
│       ├── routes/      # Route definitions
│       └── controllers/ # Request/response handlers
├── core/                # Business logic layer
│   ├── document_service.py
│   ├── extraction_service.py
│   ├── voter_service.py
│   ├── location_service.py
│   ├── settings_service.py
│   └── encryption.py
├── models/              # Database models (SQLAlchemy)
│   └── core.py
├── schemas/             # Pydantic schemas for API validation
│   ├── documents.py
│   ├── extraction.py
│   ├── voters.py
│   ├── locations.py
│   └── settings.py
└── services/            # External service integrations
    └── gemini_client.py # Google Gemini AI client
```

## Components

### Application Entry Point

- **`main.py`**: FastAPI application initialization, CORS configuration, database table creation, and route registration.

### Configuration

- **`config.py`**: Centralized settings management using Pydantic. Handles:
  - Database connection URL
  - Gemini API configuration
  - Application settings
  - Environment variable loading

### Database

- **`db.py`**: SQLAlchemy setup with:
  - Database engine and session factory
  - Context manager for transactional operations
  - FastAPI dependency for database sessions

### API Layer

#### Routes (`api/v1/routes/`)
- `documents.py` - Document upload and management
- `voters.py` - Voter data queries
- `extractions.py` - Extraction run management
- `locations.py` - Location/constituency data
- `settings.py` - Application settings and API key management

#### Controllers (`api/v1/controllers/`)
- Handle HTTP request/response logic
- Validate input using Pydantic schemas
- Delegate business logic to services
- Return appropriate HTTP status codes

### Business Logic (`core/`)

- **`document_service.py`**: Document upload, processing, and management
- **`extraction_service.py`**: Orchestrates extraction runs, segment processing, and voter data extraction
- **`voter_service.py`**: Voter data queries, filtering, and aggregation
- **`location_service.py`**: Location/constituency management
- **`settings_service.py`**: Application settings and API key management
- **`encryption.py`**: Encryption utilities for sensitive data

### Data Models (`models/core.py`)

SQLAlchemy ORM models defining the database schema:
- `Document` - PDF document metadata
- `DocumentSection` - Document sections/chunks
- `DocumentHeader` - Document header information
- `ExtractionRun` - Extraction execution tracking
- `ExtractionSegment` - Individual segment processing status
- `Voter` - Extracted voter records
- `Location` - Location/constituency data
- `Setting` - Application configuration

### Schemas (`schemas/`)

Pydantic models for:
- Request validation
- Response serialization
- Type safety
- API documentation

### External Services (`services/`)

- **`gemini_client.py`**: Integration with Google Gemini AI for:
  - PDF file uploads
  - Text extraction
  - Structured data extraction from electoral rolls

## Key Features

1. **Document Processing**: Upload PDF documents, extract pages, and process them in chunks
2. **AI-Powered Extraction**: Uses Google Gemini AI to extract structured voter data from PDFs
3. **Background Processing**: Asynchronous document processing with status tracking
4. **Data Management**: Store and query extracted voter records with filtering and pagination
5. **Location Management**: Handle constituency and location data
6. **Settings Management**: Configure API keys and application settings via database

## API Endpoints

All endpoints are prefixed with `/api/v1`:

- **Documents**: `/documents/*` - Upload, list, and retrieve documents
- **Voters**: `/voters/*` - Query and filter voter data
- **Extractions**: `/extractions/*` - Manage extraction runs
- **Locations**: `/locations/*` - Location/constituency operations
- **Settings**: `/settings/*` - Application settings and API keys

## Environment Variables

Required environment variables (see `config.py`):

- `DATABASE_URL` - PostgreSQL connection string
- `GEMINI_API_KEY` - Google Gemini API key (optional if using database-stored keys)
- `GEMINI_MODEL` - Gemini model name (default: `gemini-2.5-pro`)
- `GEMINI_MAX_PAGES_PER_CALL` - Maximum pages per API call (default: 10)
- `ELECTORAL_ROLL_PROMPT_VERSION` - Prompt version identifier (default: `v1`)

## Database

The application uses PostgreSQL with SQLAlchemy ORM. Database tables are automatically created on startup (development mode). For production, use Alembic migrations.

## Dependencies

Key dependencies (see `requirements.txt`):
- `fastapi` - Web framework
- `sqlalchemy` - ORM
- `psycopg2` - PostgreSQL adapter
- `pydantic` - Data validation
- `google-genai` - Gemini AI client
- `pypdf` - PDF processing

## Development

1. Set up environment variables in `.env` file
2. Ensure PostgreSQL is running
3. Start the application:
   ```bash
   uvicorn app.main:app --reload
   ```
4. Access API documentation at `http://localhost:8000/docs`

## Architecture Principles

- **Separation of Concerns**: Clear boundaries between API, business logic, and data layers
- **Dependency Injection**: Services receive database sessions via dependency injection
- **Type Safety**: Extensive use of Pydantic and type hints
- **Error Handling**: Proper exception handling with HTTP status codes
- **Background Tasks**: Long-running operations use FastAPI background tasks
- **Transaction Management**: Database operations wrapped in transactions

