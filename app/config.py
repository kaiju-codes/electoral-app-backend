import os
from functools import lru_cache

from dotenv import load_dotenv
from pydantic import BaseModel

# Load environment variables from .env file
load_dotenv()


class Settings(BaseModel):
    app_name: str = "Electoral Roll Ingest Service"

    # Database
    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg2://postgres:postgres@localhost:5432/electoral_roll",
    )

    # Encryption
    api_key_encryption_secret: str = os.getenv(
        "API_KEY_ENCRYPTION_SECRET",
        "default-dev-secret-change-in-production",
    )

    # Gemini / Google GenAI
    gemini_api_key: str | None = os.getenv("GEMINI_API_KEY")
    gemini_model: str = os.getenv("GEMINI_MODEL", "gemini-2.5-pro")
    gemini_max_pages_per_call: int = int(os.getenv("GEMINI_MAX_PAGES_PER_CALL", "8"))
    gemini_http_timeout_ms: int = int(os.getenv("GEMINI_HTTP_TIMEOUT_MS", "300000"))  # 5 minutes default

    electoral_roll_prompt_version: str = os.getenv(
        "ELECTORAL_ROLL_PROMPT_VERSION", "v1"
    )
    
    # Logging
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    log_format: str | None = os.getenv("LOG_FORMAT", None)


@lru_cache
def get_settings() -> Settings:
    return Settings()


