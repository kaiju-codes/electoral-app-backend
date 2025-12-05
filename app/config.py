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

    # Gemini / Google GenAI
    gemini_api_key: str | None = os.getenv("GEMINI_API_KEY")
    gemini_model: str = os.getenv("GEMINI_MODEL", "gemini-2.5-pro")
    gemini_max_pages_per_call: int = int(os.getenv("GEMINI_MAX_PAGES_PER_CALL", "10"))

    electoral_roll_prompt_version: str = os.getenv(
        "ELECTORAL_ROLL_PROMPT_VERSION", "v1"
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()


