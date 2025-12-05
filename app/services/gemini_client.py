from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, Tuple

from google import genai

from app.config import get_settings
from app.core.settings_service import SettingsService
from app.models.core import ApiKeyProvider
from app.db import SessionLocal

logger = logging.getLogger(__name__)


settings = get_settings()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROMPT_PATH = PROJECT_ROOT / "prompts" / "electoral_roll_blueprint.txt"


def _load_blueprint_prompt() -> str:
    return PROMPT_PATH.read_text(encoding="utf-8")


BLUEPRINT_PROMPT = _load_blueprint_prompt()


def get_gemini_client() -> genai.Client:
    """
    Get Gemini client with API key from database or environment variable.
    
    Priority:
    1. Database API key (if configured and active)
    2. Environment variable (backward compatibility)
    3. Raise error if neither exists
    """
    api_key = None
    
    # Try database first
    try:
        db = SessionLocal()
        try:
            service = SettingsService(db)
            # Try active key first
            api_key = service.get_active_api_key(ApiKeyProvider.GEMINI)
            # If no active key, try any key as fallback
            if not api_key:
                api_key = service.get_any_api_key(ApiKeyProvider.GEMINI)
        finally:
            db.close()
    except Exception as e:
        logger.warning(f"Failed to get API key from database: {str(e)}")
    
    # Fall back to environment variable
    if not api_key:
        api_key = settings.gemini_api_key
    
    if not api_key:
        raise RuntimeError(
            "GEMINI_API_KEY is not configured. Please set it in Settings or "
            "configure GEMINI_API_KEY environment variable."
        )
    
    client = genai.Client(
        api_key=api_key,
    )
    return client


def upload_file(file_bytes: bytes, filename: str) -> Tuple[str, str, Dict[str, Any]]:
    """
    Upload a PDF to Gemini and return (file_uri, mime_type, metadata).

    This is a synchronous helper used by the extraction orchestration.
    """
    start_time = time.time()
    file_size_kb = len(file_bytes) / 1024
    logger.info(f"Uploading file to Gemini: filename={filename}, size={file_size_kb:.2f}KB")

    client = get_gemini_client()

    # The google-genai client expects a file path; write to a temp file.
    # Caller is responsible for managing where bytes come from (FastAPI UploadFile, etc.).
    from tempfile import NamedTemporaryFile

    try:
        with NamedTemporaryFile(suffix=".pdf") as tmp:
            tmp.write(file_bytes)
            tmp.flush()
            uploaded = client.files.upload(file=tmp.name)

        duration = time.time() - start_time
        file_uri = uploaded.uri
        mime_type = getattr(uploaded, "mime_type", "application/pdf")
        metadata = {
            "name": getattr(uploaded, "name", None),
            "size_bytes": getattr(uploaded, "size_bytes", None),
        }
        logger.info(
            f"File upload completed: filename={filename}, file_uri={file_uri}, "
            f"duration={duration:.2f}s, size_bytes={metadata.get('size_bytes')}"
        )
        return file_uri, mime_type, metadata
    except Exception as exc:
        duration = time.time() - start_time
        logger.error(
            f"File upload error: filename={filename}, duration={duration:.2f}s, "
            f"error={type(exc).__name__}: {str(exc)}",
            exc_info=True,
        )
        raise


def _build_segment_instruction(
    segment_type: str, page_start: int, page_end: int, sections: list | None = None
) -> str:
    if segment_type == "HEADER":
        return f"Process page {page_start} only for header information."
    
    instruction = (
        f"Process pages {page_start}–{page_end} only for voter-list data; "
        f"ignore the header page."
    )
    
    # Add section detection instructions for LIST_CHUNK segments
    if sections:
        section_list = ", ".join([
            f"Section {sec.section_id}: {sec.section_name_english or sec.section_name_local or ''}"
            for sec in sections
            if sec.section_id is not None
        ])
        instruction += (
            f"\n\nSECTION DETECTION:\n"
            f"The following section names were extracted from the header page: {section_list}\n"
            f"While processing the voter list, look for these section name markers on any page. "
            f"Section names typically appear above serial_number fields in the voter list. "
            f"For each section name you find, identify the serial_number where that section starts. "
            f"Return the section start positions in a 'section_start_positions' array with format: "
            f"[{{\"section_number\": <int>, \"start_serial_number\": <int>}}]. "
            f"Use the section_id (section number) from the list above to identify sections. "
            f"If a section is not found in these pages, do not include it in the array."
        )
    
    return instruction


def extract_segment(
    file_uri: str,
    mime_type: str,
    segment_type: str,
    page_start: int,
    page_end: int,
    sections: list | None = None,
) -> Dict[str, Any]:
    """
    Call Gemini for a single segment and return parsed JSON (header + list).

    This is written synchronously for now; an async wrapper can be added later.
    
    Args:
        file_uri: URI of the uploaded file in Gemini
        mime_type: MIME type of the file
        segment_type: Type of segment (HEADER or LIST_CHUNK)
        page_start: Starting page number
        page_end: Ending page number
        sections: Optional list of DocumentSection objects from database for LIST_CHUNK segments
    """
    start_time = time.time()
    client = get_gemini_client()

    segment_instruction = _build_segment_instruction(
        segment_type=segment_type, page_start=page_start, page_end=page_end, sections=sections
    )
    prompt = f"{BLUEPRINT_PROMPT}\n\nSEGMENT INSTRUCTION:\n{segment_instruction}"

    logger.info(
        f"Gemini call starting: segment_type={segment_type}, pages={page_start}-{page_end}, "
        f"model={settings.gemini_model}"
    )

    try:
        from google.genai import types
                
        response = client.models.generate_content(
            model=settings.gemini_model,
            contents=[
                {
                    "role": "user",
                    "parts": [
                        {
                            "file_data": {
                                "mime_type": mime_type,
                                "file_uri": file_uri,
                            }
                        },
                        {"text": prompt},
                    ],
                }
            ],
        )

        duration = time.time() - start_time

        # Extract text from response - Gemini may return it in different formats
        text = getattr(response, "text", None)
        # Normalize empty strings to None
        if text is not None and not text.strip():
            text = None
        
        if not text:
            # Try alternative response formats
            if hasattr(response, "candidates") and response.candidates:
                candidate = response.candidates[0]
                if hasattr(candidate, "content") and hasattr(candidate.content, "parts"):
                    parts = candidate.content.parts
                    if parts:
                        text = getattr(parts[0], "text", None)
                        # Normalize empty strings to None
                        if text is not None and not text.strip():
                            text = None
            
            # If still no text, check for errors in the response
            if not text:
                error_details = {}
                if hasattr(response, "prompt_feedback"):
                    error_details["prompt_feedback"] = str(response.prompt_feedback)
                if hasattr(response, "candidates") and response.candidates:
                    candidate = response.candidates[0]
                    if hasattr(candidate, "finish_reason"):
                        error_details["finish_reason"] = candidate.finish_reason
                    if hasattr(candidate, "safety_ratings"):
                        error_details["safety_ratings"] = str(candidate.safety_ratings)
                
                logger.error(
                    f"Gemini call failed: segment_type={segment_type}, pages={page_start}-{page_end}, "
                    f"duration={duration:.2f}s, error=Empty response, details={error_details}, "
                    f"response_type={type(response)}, response_attrs={dir(response)}"
                )
                raise RuntimeError(f"Empty response from Gemini for extract_segment. Details: {error_details}")

        # Clean the text - remove markdown code blocks if present
        text = text.strip()
        if text.startswith("```json"):
            text = text[7:]  # Remove ```json
        elif text.startswith("```"):
            text = text[3:]  # Remove ```
        if text.endswith("```"):
            text = text[:-3]  # Remove closing ```
        text = text.strip()

        if not text:
            logger.error(
                f"Gemini call failed: segment_type={segment_type}, pages={page_start}-{page_end}, "
                f"duration={duration:.2f}s, error=Empty text after cleaning"
            )
            raise RuntimeError("Empty text after cleaning response from Gemini.")

        # Check if response might be truncated (doesn't end with closing brace)
        if not text.rstrip().endswith('}'):
            logger.warning(
                f"Gemini response may be truncated: segment_type={segment_type}, pages={page_start}-{page_end}, "
                f"response_ends_with={repr(text[-100:])}"
            )
        
        # Response must be exactly the JSON object according to the blueprint schema.
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as e:
            # Log the actual response text (truncated) for debugging
            preview = text[:500] if len(text) > 500 else text
            logger.error(
                f"Gemini call JSON decode error: segment_type={segment_type}, pages={page_start}-{page_end}, "
                f"duration={duration:.2f}s, error={type(e).__name__}: {str(e)}, "
                f"response_preview={repr(preview)}"
            )
            raise RuntimeError(
                f"Failed to parse JSON response from Gemini: {str(e)}. "
                f"Response preview: {repr(preview)}"
            ) from e

        # Truncate response for logging (first 200 chars of JSON string)
        response_preview = json.dumps(parsed)[:200]
        logger.info(
            f"Gemini call completed: segment_type={segment_type}, pages={page_start}-{page_end}, "
            f"duration={duration:.2f}s, response_preview={response_preview}..."
        )

        # Log counts for metrics
        header_present = bool(parsed.get("header"))
        list_count = len(parsed.get("list", [])) if isinstance(parsed.get("list"), list) else 0
        logger.info(
            f"Gemini response summary: segment_type={segment_type}, has_header={header_present}, "
            f"list_items={list_count}"
        )

        return parsed
    except Exception as exc:
        duration = time.time() - start_time
        logger.error(
            f"Gemini call error: segment_type={segment_type}, pages={page_start}-{page_end}, "
            f"duration={duration:.2f}s, error={type(exc).__name__}: {str(exc)}",
            exc_info=True,
        )
        raise


