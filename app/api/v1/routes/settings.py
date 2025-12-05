"""Settings routes."""

from typing import List

from fastapi import APIRouter

from app.api.v1.controllers.settings_controller import SettingsController
from app.models.core import ApiKeyProvider
from app.schemas.settings import (
    ApiKeySettingsRead,
    ApiKeySettingsResponse,
)

router = APIRouter(prefix="/settings", tags=["settings"])

# List all API key settings
router.get(
    "/api-keys",
    response_model=List[ApiKeySettingsResponse],
)(SettingsController.list_api_key_settings)

# Get specific provider API key setting
router.get(
    "/api-keys/{provider}",
    response_model=ApiKeySettingsResponse,
)(SettingsController.get_api_key_setting)

# Create or update API key
router.post(
    "/api-keys",
    response_model=ApiKeySettingsRead,
)(SettingsController.create_or_update_api_key)

# Update API key (partial update)
router.put(
    "/api-keys/{provider}",
    response_model=ApiKeySettingsRead,
)(SettingsController.update_api_key)

# Activate provider
router.put(
    "/api-keys/{provider}/activate",
    response_model=ApiKeySettingsRead,
)(SettingsController.activate_provider)

# Delete API key
router.delete(
    "/api-keys/{provider}",
)(SettingsController.delete_api_key)

