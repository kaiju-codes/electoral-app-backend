"""Pydantic schemas for API key settings."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from app.models.core import ApiKeyProvider


class ApiKeySettingsBase(BaseModel):
    """Base schema for API key settings."""
    provider_type: ApiKeyProvider


class ApiKeySettingsCreate(ApiKeySettingsBase):
    """Schema for creating/updating API key settings."""
    api_key: str = Field(..., min_length=1, description="Plain text API key to encrypt and store")
    is_active: Optional[bool] = Field(default=False, description="Whether this provider should be active")


class ApiKeySettingsUpdate(BaseModel):
    """Schema for updating API key settings."""
    api_key: Optional[str] = Field(None, min_length=1, description="Plain text API key to encrypt and store")
    is_active: Optional[bool] = Field(None, description="Whether this provider should be active")


class ApiKeySettingsRead(ApiKeySettingsBase):
    """Schema for reading API key settings (excludes encrypted key)."""
    id: int
    is_active: bool
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class ApiKeySettingsResponse(BaseModel):
    """Extended response schema with provider-specific information."""
    id: int
    provider_type: ApiKeyProvider
    is_active: bool
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    has_key: bool = Field(..., description="Whether an API key is configured")
    masked_key: Optional[str] = Field(None, description="Masked API key (first 7 chars + asterisks) for display only")
    key_generation_url: Optional[str] = Field(None, description="URL to generate API key for this provider")
    
    class Config:
        from_attributes = True

