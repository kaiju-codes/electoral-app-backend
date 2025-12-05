"""Settings controller for handling API key settings endpoints."""

from typing import List

from fastapi import HTTPException, Depends
from sqlalchemy.orm import Session

from app.core.settings_service import SettingsService
from app.db import get_db
from app.models.core import ApiKeyProvider
from app.schemas.settings import (
    ApiKeySettingsRead,
    ApiKeySettingsCreate,
    ApiKeySettingsUpdate,
    ApiKeySettingsResponse,
)


class SettingsController:
    """Controller for settings operations."""
    
    @staticmethod
    def list_api_key_settings(
        db: Session = Depends(get_db),
    ) -> List[ApiKeySettingsResponse]:
        """List all API key settings."""
        service = SettingsService(db)
        settings = service.get_api_key_settings()
        
        result = []
        for setting in settings:
            config = SettingsService.PROVIDER_CONFIG.get(setting.provider_type, {})
            masked_key = service.get_masked_api_key(setting.provider_type)
            result.append(
                ApiKeySettingsResponse(
                    id=setting.id,
                    provider_type=setting.provider_type,
                    is_active=setting.is_active,
                    created_at=setting.created_at,
                    updated_at=setting.updated_at,
                    has_key=True,
                    masked_key=masked_key,
                    key_generation_url=config.get("key_generation_url"),
                )
            )
        
        # Also include providers that don't have settings yet
        all_providers = list(ApiKeyProvider)
        existing_providers = {s.provider_type for s in settings}
        for provider in all_providers:
            if provider not in existing_providers:
                config = SettingsService.PROVIDER_CONFIG.get(provider, {})
                result.append(
                    ApiKeySettingsResponse(
                        id=0,  # Placeholder
                        provider_type=provider,
                        is_active=False,
                        created_at=None,  # type: ignore
                        updated_at=None,  # type: ignore
                        has_key=False,
                        masked_key=None,
                        key_generation_url=config.get("key_generation_url"),
                    )
                )
        
        return result
    
    @staticmethod
    def get_api_key_setting(
        provider: ApiKeyProvider,
        db: Session = Depends(get_db),
    ) -> ApiKeySettingsResponse:
        """Get API key setting for a specific provider."""
        service = SettingsService(db)
        setting = service.get_api_key_setting(provider)
        
        config = SettingsService.PROVIDER_CONFIG.get(provider, {})
        
        if not setting:
            return ApiKeySettingsResponse(
                id=0,  # Placeholder
                provider_type=provider,
                is_active=False,
                created_at=None,  # type: ignore
                updated_at=None,  # type: ignore
                has_key=False,
                masked_key=None,
                key_generation_url=config.get("key_generation_url"),
            )
        
        masked_key = service.get_masked_api_key(provider)
        return ApiKeySettingsResponse(
            id=setting.id,
            provider_type=setting.provider_type,
            is_active=setting.is_active,
            created_at=setting.created_at,
            updated_at=setting.updated_at,
            has_key=True,
            masked_key=masked_key,
            key_generation_url=config.get("key_generation_url"),
        )
    
    @staticmethod
    def create_or_update_api_key(
        data: ApiKeySettingsCreate,
        db: Session = Depends(get_db),
    ) -> ApiKeySettingsRead:
        """Create or update API key for a provider."""
        service = SettingsService(db)
        
        try:
            setting = service.create_or_update_api_key(
                provider=data.provider_type,
                api_key=data.api_key,
                is_active=data.is_active,
            )
            return ApiKeySettingsRead.model_validate(setting)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
    
    @staticmethod
    def update_api_key(
        provider: ApiKeyProvider,
        data: ApiKeySettingsUpdate,
        db: Session = Depends(get_db),
    ) -> ApiKeySettingsRead:
        """Update API key setting for a provider."""
        service = SettingsService(db)
        
        setting = service.get_api_key_setting(provider)
        if not setting:
            raise HTTPException(
                status_code=404,
                detail=f"API key not found for provider: {provider.value}",
            )
        
        try:
            if data.api_key is not None:
                setting = service.create_or_update_api_key(
                    provider=provider,
                    api_key=data.api_key,
                    is_active=data.is_active if data.is_active is not None else setting.is_active,
                )
            elif data.is_active is not None:
                if data.is_active:
                    setting = service.set_active_provider(provider)
                else:
                    setting.is_active = False
                    db.commit()
                    db.refresh(setting)
            
            return ApiKeySettingsRead.model_validate(setting)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
    
    @staticmethod
    def activate_provider(
        provider: ApiKeyProvider,
        db: Session = Depends(get_db),
    ) -> ApiKeySettingsRead:
        """Activate a provider (set as active and deactivate others)."""
        service = SettingsService(db)
        
        try:
            setting = service.set_active_provider(provider)
            return ApiKeySettingsRead.model_validate(setting)
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
    
    @staticmethod
    def delete_api_key(
        provider: ApiKeyProvider,
        db: Session = Depends(get_db),
    ) -> dict:
        """Delete API key for a provider."""
        service = SettingsService(db)
        
        deleted = service.delete_api_key(provider)
        if not deleted:
            raise HTTPException(
                status_code=404,
                detail=f"API key not found for provider: {provider.value}",
            )
        
        return {"message": f"API key deleted for provider: {provider.value}"}

