"""Service for managing API key settings."""

import logging
from typing import Optional, List

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from app.models.core import ApiKeySettings, ApiKeyProvider
from app.core.encryption import encrypt_api_key, decrypt_api_key

logger = logging.getLogger(__name__)


class SettingsService:
    """Service for API key settings operations."""
    
    # Provider-specific configuration
    PROVIDER_CONFIG = {
        ApiKeyProvider.GEMINI: {
            "key_generation_url": "https://aistudio.google.com/app/api-keys",
        },
        ApiKeyProvider.GPT: {
            "key_generation_url": "https://platform.openai.com/api-keys",
        },
    }
    
    @staticmethod
    def mask_api_key(api_key: str) -> str:
        """
        Mask an API key showing only first 7 characters.
        
        Args:
            api_key: The API key to mask
            
        Returns:
            Masked API key (first 7 chars + asterisks)
        """
        if not api_key or len(api_key) <= 7:
            return "*" * 12  # Return 12 asterisks if key is too short
        return api_key[:7] + "*" * (len(api_key) - 7)
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_api_key_settings(
        self, provider: Optional[ApiKeyProvider] = None
    ) -> List[ApiKeySettings]:
        """
        Get API key settings for all providers or a specific provider.
        
        Args:
            provider: Optional provider type to filter by
            
        Returns:
            List of ApiKeySettings models
        """
        query = self.db.query(ApiKeySettings)
        if provider:
            query = query.filter(ApiKeySettings.provider_type == provider)
        return query.all()
    
    def get_api_key_setting(
        self, provider: ApiKeyProvider
    ) -> Optional[ApiKeySettings]:
        """
        Get API key setting for a specific provider.
        
        Args:
            provider: Provider type
            
        Returns:
            ApiKeySettings model or None if not found
        """
        return self.db.query(ApiKeySettings).filter(
            ApiKeySettings.provider_type == provider
        ).first()
    
    def create_or_update_api_key(
        self,
        provider: ApiKeyProvider,
        api_key: str,
        is_active: Optional[bool] = None,
    ) -> ApiKeySettings:
        """
        Create or update API key for a provider.
        
        Args:
            provider: Provider type
            api_key: Plain text API key to encrypt and store
            is_active: Whether to set this provider as active (None = keep current)
            
        Returns:
            Created or updated ApiKeySettings model
            
        Raises:
            ValueError: If encryption fails
        """
        try:
            # Encrypt the API key
            encrypted_key = encrypt_api_key(api_key)
            
            # Check if setting exists
            existing = self.get_api_key_setting(provider)
            
            if existing:
                # Update existing
                existing.encrypted_api_key = encrypted_key
                if is_active is not None:
                    existing.is_active = is_active
                self.db.commit()
                self.db.refresh(existing)
                logger.info(f"Updated API key for provider: {provider.value}")
                return existing
            else:
                # Create new
                new_setting = ApiKeySettings(
                    provider_type=provider,
                    encrypted_api_key=encrypted_key,
                    is_active=is_active if is_active is not None else False,
                )
                self.db.add(new_setting)
                self.db.commit()
                self.db.refresh(new_setting)
                logger.info(f"Created API key for provider: {provider.value}")
                
                # If this is set as active, deactivate others
                if new_setting.is_active:
                    self._deactivate_other_providers(provider)
                
                return new_setting
        except IntegrityError as e:
            self.db.rollback()
            logger.error(f"Integrity error creating/updating API key: {str(e)}")
            raise ValueError(f"Failed to create/update API key: {str(e)}") from e
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error creating/updating API key: {str(e)}", exc_info=True)
            raise ValueError(f"Failed to create/update API key: {str(e)}") from e
    
    def get_active_api_key(self, provider: ApiKeyProvider) -> Optional[str]:
        """
        Get decrypted API key for the active provider.
        
        Args:
            provider: Provider type to get key for
            
        Returns:
            Decrypted API key or None if not found or not active
        """
        setting = self.get_api_key_setting(provider)
        if not setting or not setting.is_active:
            return None
        
        try:
            return decrypt_api_key(setting.encrypted_api_key)
        except Exception as e:
            logger.error(f"Failed to decrypt API key for {provider.value}: {str(e)}", exc_info=True)
            return None
    
    def get_any_api_key(self, provider: ApiKeyProvider) -> Optional[str]:
        """
        Get decrypted API key for a provider regardless of active status.
        Used for fallback scenarios.
        
        Args:
            provider: Provider type to get key for
            
        Returns:
            Decrypted API key or None if not found
        """
        setting = self.get_api_key_setting(provider)
        if not setting:
            return None
        
        try:
            return decrypt_api_key(setting.encrypted_api_key)
        except Exception as e:
            logger.error(f"Failed to decrypt API key for {provider.value}: {str(e)}", exc_info=True)
            return None
    
    def get_masked_api_key(self, provider: ApiKeyProvider) -> Optional[str]:
        """
        Get masked API key for display purposes (first 7 chars + asterisks).
        Never returns the full key.
        
        Args:
            provider: Provider type to get masked key for
            
        Returns:
            Masked API key or None if not found
        """
        setting = self.get_api_key_setting(provider)
        if not setting:
            return None
        
        try:
            decrypted_key = decrypt_api_key(setting.encrypted_api_key)
            return self.mask_api_key(decrypted_key)
        except Exception as e:
            logger.error(f"Failed to get masked API key for {provider.value}: {str(e)}", exc_info=True)
            return None
    
    def set_active_provider(self, provider: ApiKeyProvider) -> ApiKeySettings:
        """
        Set a provider as active and deactivate others.
        
        Args:
            provider: Provider type to activate
            
        Returns:
            Updated ApiKeySettings model
            
        Raises:
            ValueError: If provider not found
        """
        setting = self.get_api_key_setting(provider)
        if not setting:
            raise ValueError(f"API key not found for provider: {provider.value}")
        
        # Deactivate all other providers
        self._deactivate_other_providers(provider)
        
        # Activate this provider
        setting.is_active = True
        self.db.commit()
        self.db.refresh(setting)
        logger.info(f"Activated provider: {provider.value}")
        return setting
    
    def delete_api_key(self, provider: ApiKeyProvider) -> bool:
        """
        Delete API key for a provider.
        
        Args:
            provider: Provider type to delete
            
        Returns:
            True if deleted, False if not found
        """
        setting = self.get_api_key_setting(provider)
        if not setting:
            return False
        
        self.db.delete(setting)
        self.db.commit()
        logger.info(f"Deleted API key for provider: {provider.value}")
        return True
    
    def _deactivate_other_providers(self, active_provider: ApiKeyProvider) -> None:
        """Deactivate all providers except the specified one."""
        self.db.query(ApiKeySettings).filter(
            ApiKeySettings.provider_type != active_provider
        ).update({"is_active": False})
        self.db.commit()

