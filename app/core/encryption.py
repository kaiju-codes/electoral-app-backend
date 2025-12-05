"""Encryption utilities for API keys."""

import os
import logging
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend
import base64

logger = logging.getLogger(__name__)


def _get_encryption_key() -> bytes:
    """
    Get or generate the encryption key from environment variable.
    
    Uses API_KEY_ENCRYPTION_SECRET as a password to derive a Fernet key.
    If not set, generates a key (for development only - should be set in production).
    """
    secret = os.getenv("API_KEY_ENCRYPTION_SECRET")
    
    if not secret:
        logger.warning(
            "API_KEY_ENCRYPTION_SECRET not set. Using default key for development. "
            "Set this in production!"
        )
        # For development: use a default key (DO NOT USE IN PRODUCTION)
        secret = "default-dev-secret-change-in-production"
    
    # Derive a Fernet key from the secret using PBKDF2
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=b"electoral_data_extract_salt",  # Fixed salt for consistency
        iterations=100000,
        backend=default_backend()
    )
    key = base64.urlsafe_b64encode(kdf.derive(secret.encode()))
    return key


def encrypt_api_key(api_key: str) -> str:
    """
    Encrypt an API key using Fernet.
    
    Args:
        api_key: The plain text API key to encrypt
        
    Returns:
        Encrypted API key as a base64-encoded string
    """
    try:
        key = _get_encryption_key()
        fernet = Fernet(key)
        encrypted = fernet.encrypt(api_key.encode())
        return encrypted.decode()
    except Exception as e:
        logger.error(f"Failed to encrypt API key: {str(e)}", exc_info=True)
        raise RuntimeError(f"Failed to encrypt API key: {str(e)}") from e


def decrypt_api_key(encrypted_api_key: str) -> str:
    """
    Decrypt an encrypted API key.
    
    Args:
        encrypted_api_key: The encrypted API key (base64-encoded string)
        
    Returns:
        Decrypted plain text API key
    """
    try:
        key = _get_encryption_key()
        fernet = Fernet(key)
        decrypted = fernet.decrypt(encrypted_api_key.encode())
        return decrypted.decode()
    except Exception as e:
        logger.error(f"Failed to decrypt API key: {str(e)}", exc_info=True)
        raise RuntimeError(f"Failed to decrypt API key: {str(e)}") from e

