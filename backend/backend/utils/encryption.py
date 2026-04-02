from base64 import b64decode
from typing import Dict, Any

from cryptography.fernet import Fernet
from django.conf import settings

# Sensitive fields that need to be encrypted
SENSITIVE_FIELDS = {
    "password",
    "api_key",
    "access_key",
    "access_token",
    "secret_key",
    "token",
    "connection_string",
    "passw",
    "connection_url",
    "credentials",
    "credentials_dict",
}


def get_encryption_key() -> bytes:
    """Get or generate an encryption key from environment variable."""
    key = settings.VISITRAN_ENCRYPTION_KEY
    return b64decode(key.encode("utf-8"))


def get_fernet() -> Fernet:
    """Get Fernet instance with an encryption key."""
    return Fernet(get_encryption_key())


def encrypt_value(value: str) -> str:
    """Encrypt a single string value."""
    if not value:
        return value
    return get_fernet().encrypt(value.encode("utf-8")).decode("utf-8")


def decrypt_value(encrypted_value: str) -> str:
    """Decrypt a single encrypted string value."""
    if not encrypted_value:
        return encrypted_value
    try:
        return get_fernet().decrypt(encrypted_value.encode("utf-8")).decode("utf-8")
    except Exception:
        return encrypted_value  # Return original if not encrypted


def encrypt_connection_details(details: dict[str, Any]) -> dict[str, Any]:
    """Encrypt sensitive fields in connection details."""
    if not details:
        return details

    encrypted = details.copy()
    for key, value in details.items():
        if key.lower() in SENSITIVE_FIELDS:
            if isinstance(value, str):
                encrypted[key] = encrypt_value(value)
            elif isinstance(value, dict):
                encrypted[key] = encrypt_value(str(value))
    return encrypted


def decrypt_connection_details(details: dict[str, Any]) -> dict[str, Any]:
    """Decrypt sensitive fields in connection details."""
    if not details:
        return details

    decrypted = details.copy()
    for key, value in details.items():
        if key.lower() in SENSITIVE_FIELDS and isinstance(value, str):
            decrypted_string = decrypt_value(value)
            try:
                import ast
                decrypted_string = ast.literal_eval(decrypted_string)
                decrypted[key] = decrypted_string
            except (ValueError, SyntaxError):
                decrypted[key] = decrypted_string
    return decrypted


def mask_value(key: str, value: str) -> str:
    """Mask a single value based on its key and content."""
    if not value:
        return value

    if key.lower() == "connection_url":
        # For connection URL, mask half of the characters while preserving length
        length = len(value)
        mask_length = length // 2
        start_pos = (length - mask_length) // 2  # Start masking from middle

        # Create list of characters for easier manipulation
        chars = list(value)

        # Replace middle portion with * for each character type
        for i in range(start_pos, start_pos + mask_length):
            if chars[i].isalpha():
                chars[i] = "*"
            elif chars[i].isdigit():
                chars[i] = "*"
            # Keep special characters as is

        return "".join(chars)
    else:
        return "********"


def mask_connection_details(details: dict[str, Any]) -> dict[str, Any]:
    """Mask sensitive fields in connection details for API responses."""
    if not details:
        return details

    masked = details.copy()
    for key in details:
        if key.lower() in SENSITIVE_FIELDS and details[key]:
            masked[key] = mask_value(key, str(details[key]))
    return masked
