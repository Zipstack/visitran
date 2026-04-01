from typing import Dict, Any, Tuple, Optional
import base64
import logging
import os

from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.backends import default_backend
from django.conf import settings

logger = logging.getLogger(__name__)

# RSA key size for encryption
RSA_KEY_SIZE = 2048

# Maximum data size that can be encrypted with RSA (depends on key size)
MAX_RSA_ENCRYPT_SIZE = 190  # For 2048-bit key


def _load_pem_from_dotenv(key_name: str) -> Optional[str]:
    """Fallback: read a PEM key directly from the .env file using dotenv_values."""
    try:
        from dotenv import find_dotenv, dotenv_values
        env_file = find_dotenv()
        if not env_file:
            return None
        values = dotenv_values(env_file)
        return values.get(key_name) or None
    except Exception:
        return None


def _normalize_pem(pem: str) -> str:
    """Handle literal \\n from Docker env_file or other env-passing mechanisms."""
    if "\\n" in pem and "\n" not in pem:
        pem = pem.replace("\\n", "\n")
    return pem


def _is_valid_pem(pem: str) -> bool:
    """Quick check that a PEM string has both BEGIN and END markers."""
    return "-----BEGIN" in pem and "-----END" in pem


def _resolve_pem(setting_name: str) -> Optional[str]:
    """Resolve a PEM key from settings, os.environ, or .env file (in that order)."""
    # 1. Try Django settings
    pem = getattr(settings, setting_name, None)
    if pem:
        pem = _normalize_pem(pem)
        if _is_valid_pem(pem):
            return pem
        logger.warning(f"{setting_name} from settings is malformed (len={len(pem)}), trying fallbacks")

    # 2. Try os.environ directly (may differ from settings if loaded later)
    pem = os.environ.get(setting_name)
    if pem:
        pem = _normalize_pem(pem)
        if _is_valid_pem(pem):
            return pem
        logger.warning(f"{setting_name} from os.environ is malformed (len={len(pem)}), trying .env file")

    # 3. Fallback: read .env file directly (handles multiline PEM parsing issues)
    pem = _load_pem_from_dotenv(setting_name)
    if pem:
        pem = _normalize_pem(pem)
        if _is_valid_pem(pem):
            logger.info(f"Loaded {setting_name} from .env file fallback")
            return pem

    return None


def get_rsa_private_key() -> Optional[rsa.RSAPrivateKey]:
    """Get RSA private key from environment variable."""
    try:
        private_key_pem = _resolve_pem("VISITRAN_RSA_PRIVATE_KEY")
        if not private_key_pem:
            logger.error("RSA private key not found in settings, env, or .env file")
            return None

        private_key = serialization.load_pem_private_key(
            private_key_pem.encode('utf-8'),
            password=None,
            backend=default_backend()
        )
        return private_key
    except Exception as e:
        logger.error(f"Error loading RSA private key: {e}", exc_info=True)
        return None


def get_rsa_public_key() -> Optional[rsa.RSAPublicKey]:
    """Get RSA public key from environment variable."""
    try:
        public_key_pem = _resolve_pem("VISITRAN_RSA_PUBLIC_KEY")
        if not public_key_pem:
            logger.error("RSA public key not found in settings, env, or .env file")
            return None

        public_key = serialization.load_pem_public_key(
            public_key_pem.encode('utf-8'),
            backend=default_backend()
        )
        return public_key
    except Exception as e:
        logger.error(f"Error loading RSA public key: {e}", exc_info=True)
        return None


def generate_rsa_key_pair() -> Tuple[str, str]:
    """Generate a new RSA key pair and return as PEM strings."""
    try:
        # Generate private key
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=RSA_KEY_SIZE,
            backend=default_backend()
        )

        # Get public key
        public_key = private_key.public_key()

        # Convert to PEM format
        private_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ).decode('utf-8')

        public_pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode('utf-8')

        logger.info("RSA key pair generated successfully")
        return private_pem, public_pem

    except Exception as e:
        logger.error(f"Error generating RSA key pair: {e}")
        raise


def encrypt_with_public_key(data: str) -> Optional[str]:
    """Encrypt data using RSA public key."""
    try:
        public_key = get_rsa_public_key()
        if not public_key:
            logger.error("Cannot encrypt: RSA public key not available")
            return None

        # Convert string to bytes
        data_bytes = data.encode('utf-8')

        # Check data size
        if len(data_bytes) > MAX_RSA_ENCRYPT_SIZE:
            logger.error(f"Data too large for RSA encryption: {len(data_bytes)} bytes")
            return None

        # Encrypt data
        encrypted_bytes = public_key.encrypt(
            data_bytes,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )

        # Convert to base64 for safe transmission
        encrypted_b64 = base64.b64encode(encrypted_bytes).decode('utf-8')
        logger.debug(f"Data encrypted successfully: {len(data_bytes)} bytes")
        return encrypted_b64

    except Exception as e:
        logger.error(f"Error encrypting data with RSA public key: {e}")
        return None


def decrypt_with_private_key(encrypted_data: str) -> Optional[str]:
    """Decrypt data using RSA private key."""
    try:
        private_key = get_rsa_private_key()
        if not private_key:
            logger.error("Cannot decrypt: RSA private key not available")
            return None

        # Validate input
        if not isinstance(encrypted_data, str):
            logger.error(f"Invalid input type: {type(encrypted_data)}, expected str")
            return None

        if not encrypted_data.strip():
            logger.error("Empty encrypted data")
            return None

        # Check if it looks like base64 data
        try:
            # Convert from base64
            encrypted_bytes = base64.b64decode(encrypted_data.encode('utf-8'))
            logger.debug(f"Successfully decoded base64, length: {len(encrypted_bytes)} bytes")
        except Exception as e:
            logger.error(f"Failed to decode base64: {e}")
            logger.debug(f"Encrypted data preview: {encrypted_data[:100]}...")
            return None

        # Check if the data size is reasonable for RSA
        if len(encrypted_bytes) != 256:  # 2048-bit RSA produces 256-byte output
            logger.warning(f"Unexpected encrypted data size: {len(encrypted_bytes)} bytes (expected 256 for RSA-2048)")
            logger.debug(f"This might indicate the data is not properly encrypted")

        # Try different padding schemes
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding

        # Method 1: OAEP with SHA256 (original)
        try:
            logger.debug("Attempting decryption with OAEP SHA256...")
            decrypted_bytes = private_key.decrypt(
                encrypted_bytes,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )
            decrypted_data = decrypted_bytes.decode('utf-8')
            logger.debug(f"Successfully decrypted with OAEP SHA256: {len(decrypted_bytes)} bytes")
            return decrypted_data
        except Exception as e:
            logger.debug(f"OAEP SHA256 decryption failed: {e}")

        # Method 2: OAEP with SHA1
        try:
            logger.debug("Attempting decryption with OAEP SHA1...")
            decrypted_bytes = private_key.decrypt(
                encrypted_bytes,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA1()),
                    algorithm=hashes.SHA1(),
                    label=None
                )
            )
            decrypted_data = decrypted_bytes.decode('utf-8')
            logger.debug(f"Successfully decrypted with OAEP SHA1: {len(decrypted_bytes)} bytes")
            return decrypted_data
        except Exception as e:
            logger.debug(f"OAEP SHA1 decryption failed: {e}")

        # Method 3: PKCS1v15
        try:
            logger.debug("Attempting decryption with PKCS1v15...")
            decrypted_bytes = private_key.decrypt(
                encrypted_bytes,
                padding.PKCS1v15()
            )
            decrypted_data = decrypted_bytes.decode('utf-8')
            logger.debug(f"Successfully decrypted with PKCS1v15: {len(decrypted_bytes)} bytes")
            return decrypted_data
        except Exception as e:
            logger.debug(f"PKCS1v15 decryption failed: {e}")

        # If all methods fail, log the error
        logger.error("All decryption methods failed")
        logger.debug(f"Encrypted data length: {len(encrypted_data)}")
        logger.debug(f"Encrypted data preview: {encrypted_data[:100]}...")
        logger.debug(f"Decoded bytes length: {len(encrypted_bytes)}")

        return None

    except Exception as e:
        logger.error(f"Error decrypting data with RSA private key: {e}")
        logger.debug(f"Encrypted data type: {type(encrypted_data)}")
        logger.debug(f"Encrypted data length: {len(str(encrypted_data))}")
        logger.debug(f"Encrypted data preview: {str(encrypted_data)[:100]}...")
        return None


def validate_rsa_keys() -> bool:
    """Validate that RSA keys are properly configured."""
    try:
        private_key = get_rsa_private_key()
        public_key = get_rsa_public_key()

        if not private_key or not public_key:
            logger.error("RSA keys validation failed: keys not available")
            return False

        # Test encryption/decryption
        test_data = "test_encryption"
        encrypted = encrypt_with_public_key(test_data)
        if not encrypted:
            logger.error("RSA keys validation failed: encryption failed")
            return False

        decrypted = decrypt_with_private_key(encrypted)
        if not decrypted or decrypted != test_data:
            logger.error("RSA keys validation failed: decryption failed")
            return False

        logger.info("RSA keys validation successful")
        return True

    except Exception as e:
        logger.error(f"RSA keys validation failed: {e}")
        return False


def validate_encrypted_data(encrypted_data: str) -> dict:
    """
    Validate encrypted data and provide detailed analysis.

    Args:
        encrypted_data: The encrypted data string to validate

    Returns:
        Dictionary with validation results and analysis
    """
    result = {
        "is_valid": False,
        "errors": [],
        "warnings": [],
        "analysis": {}
    }

    try:
        # Check if it's a string
        if not isinstance(encrypted_data, str):
            result["errors"].append(f"Invalid type: {type(encrypted_data)}, expected str")
            return result

        # Check if it's empty
        if not encrypted_data.strip():
            result["errors"].append("Empty encrypted data")
            return result

        # Check length
        result["analysis"]["length"] = len(encrypted_data)
        if len(encrypted_data) < 100:
            result["warnings"].append(f"Data seems too short for RSA encryption: {len(encrypted_data)} chars")

        # Check if it contains only base64 characters
        valid_chars = set('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=')
        invalid_chars = set(encrypted_data) - valid_chars
        if invalid_chars:
            result["errors"].append(f"Contains invalid base64 characters: {invalid_chars}")
            return result

        # Try to decode as base64
        try:
            decoded = base64.b64decode(encrypted_data)
            result["analysis"]["decoded_length"] = len(decoded)
            result["analysis"]["decoded_bytes"] = decoded[:10].hex()  # First 10 bytes as hex

            # Check if it's the right size for RSA-2048
            if len(decoded) == 256:
                result["analysis"]["rsa_size"] = "correct"
            else:
                result["warnings"].append(f"Unexpected size for RSA-2048: {len(decoded)} bytes (expected 256)")
                result["analysis"]["rsa_size"] = "incorrect"

            result["is_valid"] = True

        except Exception as e:
            result["errors"].append(f"Invalid base64: {e}")
            return result

    except Exception as e:
        result["errors"].append(f"Validation error: {e}")

    return result


def get_encryption_debug_info(encrypted_data: str) -> str:
    """
    Get detailed debug information about encrypted data.

    Args:
        encrypted_data: The encrypted data to analyze

    Returns:
        Formatted debug information string
    """
    validation = validate_encrypted_data(encrypted_data)

    debug_info = f"""
🔍 Encrypted Data Analysis
========================
Input Type: {type(encrypted_data)}
Input Length: {len(encrypted_data)}
Input Preview: {encrypted_data[:100]}...

Validation Results:
- Valid: {validation['is_valid']}
- Errors: {validation['errors']}
- Warnings: {validation['warnings']}

Analysis:
"""

    for key, value in validation['analysis'].items():
        debug_info += f"- {key}: {value}\n"

    return debug_info
