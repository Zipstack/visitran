import hashlib
import hmac
import secrets

from django.conf import settings

# New simplified token prefix (no HMAC signature needed)
VTK_PREFIX = "vtk_"

# Legacy prefixes (kept for backward compatibility)
API_KEY_PREFIX = "pk_live_"
SIGNATURE_PREFIX = "sig_"


def generate_api_key() -> str:
    """Generate a new API key with vtk_ prefix (simplified token)."""
    return VTK_PREFIX + secrets.token_urlsafe(32)


def generate_signature(api_key: str, signing_secret: str = "") -> str:
    """Generate HMAC-SHA256 signature for an API key.

    Note: vtk_ tokens don't require signatures (validated directly by cloud API).
    This function is kept for backward compatibility with pk_live_ tokens.

    Args:
        api_key: The API key to sign.
        signing_secret: The server signing secret. If empty, reads from settings.
    """
    # vtk_ tokens don't need signatures
    if api_key.startswith(VTK_PREFIX):
        return ""

    secret = signing_secret or getattr(settings, "SERVER_SIGNING_SECRET", "")
    if not secret:
        raise ValueError("SERVER_SIGNING_SECRET is not configured")
    digest = hmac.new(secret.encode(), api_key.encode(), hashlib.sha256).hexdigest()
    return SIGNATURE_PREFIX + digest


def validate_api_key(api_key: str, signature: str, signing_secret: str = "") -> bool:
    """Stateless HMAC validation — recompute signature and compare.

    Args:
        api_key: The API key (vtk_...).
        signature: The signature to validate (sig_...).
        signing_secret: The server signing secret. If empty, reads from settings.

    Returns:
        True if signature is valid, False otherwise.
    """
    if not api_key or not signature:
        return False

    secret = signing_secret or getattr(settings, "SERVER_SIGNING_SECRET", "")
    if not secret:
        return False

    expected = SIGNATURE_PREFIX + hmac.new(secret.encode(), api_key.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)
