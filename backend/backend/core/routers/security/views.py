import logging

from django.conf import settings
from django.http import JsonResponse
from cryptography.hazmat.primitives import serialization
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator

from backend.utils.rsa_encryption import get_rsa_public_key

logger = logging.getLogger(__name__)


@csrf_exempt
def get_public_key(request):
    """Serve the RSA public key for frontend encryption."""
    try:
        # Get public key
        public_key = get_rsa_public_key()
        if not public_key:
            # Debug: log the raw setting value to help diagnose
            raw = settings.VISITRAN_RSA_PUBLIC_KEY
            logger.error(
                "RSA public key unavailable. Setting present=%s, length=%s, preview=%s",
                raw is not None,
                len(raw) if raw else 0,
                repr(raw[:60]) if raw else "None",
            )
            return JsonResponse(
                {"status": "error", "message": "RSA public key not available"},
                status=503
            )

        # Return public key in PEM format
        response_data = {
            "status": "success",
            "data": {
                "public_key": public_key.public_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PublicFormat.SubjectPublicKeyInfo
                ).decode('utf-8'),
                "key_size": 2048,
                "algorithm": "RSA"
            }
        }

        return JsonResponse(data=response_data, status=200)

    except Exception as e:
        return JsonResponse(
            {"status": "error", "message": f"Error serving public key: {str(e)}"},
            status=500
        )
