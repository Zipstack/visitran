import logging
from datetime import timedelta

from django.conf import settings as django_settings
from django.utils.timezone import now
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response

from backend.core.models.api_tokens import APIToken
from backend.core.services.api_key_audit import log_api_key_event
from backend.core.services.api_key_service import generate_api_key, generate_signature
from backend.core.utils import handle_http_request
from backend.utils.constants import HTTPMethods

logger = logging.getLogger(__name__)

API_KEY_NOT_FOUND = "API key not found."


def _serialize_token(token, include_secret=False):
    """Serialize an APIToken to a dict."""
    data = {
        "id": str(token.id),
        "label": token.label,
        "api_key": token.masked_token,
        "signature": token.masked_signature,
        "email": token.user.email,
        "status": token.status,
        "created_at": token.created_at.isoformat() if token.created_at else None,
        "expires_at": token.expires_at.isoformat() if token.expires_at else None,
        "last_used_at": token.last_used_at.isoformat() if token.last_used_at else None,
        "is_disabled": token.is_disabled,
    }
    if include_secret:
        data["api_key"] = token.token
        data["signature"] = token.signature
    return data


@api_view([HTTPMethods.GET])
@handle_http_request
def list_api_keys(request: Request) -> Response:
    """List all API keys for the current user."""
    tokens = APIToken.objects.filter(user=request.user).order_by("-created_at")
    return Response({
        "keys": [_serialize_token(t) for t in tokens],
        "max_keys": django_settings.MAX_KEYS_PER_USER,
    }, status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST])
@handle_http_request
def create_api_key(request: Request) -> Response:
    """Generate a new API key + signature."""
    label = request.data.get("label", "").strip()
    if not label:
        return Response(
            {"message": "Label is required."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    # D1: Rate limit — max keys per user (configurable via .env)
    max_keys = django_settings.MAX_KEYS_PER_USER
    existing_count = APIToken.objects.filter(user=request.user).count()
    if existing_count >= max_keys:
        return Response(
            {"message": f"Maximum {max_keys} API keys allowed per user."},
            status=status.HTTP_429_TOO_MANY_REQUESTS,
        )

    api_key = generate_api_key()
    sig = generate_signature(api_key)

    token = APIToken.objects.create(
        user=request.user,
        token=api_key,
        signature=sig,
        label=label,
        expires_at=now() + timedelta(days=django_settings.API_KEY_EXPIRY_DAYS),
    )

    logger.info(f"API key created: id={token.id}, label={label}, user={request.user.email}")
    log_api_key_event(
        request, action="create", key_id=token.id,
        key_label=label, key_masked=token.masked_token,
    )

    return Response(
        _serialize_token(token, include_secret=True),
        status=status.HTTP_201_CREATED,
    )


@api_view([HTTPMethods.GET])
@handle_http_request
def get_api_key(request: Request, key_id: str) -> Response:
    """Get a single API key's details (masked)."""
    try:
        token = APIToken.objects.get(id=key_id, user=request.user)
    except APIToken.DoesNotExist:
        return Response({"message": API_KEY_NOT_FOUND}, status=status.HTTP_404_NOT_FOUND)
    return Response(_serialize_token(token, include_secret=True), status=status.HTTP_200_OK)


@api_view([HTTPMethods.DELETE])
@handle_http_request
def delete_api_key(request: Request, key_id: str) -> Response:
    """Permanently delete an API key."""
    try:
        token = APIToken.objects.get(id=key_id, user=request.user)
    except APIToken.DoesNotExist:
        return Response({"message": API_KEY_NOT_FOUND}, status=status.HTTP_404_NOT_FOUND)

    key_id = str(token.id)
    key_label = token.label
    key_masked = token.masked_token
    logger.info(f"API key deleted: id={key_id}, label={key_label}, user={request.user.email}")
    token.delete()
    log_api_key_event(
        request, action="delete", key_id=key_id,
        key_label=key_label, key_masked=key_masked,
    )
    return Response({"message": "API key deleted."}, status=status.HTTP_200_OK)


@api_view(["PATCH"])
@handle_http_request
def toggle_api_key(request: Request, key_id: str) -> Response:
    """Toggle enable/disable for an API key."""
    try:
        token = APIToken.objects.get(id=key_id, user=request.user)
    except APIToken.DoesNotExist:
        return Response({"message": API_KEY_NOT_FOUND}, status=status.HTTP_404_NOT_FOUND)

    token.is_disabled = not token.is_disabled
    token.save(update_fields=["is_disabled"])

    toggle_action = "disabled" if token.is_disabled else "enabled"
    logger.info(f"API key {toggle_action}: id={token.id}, label={token.label}, user={request.user.email}")
    log_api_key_event(
        request, action="toggle", key_id=token.id,
        key_label=token.label, key_masked=token.masked_token,
        details={"new_status": toggle_action},
    )

    return Response(_serialize_token(token), status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST])
@handle_http_request
def regenerate_api_key(request: Request, key_id: str) -> Response:
    """Regenerate key + signature for an existing API key (keeps label)."""
    try:
        token = APIToken.objects.get(id=key_id, user=request.user)
    except APIToken.DoesNotExist:
        return Response({"message": API_KEY_NOT_FOUND}, status=status.HTTP_404_NOT_FOUND)

    new_key = generate_api_key()
    new_sig = generate_signature(new_key)

    token.token = new_key
    token.signature = new_sig
    token.is_disabled = False
    token.expires_at = now() + timedelta(days=django_settings.API_KEY_EXPIRY_DAYS)
    token.save(update_fields=["token", "signature", "is_disabled", "expires_at"])

    logger.info(f"API key regenerated: id={token.id}, label={token.label}, user={request.user.email}")
    log_api_key_event(
        request, action="regenerate", key_id=token.id,
        key_label=token.label, key_masked=token.masked_token,
    )

    return Response(
        _serialize_token(token, include_secret=True),
        status=status.HTTP_200_OK,
    )


# Keep legacy endpoint for backward compatibility
@api_view([HTTPMethods.POST])
@handle_http_request
def generate_token(request: Request) -> Response:
    """Legacy token generation endpoint.

    Now creates a proper APIToken record with vtk_ prefix, label, and expiry
    to maintain consistency with the api-keys/create endpoint.
    """
    # Delete any existing default token for this user
    APIToken.objects.filter(user=request.user, label="Default").delete()

    api_key = generate_api_key()
    sig = generate_signature(api_key)

    APIToken.objects.create(
        user=request.user,
        token=api_key,
        signature=sig,
        label="Default",
        expires_at=now() + timedelta(days=django_settings.API_KEY_EXPIRY_DAYS),
    )

    return Response({
        "message": "Token generated successfully.",
        "token": api_key,
    }, status=status.HTTP_200_OK)
