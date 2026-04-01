"""Authentication middleware for OSS deployment.

Handles authentication and tenant context setup for OSS mode.
"""

import logging
import re

from django.conf import settings
from django.http import HttpRequest, HttpResponse, JsonResponse

from backend.account.authentication_service import AuthenticationService
from backend.utils.tenant_context import _get_tenant_context, clear_tenant_context

Logger = logging.getLogger(__name__)


class OSSAuthMiddleware:
    """OSS authentication and tenant context middleware.

    Handles:
    - Session-based authentication via AuthenticationService
    - Tenant context setup from URL
    - User context setup
    - Context cleanup
    """

    def __init__(self, get_response: HttpResponse):
        self.get_response = get_response

    def __call__(self, request: HttpRequest) -> HttpResponse:
        try:
            # Skip auth for whitelisted paths (login, logout, health, static, etc.)
            whitelisted = getattr(settings, "WHITELISTED_PATHS", [])
            if any(request.path.startswith(path) for path in whitelisted):
                return self.get_response(request)

            # Strip tenant prefix from URL (e.g. /api/v1/visitran/{orgId}/...)
            tenant_prefix_pattern = r"^/api/v1/visitran/(?P<tenant_id>[^/]+)(?P<path>/.*)?$"
            match = re.match(tenant_prefix_pattern, request.path)

            # Set tenant context
            context = _get_tenant_context()
            tenant_id = ""
            if match:
                tenant_id = match.group("tenant_id")
                new_path = match.group("path") or "/"
                request.path_info = new_path
                request.tenant_id = tenant_id
                context.set_tenant(tenant_id, source="dev")
                Logger.debug(f"Set tenant context: {tenant_id}")

            # Check if user is authenticated using AuthenticationService
            auth_service = AuthenticationService()
            is_authenticated = auth_service.is_authenticated(request)

            # Set user role from OrganizationMember if tenant is specified
            if is_authenticated and tenant_id:
                try:
                    from backend.core.models.organization_member import OrganizationMember

                    org_member = OrganizationMember.objects.filter(
                        user=request.user, organization__organization_id=tenant_id
                    ).first()
                    if org_member:
                        request.user.role = org_member.role
                        Logger.debug(f"Set user role: {org_member.role}")
                except Exception as e:
                    Logger.debug(f"Could not get org member role: {e}")

            # Enforce authentication for API endpoints
            if request.path.startswith(f"/{settings.PATH_PREFIX}"):
                if not is_authenticated:
                    return JsonResponse({"message": "Unauthorized"}, status=401)

            # Set user in context for authenticated requests
            if is_authenticated:
                context.set_user(request.user)

            response = self.get_response(request)
            return response

        finally:
            # Always cleanup tenant context to prevent leakage
            _get_tenant_context().clear()
            clear_tenant_context()
