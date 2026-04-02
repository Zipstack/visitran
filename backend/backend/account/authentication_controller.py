"""Authentication controller that delegates to plugin or OSS service.

Uses the same interface as ScalekitService for compatibility. If an
authentication plugin is available (cloud), it uses the plugin.
Otherwise, it falls back to the default AuthenticationService (OSS).
"""

import logging
from typing import Any, Optional

from django.conf import settings
from django.contrib.auth import logout as django_logout
from django.http import HttpRequest, HttpResponse
from rest_framework import status
from rest_framework.request import Request
from rest_framework.response import Response

from backend.account.authentication_plugin_registry import (
    AuthenticationPluginRegistry,
)
from backend.account.authentication_service import AuthenticationService
from backend.utils.tenant_context import get_current_tenant

Logger = logging.getLogger(__name__)


class AuthenticationController:
    """Authentication controller that delegates to plugin or default service.

    Uses scalekit-compatible interface naming for all methods.
    """

    def __init__(self) -> None:
        if AuthenticationPluginRegistry.is_plugin_available():
            self.auth_service = AuthenticationPluginRegistry.get_plugin()
        else:
            self.auth_service = AuthenticationService()

    # =========================================================================
    # Core Authentication
    # =========================================================================

    def is_authenticated(self, request: HttpRequest) -> bool:
        """Check if the current request is authenticated."""
        return self.auth_service.is_authenticated(request)

    def user_login(self, request: HttpRequest) -> HttpResponse:
        """Handle user login."""
        return self.auth_service.user_login(request)

    def user_logout(self, request: HttpRequest) -> HttpResponse:
        """Handle user logout."""
        response = self.auth_service.user_logout(request=request)
        django_logout(request)
        return response

    def user_signup(self, request: Request) -> Response:
        """Handle user signup.

        For OSS: Creates user with email/password, auto-creates personal org.
        For Cloud: Delegates to plugin (typically redirects to SSO).
        """
        if hasattr(self.auth_service, "user_signup"):
            return self.auth_service.user_signup(request)

        # Cloud plugins typically don't support direct signup (use SSO instead)
        return Response(
            status=status.HTTP_400_BAD_REQUEST,
            data={"error": "Signup not available. Please use SSO login."},
        )

    # =========================================================================
    # Authorization Callback (SSO)
    # =========================================================================

    def handle_authorization_callback(
        self, request: HttpRequest, backend: str = ""
    ) -> HttpResponse:
        """Handle SSO authorization callback."""
        if hasattr(self.auth_service, "handle_authorization_callback"):
            return self.auth_service.handle_authorization_callback(request, backend)
        return Response(
            status=status.HTTP_400_BAD_REQUEST,
            data={"error": "SSO callback not supported."},
        )

    # =========================================================================
    # Organization Methods
    # =========================================================================

    def user_organizations(self, request: HttpRequest) -> Response:
        """Get list of organizations the current user belongs to."""
        organizations = self.auth_service.user_organizations(request)
        # Cloud plugin returns Pydantic Membership models, OSS returns
        # plain dicts. Normalize to dicts for consistent DRF serialization.
        org_list = [
            org.model_dump() if hasattr(org, "model_dump") else org
            for org in organizations
        ]
        return Response(
            status=status.HTTP_200_OK,
            data={
                "message": "success",
                "organizations": org_list,
            },
        )

    def switch_organization(
        self, request: HttpRequest, user_id: str, organization_id: str
    ) -> HttpResponse:
        """Switch user's current organization."""
        return self.auth_service.switch_organization(request, user_id, organization_id)

    def create_organization(self, request: Request) -> Response:
        """Create a new organization."""
        return self.auth_service.create_organization(request)

    # =========================================================================
    # Session & User Info
    # =========================================================================

    def get_session(self, request: Request) -> Response:
        """Get current session information."""
        session_info = self.auth_service.get_session_info(request)
        if session_info is None:
            return Response(
                status=status.HTTP_401_UNAUTHORIZED,
                data={"error": "Not authenticated"},
            )
        return Response(
            status=status.HTTP_200_OK,
            data=session_info,
        )

    def get_user_info(self, request: HttpRequest) -> Optional[dict]:
        """Get current user information."""
        return self.auth_service.get_user_info(request)

    def landing(self, request: Request) -> Response:
        """Landing page endpoint."""
        return self.auth_service.landing(request)

    # =========================================================================
    # Role Management
    # =========================================================================

    def get_roles(self) -> list:
        """Get available roles."""
        return self.auth_service.get_roles()

    def add_organization_user_role(
        self, organization_id: str, user: Any, user_role_name: str
    ) -> Optional[list]:
        """Add role to user."""
        return self.auth_service.add_organization_user_role(
            organization_id, user, user_role_name
        )

    def assign_role_to_org_user(
        self, organization_id: str, user: Any, user_role_name: str = "admin"
    ) -> list:
        """Assign role to organization user."""
        return self.auth_service.assign_role_to_org_user(
            organization_id, user, user_role_name
        )

    def get_organization_role_of_user(
        self, user_id: str, organization_id: str
    ) -> list:
        """Get user's role in organization."""
        return self.auth_service.get_organization_role_of_user(user_id, organization_id)

    # =========================================================================
    # User Management
    # =========================================================================

    def invite_user(
        self, admin: Any, org_id: str, user_list: list = None, email: str = None, role: str = "admin"
    ) -> list:
        """Invite user(s) to organization.

        Accepts either a user_list (from the view) or a single
        email+role. Returns a list of {email, status, message} dicts for
        failed invites.
        """
        if user_list is None and email:
            user_list = [{"email": email, "role": role}]
        if not user_list:
            return []

        failed_invites = []
        for user in user_list:
            user_email = user.get("email")
            user_role = user.get("role", "admin")
            try:
                self.auth_service.invite_user(admin, org_id, user_email, user_role)
            except Exception as e:
                logging.exception(f"Failed to invite {user_email}: {e}")
                failed_invites.append({
                    "email": user_email,
                    "status": "failed",
                    "message": str(e),
                })
        return failed_invites

    def remove_users_from_organization(
        self, admin: Any, organization_id: str, user_emails: list
    ) -> list:
        """Remove users from organization by email.

        Looks up users by email, deletes their OrganizationMember
        records, and delegates to auth service for any cloud-specific
        cleanup.

        Returns a list of failed removals.
        """
        from django.contrib.auth import get_user_model

        from backend.core.models.organization_member import OrganizationMember

        User = get_user_model()
        failed_removals = []

        for email in user_emails:
            try:
                user = User.objects.get(email=email)
            except User.DoesNotExist:
                Logger.error(f"User with email {email} not found")
                failed_removals.append({
                    "email": email,
                    "status": "failed",
                    "message": "User not found",
                })
                continue

            deleted_count, _ = OrganizationMember.objects.filter(
                user=user,
                organization__organization_id=organization_id,
            ).delete()

            if not deleted_count:
                Logger.error(
                    f"No membership found for {email} in org {organization_id}"
                )
                failed_removals.append({
                    "email": email,
                    "status": "failed",
                    "message": "No membership found",
                })

        return failed_removals

    def get_organizations_users(self, org_id: str) -> list:
        """Get organization members."""
        return self.auth_service.get_organizations_users(org_id)

    def get_invitations(self, organization_id: str) -> list:
        """Get pending invitations."""
        return self.auth_service.get_invitations(organization_id)

    def delete_invitation(self, organization_id: str, invitation_id: str) -> bool:
        """Delete invitation."""
        return self.auth_service.delete_invitation(organization_id, invitation_id)

    # =========================================================================
    # Additional Methods
    # =========================================================================

    def get_organization_by_org_id(self, org_id: str) -> Any:
        """Get organization by ID."""
        return self.auth_service.get_organization_by_org_id(org_id)

    def is_user_member_of_organization(
        self, user_id: str, organization_id: str
    ) -> bool:
        """Check if user is member of organization."""
        return self.auth_service.is_user_member_of_organization(user_id, organization_id)

    def get_organizations_by_user_id(self, user_id: str) -> list:
        """Get organizations for a user by user_id."""
        return self.auth_service.get_organizations_by_user_id(user_id)

    def create_roles(self, role: Any) -> Any:
        """Create role."""
        return self.auth_service.create_roles(role)

    def delete_role(self, role_id: str) -> bool:
        """Delete role."""
        return self.auth_service.delete_role(role_id)

    def forgot_password(self, request):
        """Handle forgot password request.

        OSS-only: returns 404 in cloud mode to prevent endpoint discovery.
        """
        if settings.IS_CLOUD:
            return Response(status=status.HTTP_404_NOT_FOUND)
        if hasattr(self.auth_service, "forgot_password"):
            return self.auth_service.forgot_password(request)
        return Response(
            status=status.HTTP_400_BAD_REQUEST,
            data={"error": "Password reset not available."},
        )

    def reset_password(self, request):
        """Handle password reset.

        OSS-only: returns 404 in cloud mode to prevent endpoint discovery.
        """
        if settings.IS_CLOUD:
            return Response(status=status.HTTP_404_NOT_FOUND)
        if hasattr(self.auth_service, "reset_password"):
            return self.auth_service.reset_password(request)
        return Response(
            status=status.HTTP_400_BAD_REQUEST,
            data={"error": "Password reset not available."},
        )

    def validate_reset_token(self, request):
        """Validate a password reset token without consuming it.

        OSS-only: returns 404 in cloud mode.
        """
        if settings.IS_CLOUD:
            return Response(status=status.HTTP_404_NOT_FOUND)
        if hasattr(self.auth_service, "validate_reset_token"):
            return self.auth_service.validate_reset_token(request)
        return Response(
            status=status.HTTP_400_BAD_REQUEST,
            data={"error": "Token validation not available."},
        )

    def reset_user_password(self, user: Any) -> Response:
        """Reset user password."""
        return self.auth_service.reset_user_password(user)

    # =========================================================================
    # Cloud-compatible Methods (for multi-tenant operations)
    # =========================================================================

    def authorization_callback(
        self, request: HttpRequest, backend: str = ""
    ) -> HttpResponse:
        """Alias for handle_authorization_callback (cloud naming)."""
        return self.handle_authorization_callback(request, backend)

    def set_user_organization(
        self, request: HttpRequest, organization_id: str
    ) -> HttpResponse:
        """Alias for switch_organization (cloud naming)."""
        user = request.user
        user_id = getattr(user, "user_id", str(user.id)) if user.is_authenticated else ""
        return self.switch_organization(request, user_id, organization_id)

    def is_admin_by_role(self, role: str) -> bool:
        """Check if the role is an admin role."""
        if hasattr(self.auth_service, "is_admin_by_role"):
            return self.auth_service.is_admin_by_role(role)
        # Default: admin role names
        return role.lower() in ["admin", "super_admin", "visitran_admin"]

    def get_organization_info(self, org_id: str) -> Optional[Any]:
        """Get organization info by ID."""
        if hasattr(self.auth_service, "get_organization_info"):
            return self.auth_service.get_organization_info(org_id)
        return self.get_organization_by_org_id(org_id)

    def get_organization_members_by_user(self, user: Any) -> Optional[Any]:
        """Get organization membership for a user."""
        if hasattr(self.auth_service, "get_organization_members_by_user"):
            return self.auth_service.get_organization_members_by_user(user)
        # OSS: Get from OrganizationMember model
        from backend.core.models.organization_member import OrganizationMember
        return OrganizationMember.objects.filter(user=user).first()

    def get_user_roles(self) -> list:
        """Alias for get_roles (cloud naming)."""
        return self.get_roles()

    def create_user_roles(self, data: Any) -> Any:
        """Alias for create_roles (cloud naming)."""
        return self.create_roles(data)

    def delete_user_role(self, role_id: str) -> bool:
        """Alias for delete_role (cloud naming)."""
        return self.delete_role(role_id)

    def get_user_invitations(self, organization_id: str) -> list:
        """Alias for get_invitations (cloud naming)."""
        return self.get_invitations(organization_id)

    def delete_user_invitation(
        self, organization_id: str, invitation_id: str
    ) -> bool:
        """Alias for delete_invitation (cloud naming)."""
        return self.delete_invitation(organization_id, invitation_id)

    @staticmethod
    def _resolve_role_name(role: str) -> str:
        """Resolve a role_id to its role name if needed.

        If 'role' is already a name (e.g. 'admin'), return as-is. If
        'role' is a role_id (e.g. 'rol_123'), look up the Roles table.
        """
        try:
            from pluggable_apps.user_access_control.models.roles import Roles
            role_obj = Roles.objects.filter(role_id=role).first()
            if role_obj:
                return role_obj.name
        except Exception:
            pass
        return role

    def add_user_role(
        self, admin: Any, org_id: str, email: str, role: str
    ) -> Optional[dict]:
        """Change a user's role in an organization.

        Looks up the user by email, updates the OrganizationMember
        record, and delegates to Scalekit if available.
        """
        from backend.core.models.organization_member import OrganizationMember
        from django.contrib.auth import get_user_model

        User = get_user_model()
        try:
            user = User.objects.get(email=email)
        except User.DoesNotExist:
            Logger.error(f"User with email {email} not found")
            return None

        # Resolve role_id to role name if a role_id was passed
        role_name = self._resolve_role_name(role)

        # Update local OrganizationMember role
        updated = OrganizationMember.objects.filter(
            user=user,
            organization__organization_id=org_id,
        ).update(role=role_name)

        if not updated:
            Logger.error(f"No membership found for {email} in org {org_id}")
            return None

        self.auth_service.assign_role_to_org_user(org_id, user, role_name)

        return {"email": email, "role": role_name}

    def remove_user_role(
        self, admin: Any, org_id: str, email: str, role: str
    ) -> Optional[str]:
        """Remove a role from a user in an organization."""
        if hasattr(self.auth_service, "remove_user_role"):
            return self.auth_service.remove_user_role(admin, org_id, email, role)
        # OSS stub - not supported
        return None
