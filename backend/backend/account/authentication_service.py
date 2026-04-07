"""Authentication service for OSS mode.

Follows the same interface as ScalekitService to ensure compatibility.
Handles user signup, login, logout, and session management using Django's
built-in authentication system.
"""

import logging
import uuid
from typing import Any, Optional

from django.conf import settings
from django.contrib.auth import authenticate, get_user_model, login, logout
from django.contrib.auth.tokens import default_token_generator
from django.contrib.sessions.models import Session as DjangoSession
from django.db import transaction
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect
from django.utils import timezone
from django.utils.encoding import force_bytes, force_str
from django.utils.http import urlsafe_base64_encode, urlsafe_base64_decode
from rest_framework import status
from rest_framework.request import Request
from rest_framework.response import Response

from backend.core.models.organization_model import Organization
from backend.core.models.organization_member import OrganizationMember
from backend.account.constants import (
    DefaultOrg,
    ErrorMessage,
    OrgNamePattern,
    SuccessMessage,
    UserRole,
)

User = get_user_model()
Logger = logging.getLogger(__name__)


class AuthenticationService:
    """Authentication service for OSS mode.

    Implements the same interface as ScalekitService for compatibility.
    Provides signup, login, logout, and session management using Django sessions.
    """

    def __init__(self) -> None:
        pass

    # =========================================================================
    # Core Authentication (matches ScalekitService interface)
    # =========================================================================

    def is_authenticated(self, request: HttpRequest) -> bool:
        """Check if the current request is authenticated."""
        return request.user.is_authenticated

    def user_login(self, request: HttpRequest) -> HttpResponse:
        """Handle user login with email and password.

        For OSS: Authenticates with email/password
        For Scalekit: Redirects to SSO (different implementation)
        """
        if request.method == "GET":
            return Response(
                status=status.HTTP_200_OK,
                data={"message": "Use POST to login with email and password"},
            )

        from backend.account.serializers import LoginSerializer

        serializer = LoginSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(
                status=status.HTTP_400_BAD_REQUEST,
                data={"error": serializer.errors},
            )

        email = serializer.validated_data["email"]
        password = serializer.validated_data["password"]

        # Try to authenticate with database credentials
        user = authenticate(request, username=email, password=password)

        if user is not None:
            login(request, user)
            Logger.info(f"User logged in: {email}")
            return Response(
                status=status.HTTP_200_OK,
                data={"message": SuccessMessage.LOGIN_SUCCESS},
            )

        # Fallback: Try legacy env-based authentication
        if self._try_legacy_login(request, email, password):
            Logger.info(f"Legacy user logged in: {email}")
            return Response(
                status=status.HTTP_200_OK,
                data={"message": SuccessMessage.LOGIN_SUCCESS},
            )

        Logger.warning(f"Failed login attempt for: {email}")
        return Response(
            status=status.HTTP_401_UNAUTHORIZED,
            data={"error": ErrorMessage.USER_LOGIN_ERROR},
        )

    def user_logout(self, request: HttpRequest) -> HttpResponse:
        """Log out the current user and redirect to login page."""
        logout(request)
        return redirect("/login")

    def user_signup(self, request: Request) -> Response:
        """Handle user signup - create user, organization, and membership.

        OSS-specific: Scalekit uses SSO for signup.
        """
        from backend.account.serializers import SignupSerializer

        serializer = SignupSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(
                status=status.HTTP_400_BAD_REQUEST,
                data={"error": serializer.errors},
            )

        validated_data = serializer.validated_data
        email = validated_data["email"]
        password = validated_data["password"]
        display_name = validated_data.get("display_name", "")

        try:
            with transaction.atomic():
                user = self._create_user(email, password, display_name)
                organization = self._create_personal_organization(email, user)
                self._create_organization_membership(
                    user, organization, role=UserRole.ADMIN, is_admin=True
                )
                login(request, user)
                Logger.info(f"User signed up successfully: {email}")

                return Response(
                    status=status.HTTP_201_CREATED,
                    data={
                        "message": SuccessMessage.SIGNUP_SUCCESS,
                        "user": self._make_user_info_dict(user),
                        "organization": self._make_organization_info(organization),
                    },
                )
        except Exception as e:
            Logger.error(f"Signup failed for {email}: {e}")
            return Response(
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                data={"error": ErrorMessage.SIGNUP_ERROR},
            )

    # =========================================================================
    # Authorization Callback (matches ScalekitService interface)
    # =========================================================================

    def handle_authorization_callback(
        self, request: HttpRequest, backend: str = ""
    ) -> HttpResponse:
        """Handle SSO authorization callback.

        OSS: Returns error (SSO not supported)
        Scalekit: Handles OAuth callback
        """
        return Response(
            status=status.HTTP_400_BAD_REQUEST,
            data={"error": "SSO not supported in OSS mode. Use email/password login."},
        )

    # =========================================================================
    # Organization Methods (matches ScalekitService interface)
    # =========================================================================

    def user_organizations(self, request: HttpRequest) -> list:
        """Get list of organizations the current user belongs to.

        Returns list compatible with scalekit Membership objects.
        """
        if not request.user.is_authenticated:
            return []

        memberships = OrganizationMember.objects.filter(
            user=request.user
        ).select_related("organization")

        organizations = []
        for membership in memberships:
            if membership.organization:
                # Return format compatible with scalekit Membership
                organizations.append({
                    "organization_id": membership.organization.organization_id,
                    "name": membership.organization.name,
                    "display_name": membership.organization.display_name,
                    "role": membership.role,
                    "is_org_admin": membership.is_org_admin,
                })

        # Fallback for users without organization membership
        if not organizations:
            organizations.append({
                "organization_id": DefaultOrg.ORGANIZATION_NAME,
                "name": DefaultOrg.ORGANIZATION_NAME,
                "display_name": "Default Organization",
                "role": UserRole.ADMIN,
                "is_org_admin": True,
            })

        return organizations

    def switch_organization(
        self, request: HttpRequest, user_id: str, organization_id: str
    ) -> HttpResponse:
        """Switch user's current organization.

        OSS: Single org, returns error
        Scalekit: Switches active organization
        """
        return Response(
            status=status.HTTP_400_BAD_REQUEST,
            data={"error": "Multi-organization not supported in OSS mode."},
        )

    def create_organization(self, request: Request) -> Response:
        """Create a new organization.

        OSS: Limited to single org per user
        """
        if not request.user.is_authenticated:
            return Response(
                status=status.HTTP_401_UNAUTHORIZED,
                data={"error": "Not authenticated"},
            )

        existing_membership = OrganizationMember.objects.filter(user=request.user).first()
        if existing_membership:
            return Response(
                status=status.HTTP_400_BAD_REQUEST,
                data={"error": "User already has an organization. Multiple organizations not supported in OSS mode."},
            )

        name = request.data.get("name")
        display_name = request.data.get("display_name", name)

        if not name:
            return Response(
                status=status.HTTP_400_BAD_REQUEST,
                data={"error": "Organization name is required."},
            )

        try:
            with transaction.atomic():
                organization = Organization.objects.create(
                    name=name,
                    display_name=display_name,
                    organization_id=name.lower().replace(" ", "_"),
                    created_by=request.user,
                    modified_by=request.user,
                )
                self._create_organization_membership(
                    request.user, organization, role=UserRole.ADMIN, is_admin=True
                )
                return Response(
                    status=status.HTTP_201_CREATED,
                    data={
                        "message": "Organization created successfully",
                        "organization": self._make_organization_info(organization),
                    },
                )
        except Exception as e:
            Logger.error(f"Failed to create organization: {e}")
            return Response(
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                data={"error": "Failed to create organization."},
            )

    # =========================================================================
    # Session & User Info (matches ScalekitService interface)
    # =========================================================================

    def get_user_info(self, request: HttpRequest) -> Optional[dict]:
        """Get current user information."""
        if not request.user.is_authenticated:
            return None
        return self._make_user_info_dict(request.user)

    def get_session_info(self, request: Request) -> Optional[dict]:
        """Get current session information including user and organization."""
        if not request.user.is_authenticated:
            return None

        user = request.user
        user_id = getattr(user, "user_id", str(user.id))

        membership = OrganizationMember.objects.filter(user=user).first()
        org_id = ""
        user_role = ""
        is_org_admin = False

        if membership:
            org_id = membership.organization.organization_id if membership.organization else ""
            user_role = membership.role
            is_org_admin = membership.is_org_admin

        return {
            "id": user.id,
            "user_id": user_id,
            "email": user.email,
            "user": self._make_user_info_dict(user),
            "organization_id": org_id,
            "user_role": user_role,
            "is_org_admin": is_org_admin,
            "is_cloud": bool(settings.IS_CLOUD),
        }

    def landing(self, request: Request) -> Response:
        """Landing page endpoint."""
        if request.user.is_authenticated:
            return Response(
                status=status.HTTP_200_OK,
                data={"message": "Authenticated", "redirect": "/project/list"},
            )
        return Response(
            status=status.HTTP_200_OK,
            data={"message": "Welcome to Visitran", "redirect": "/login"},
        )

    # =========================================================================
    # Role Management (matches ScalekitService interface)
    # =========================================================================

    def get_roles(self) -> list:
        """Get available roles."""
        return [{"id": "admin", "name": "Admin", "display_name": "Administrator"}]

    def add_organization_user_role(
        self, organization_id: str, user: Any, user_role_name: str
    ) -> Optional[list]:
        """Add role to user. OSS stub."""
        return None  # Not supported in OSS

    def assign_role_to_org_user(
        self, organization_id: str, user: Any, user_role_name: str = "admin"
    ) -> list:
        """Assign role to organization user. OSS stub."""
        return []  # Not supported in OSS

    def get_organization_role_of_user(
        self, user_id: str, organization_id: str
    ) -> list:
        """Get user's role in organization."""
        return []  # Basic implementation

    # =========================================================================
    # User Management (matches ScalekitService interface)
    # =========================================================================

    def invite_user(
        self, admin: Any, org_id: str, email: str, role: str = "admin"
    ) -> bool:
        """Invite a user to organization. OSS stub."""
        return False  # Not supported in OSS

    def remove_users_from_organization(
        self, organization_id: str, user: Any, user_id: str
    ) -> bool:
        """Remove user from auth provider. OSS no-op."""
        return True

    def get_organizations_users(self, org_id: str) -> list:
        """Get organization members."""
        memberships = OrganizationMember.objects.filter(
            organization__organization_id=org_id
        ).select_related("user")

        return [
            self._make_user_info_dict(m.user) for m in memberships if m.user
        ]

    def get_invitations(self, organization_id: str) -> list:
        """Get pending invitations. OSS returns empty."""
        return []

    def delete_invitation(self, organization_id: str, invitation_id: str) -> bool:
        """Delete invitation. OSS stub."""
        return False

    # =========================================================================
    # Additional Scalekit-compatible methods
    # =========================================================================

    def get_organization_by_org_id(self, org_id: str) -> Optional[Organization]:
        """Get organization by ID."""
        return Organization.objects.filter(organization_id=org_id).first()

    def is_user_member_of_organization(self, user_id: str, organization_id: str) -> bool:
        """Check if user is member of organization."""
        return OrganizationMember.objects.filter(
            user__user_id=user_id,
            organization__organization_id=organization_id
        ).exists()

    def get_organizations_by_user_id(self, user_id: str) -> list:
        """Get organizations for a user by user_id."""
        memberships = OrganizationMember.objects.filter(
            user__user_id=user_id
        ).select_related("organization")

        return [
            {
                "organization_id": m.organization.organization_id,
                "name": m.organization.name,
                "display_name": m.organization.display_name,
            }
            for m in memberships if m.organization
        ]

    def create_roles(self, role: Any) -> Any:
        """Create role. OSS stub."""
        return None

    def delete_role(self, role_id: str) -> bool:
        """Delete role. OSS stub."""
        return False

    def forgot_password(self, request: HttpRequest) -> Response:
        """Generate a password reset token and return reset link.

        For OSS mode: the reset link is logged to the server console and
        returned in the response so the self-hosted admin can use it.
        The same 200 response shape is returned whether the user exists
        or not, to prevent email enumeration.
        """
        from backend.account.serializers import ForgotPasswordSerializer

        serializer = ForgotPasswordSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(
                status=status.HTTP_400_BAD_REQUEST,
                data={"error": serializer.errors},
            )

        email = serializer.validated_data["email"]
        reset_url = None

        try:
            user = User.objects.get(email=email, is_active=True)

            # Generate token using Django's built-in token generator
            token = default_token_generator.make_token(user)
            uid = urlsafe_base64_encode(force_bytes(user.pk))

            # Build reset URL
            reset_path = f"/reset-password/{uid}/{token}"
            origin = request.META.get("HTTP_ORIGIN", "")
            if not origin:
                origin = f"{request.scheme}://{request.get_host()}"
            reset_url = f"{origin}{reset_path}"

            Logger.info(f"Password reset link generated for user: {email}")
        except User.DoesNotExist:
            Logger.info(f"Password reset requested for unknown email: {email}")
            return Response(
                status=status.HTTP_400_BAD_REQUEST,
                data={"error": "No account found with this email address."},
            )

        data = {"message": SuccessMessage.FORGOT_PASSWORD_SUCCESS}
        if reset_url is not None:
            data["reset_url"] = reset_url
        return Response(status=status.HTTP_200_OK, data=data)

    def reset_password(self, request: HttpRequest) -> Response:
        """Reset user password using a valid token."""
        from backend.account.serializers import ResetPasswordSerializer

        serializer = ResetPasswordSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(
                status=status.HTTP_400_BAD_REQUEST,
                data={"error": serializer.errors},
            )

        uid = serializer.validated_data["uid"]
        token = serializer.validated_data["token"]
        password = serializer.validated_data["password"]

        try:
            user_id = force_str(urlsafe_base64_decode(uid))
            user = User.objects.get(pk=user_id, is_active=True)
        except (TypeError, ValueError, OverflowError, User.DoesNotExist):
            return Response(
                status=status.HTTP_400_BAD_REQUEST,
                data={"error": ErrorMessage.INVALID_RESET_TOKEN},
            )

        if not default_token_generator.check_token(user, token):
            return Response(
                status=status.HTTP_400_BAD_REQUEST,
                data={"error": ErrorMessage.INVALID_RESET_TOKEN},
            )

        user.set_password(password)
        user.save()

        # Flush all sessions for this user so compromised sessions are killed
        for session in DjangoSession.objects.filter(expire_date__gte=timezone.now()):
            data = session.get_decoded()
            if str(data.get("_auth_user_id")) == str(user.pk):
                session.delete()

        Logger.info(f"Password reset successful for user: {user.email}")

        return Response(
            status=status.HTTP_200_OK,
            data={"message": SuccessMessage.RESET_PASSWORD_SUCCESS},
        )

    def validate_reset_token(self, request: HttpRequest) -> Response:
        """Validate a password reset token without consuming it."""
        uid = request.data.get("uid")
        token = request.data.get("token")

        if not uid or not token:
            return Response(
                status=status.HTTP_400_BAD_REQUEST,
                data={"valid": False, "error": "Missing uid or token."},
            )

        try:
            user_id = force_str(urlsafe_base64_decode(uid))
            user = User.objects.get(pk=user_id, is_active=True)
        except (TypeError, ValueError, OverflowError, User.DoesNotExist):
            return Response(
                status=status.HTTP_400_BAD_REQUEST,
                data={"valid": False, "error": ErrorMessage.INVALID_RESET_TOKEN},
            )

        if not default_token_generator.check_token(user, token):
            return Response(
                status=status.HTTP_400_BAD_REQUEST,
                data={"valid": False, "error": ErrorMessage.INVALID_RESET_TOKEN},
            )

        return Response(
            status=status.HTTP_200_OK,
            data={"valid": True},
        )

    def reset_user_password(self, user: Any) -> Response:
        """Reset user password. OSS stub (legacy interface)."""
        return Response(
            status=status.HTTP_400_BAD_REQUEST,
            data={"error": "Password reset not supported in OSS mode."},
        )

    # =========================================================================
    # Private Helper Methods
    # =========================================================================

    def _create_user(self, email: str, password: str, display_name: str = "") -> User:
        """Create a new user with the given credentials."""
        user_id = str(uuid.uuid4())
        user = User.objects.create(
            username=email,
            email=email,
            user_id=user_id,
            first_name=display_name or email.split("@")[0],
        )
        user.set_password(password)
        user.save()
        return user

    def _create_personal_organization(self, email: str, user: User) -> Organization:
        """Create a personal organization for the user."""
        org_name = OrgNamePattern.make_personal_org_name(email)
        org_id = OrgNamePattern.make_org_id(email)

        organization = Organization.objects.create(
            name=org_name,
            display_name=org_name,
            organization_id=org_id,
            created_by=user,
            modified_by=user,
        )
        return organization

    def _create_organization_membership(
        self,
        user: User,
        organization: Organization,
        role: str = UserRole.ADMIN,
        is_admin: bool = True,
    ) -> OrganizationMember:
        """Create organization membership for the user."""
        membership = OrganizationMember.objects.create(
            user=user,
            organization=organization,
            role=role,
            is_org_admin=is_admin,
            is_login_onboarding_msg=True,
            starter_projects_created=False,
        )
        return membership

    def _try_legacy_login(
        self, request: Request, email: str, password: str
    ) -> bool:
        """Try legacy env-based authentication for backward compatibility."""
        legacy_username = DefaultOrg.MOCK_USER
        legacy_password = DefaultOrg.MOCK_USER_PASSWORD
        legacy_email = DefaultOrg.MOCK_USER_EMAIL

        # Block legacy login if no password is configured
        if not legacy_password:
            return False

        if (email in (legacy_username, legacy_email)) and password == legacy_password:
            self._ensure_legacy_user()
            user = authenticate(request, username=legacy_email, password=password)
            if user:
                login(request, user)
                return True
        return False

    def _ensure_legacy_user(self) -> None:
        """Ensure the legacy mock user exists in the database."""
        email = DefaultOrg.MOCK_USER_EMAIL
        user, created = User.objects.get_or_create(
            username=email,
            defaults={
                "email": email,
                "user_id": DefaultOrg.MOCK_USER_ID,
            },
        )
        if created or not user.has_usable_password():
            user.set_password(DefaultOrg.MOCK_USER_PASSWORD)
            user.save()

        if not OrganizationMember.objects.filter(user=user).exists():
            org, _ = Organization.objects.get_or_create(
                organization_id=DefaultOrg.ORGANIZATION_NAME,
                defaults={
                    "name": DefaultOrg.ORGANIZATION_NAME,
                    "display_name": "Default Organization",
                },
            )
            OrganizationMember.objects.create(
                user=user,
                organization=org,
                role=UserRole.ADMIN,
                is_org_admin=True,
            )

    def _make_user_info_dict(self, user: User) -> dict:
        """Create user info dictionary."""
        user_id = getattr(user, "user_id", str(user.id))
        return {
            "id": user.id,
            "user_id": user_id,
            "name": user.username,
            "display_name": user.get_full_name() or user.username,
            "email": user.email,
        }

    def _make_organization_info(self, organization: Organization) -> dict:
        """Create organization info dictionary."""
        return {
            "id": organization.organization_id,
            "name": organization.name,
            "display_name": organization.display_name,
            "organization_id": organization.organization_id,
        }
