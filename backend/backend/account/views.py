"""Views for account module - authentication endpoints.

Uses scalekit-compatible interface naming.
"""

from django.views.decorators.csrf import csrf_exempt
from rest_framework.decorators import api_view, permission_classes, throttle_classes
from rest_framework.permissions import AllowAny
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.throttling import AnonRateThrottle

from backend.account.authentication_controller import AuthenticationController


class LoginThrottle(AnonRateThrottle):
    """Limit login/signup/callback requests to 10/minute per IP."""

    rate = "10/min"


class ForgotPasswordThrottle(AnonRateThrottle):
    """Limit forgot-password requests to 5/minute per IP."""

    rate = "5/min"


@api_view(["GET"])
@permission_classes([AllowAny])
def landing(request: Request) -> Response:
    """Landing page endpoint.

    GET /api/v1/landing

    Returns:
        200: Landing page response
    """
    auth_controller = AuthenticationController()
    return auth_controller.landing(request)


@csrf_exempt
@api_view(["POST"])
@permission_classes([AllowAny])
@throttle_classes([LoginThrottle])
def signup(request: Request) -> Response:
    """Handle user signup.

    POST /api/v1/signup
    Body:
        - email: User's email (required)
        - password: Password (required)
        - confirm_password: Password confirmation (required)
        - display_name: Optional display name

    Returns:
        201: User created successfully
        400: Validation error
        500: Server error
    """
    auth_controller = AuthenticationController()
    return auth_controller.user_signup(request)


@csrf_exempt
@api_view(["GET", "POST"])
@permission_classes([AllowAny])
@throttle_classes([LoginThrottle])
def login(request: Request) -> Response:
    """Handle user login.

    GET /api/v1/login - Returns login instructions (OSS) or redirects to SSO (cloud)
    POST /api/v1/login - Authenticates user with email/password (OSS)

    Returns:
        200: Login successful
        302: Redirect to SSO (cloud)
        400: Validation error
        401: Invalid credentials
    """
    auth_controller = AuthenticationController()
    return auth_controller.user_login(request)


@csrf_exempt
@api_view(["GET", "POST"])
@permission_classes([AllowAny])
def logout(request: Request) -> Response:
    """Handle user logout.

    GET/POST /api/v1/logout - Logs out the current user

    Returns:
        200: Logout successful
    """
    auth_controller = AuthenticationController()
    return auth_controller.user_logout(request)


@api_view(["GET"])
@permission_classes([AllowAny])
@throttle_classes([LoginThrottle])
def callback(request: Request) -> Response:
    """Handle SSO authorization callback.

    GET /api/v1/callback

    Returns:
        302: Redirect to app on success (cloud)
        400: Error if SSO not supported (OSS)
    """
    auth_controller = AuthenticationController()
    return auth_controller.handle_authorization_callback(request)


@api_view(["GET"])
def get_session_data(request: Request) -> Response:
    """Get current session information.

    GET /api/v1/session

    Returns:
        200: Session data with user and organization info
        401: Not authenticated
    """
    auth_controller = AuthenticationController()
    return auth_controller.get_session(request)


@csrf_exempt
@api_view(["POST"])
@permission_classes([AllowAny])
@throttle_classes([ForgotPasswordThrottle])
def forgot_password(request: Request) -> Response:
    """Handle forgot password request.

    POST /api/v1/forgot-password
    Body:
        - email: User's email (required)

    Returns:
        200: Reset link generated (OSS: included in response)
        400: Validation error
        429: Rate limited
    """
    auth_controller = AuthenticationController()
    return auth_controller.forgot_password(request)


@csrf_exempt
@api_view(["POST"])
@permission_classes([AllowAny])
def reset_password(request: Request) -> Response:
    """Handle password reset.

    POST /api/v1/reset-password
    Body:
        - uid: Base64-encoded user ID (required)
        - token: Password reset token (required)
        - password: New password (required)
        - confirm_password: Password confirmation (required)

    Returns:
        200: Password reset successful
        400: Invalid/expired token or validation error
    """
    auth_controller = AuthenticationController()
    return auth_controller.reset_password(request)


@csrf_exempt
@api_view(["POST"])
@permission_classes([AllowAny])
def validate_reset_token(request: Request) -> Response:
    """Validate a password reset token without consuming it.

    POST /api/v1/validate-reset-token
    Body:
        - uid: Base64-encoded user ID (required)
        - token: Password reset token (required)

    Returns:
        200: Token is valid
        400: Token is invalid or expired
    """
    auth_controller = AuthenticationController()
    return auth_controller.validate_reset_token(request)


@api_view(["GET"])
def get_organizations(request: Request) -> Response:
    """Get organizations for the current user.

    GET /api/v1/organization

    Returns:
        200: List of organizations
    """
    auth_controller = AuthenticationController()
    return auth_controller.user_organizations(request)


@api_view(["GET", "POST", "PUT"])
def set_organization(request: Request, id: str) -> Response:
    """Set/switch the current organization.

    GET/POST/PUT /api/v1/organization/<id>/set

    Returns:
        200: Organization switched successfully
        400: Error (single org in OSS mode)
    """
    auth_controller = AuthenticationController()
    # Extract user_id from request for scalekit-compatible interface
    user_id = getattr(request.user, "user_id", str(request.user.id)) if request.user.is_authenticated else ""
    return auth_controller.switch_organization(request, user_id, id)


@api_view(["POST"])
def create_organization(request: Request) -> Response:
    """Create a new organization.

    POST /api/v1/organization/create
    Body:
        - name: Organization name (required)
        - display_name: Display name (optional)

    Returns:
        201: Organization created
        400: Error
    """
    auth_controller = AuthenticationController()
    return auth_controller.create_organization(request)
