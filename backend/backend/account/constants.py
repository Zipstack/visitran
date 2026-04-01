"""Constants for account module."""

import os


class DefaultOrg:
    """Default organization constants.

    Used for OSS mode when auto-creating a personal organization. Legacy
    mock user support retained for backward compatibility.
    """

    # Legacy mock user support (for backward compatibility with env-based auth)
    ORGANIZATION_NAME = "default_org"
    MOCK_USER = os.environ.get("SYSTEM_ADMIN_USERNAME", "admin")
    MOCK_USER_ID = "default_user_id"
    MOCK_USER_EMAIL = os.environ.get("SYSTEM_ADMIN_EMAIL", "admin@visitran.local")
    MOCK_USER_PASSWORD = os.environ.get("SYSTEM_ADMIN_PASSWORD", "")


class UserRole:
    """User role constants."""

    ADMIN = "admin"
    USER = "user"
    # For compatibility with cloud roles
    VISITRAN_ADMIN = "visitran_admin"
    VISITRAN_USER = "visitran_user"
    VISITRAN_SUPER_ADMIN = "visitran_super_admin"

    @classmethod
    def is_admin_role(cls, role: str) -> bool:
        """Check if role has admin privileges."""
        return role.lower() in (
            cls.ADMIN,
            cls.VISITRAN_ADMIN,
            cls.VISITRAN_SUPER_ADMIN,
        )


class ErrorMessage:
    """Error messages for authentication."""

    USER_LOGIN_ERROR = "Invalid email or password. Please try again."
    USER_NOT_FOUND = "No account found with this email."
    SIGNUP_ERROR = "Unable to create account. Please try again."
    EMAIL_EXISTS = "An account with this email already exists."
    PASSWORD_MISMATCH = "Passwords do not match."
    INVALID_CREDENTIALS = "Invalid credentials."
    INVALID_RESET_TOKEN = "This password reset link is invalid or has expired."


class SuccessMessage:
    """Success messages for authentication."""

    SIGNUP_SUCCESS = "Account created successfully."
    LOGIN_SUCCESS = "Login successful."
    LOGOUT_SUCCESS = "Logged out successfully."
    FORGOT_PASSWORD_SUCCESS = "If an account exists with that email, a password reset link has been generated."
    RESET_PASSWORD_SUCCESS = "Password has been reset successfully. You can now log in with your new password."


class Cookie:
    """Cookie name constants."""

    ORG_ID = "org_id"
    CSRFTOKEN = "csrftoken"


class OrgNamePattern:
    """Patterns for generating organization names."""

    PERSONAL_ORG_SUFFIX = "'s Workspace"
    DEFAULT_ORG_NAME = "Personal Workspace"

    @classmethod
    def make_personal_org_name(cls, email: str) -> str:
        """Generate a personal organization name from email."""
        username = email.split("@")[0]
        # Capitalize first letter
        username = username.capitalize()
        return f"{username}{cls.PERSONAL_ORG_SUFFIX}"

    @classmethod
    def make_org_id(cls, email: str) -> str:
        """Generate a unique organization ID from email."""
        import uuid

        # Use email prefix + short uuid for uniqueness
        prefix = email.split("@")[0].lower()[:20]
        short_uuid = str(uuid.uuid4())[:8]
        return f"org_{prefix}_{short_uuid}"


class Common:
    """Common constants used across the application."""

    NEXT_URL_VARIABLE = "next"
    PUBLIC_SCHEMA_NAME = "public"
    ID = "id"
    USER_ID = "user_id"
    USER_EMAIL = "email"
    USER_EMAILS = "emails"
    USER_IDS = "user_ids"
    USER_ROLE = "role"
    MAX_EMAIL_IN_REQUEST = 10
    LOG_EVENTS_ID = "log_events_id"
    CURRENT_ORG = "current_org"


class LoginConstant:
    """Login related constants."""

    INVITATION = "invitation"
    ORGANIZATION = "organization"
    ORGANIZATION_NAME = "organization_name"


class UserModel:
    """User model field constants."""

    USER_ID = "user_id"
    ID = "id"


class OrganizationMemberModel:
    """Organization member model field constants."""

    USER_ID = "user__user_id"
    ID = "user__id"


class PluginConfig:
    """Plugin configuration constants."""

    PLUGINS_APP = "plugins"
    AUTH_MODULE_PREFIX = "scalekit"
    AUTH_PLUGIN_DIR = "authentication"
    AUTH_MODULE = "module"
    AUTH_METADATA = "metadata"
    METADATA_SERVICE_CLASS = "service_class"
    METADATA_IS_ACTIVE = "is_active"


class UserLoginTemplate:
    """Login template constants."""

    TEMPLATE = "login.html"
    ERROR_PLACE_HOLDER = "error_message"


class AuthorizationErrorCode:
    """Authorization error codes."""

    IDM = "IDM"
    UMM = "UMM"
    INF = "INF"
    USF = "USF"
