"""Data transfer objects for account module.

These DTOs provide a common interface for both OSS and cloud
authentication.
"""

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class MemberData:
    """Data for organization member."""
    user_id: str
    email: Optional[str] = None
    name: Optional[str] = None
    picture: Optional[str] = None
    role: Optional[list[str]] = None
    organization_id: Optional[str] = None


@dataclass
class OrganizationData:
    """Data for organization."""
    id: str
    display_name: str
    name: str


@dataclass
class CallbackData:
    """Data from SSO callback."""
    user_id: str
    email: str
    token: Any


@dataclass
class OrganizationSignupRequestBody:
    """Request body for organization signup."""
    name: str
    display_name: str
    organization_id: str


@dataclass
class OrganizationSignupResponse:
    """Response for organization signup."""
    name: str
    display_name: str
    organization_id: str
    created_at: str


@dataclass
class UserInfo:
    """User information."""
    email: str
    user_id: str
    id: Optional[str] = None
    name: Optional[str] = None
    display_name: Optional[str] = None
    family_name: Optional[str] = None
    picture: Optional[str] = None


@dataclass
class UserSessionInfo:
    """Session information for a user."""
    id: str
    user_id: str
    email: str
    organization_id: str
    user: UserInfo
    user_role: str
    is_org_admin: bool

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "UserSessionInfo":
        return UserSessionInfo(
            id=data["id"],
            user_id=data["user_id"],
            email=data["email"],
            organization_id=data["organization_id"],
            user_role=data["user_role"],
            is_org_admin=data["is_org_admin"],
            user=data.get("user"),
        )

    def to_dict(self) -> Any:
        return {
            "id": self.id,
            "user_id": self.user_id,
            "email": self.email,
            "organization_id": self.organization_id,
            "user_role": self.user_role,
            "is_org_admin": self.is_org_admin,
        }


@dataclass
class GetUserResponse:
    """Response for get user request."""
    user: UserInfo
    organizations: list[OrganizationData]


@dataclass
class ResetUserPasswordDto:
    """DTO for password reset."""
    status: bool
    message: str


@dataclass
class UserInviteResponse:
    """Response for user invitation."""
    email: str
    status: str
    message: Optional[str] = None


@dataclass
class UserRoleData:
    """Data for user role."""
    name: str
    display_name: Optional[str] = None
    id: Optional[str] = None
    description: Optional[str] = None


@dataclass
class MemberInvitation:
    """Represents an invitation to join an organization.

    Attributes:
        id (str): The unique identifier for the invitation.
        email (str): The user email.
        roles (List[str]): The roles assigned to the invitee.
        created_at (Optional[str]): The timestamp when the invitation
            was created.
        expires_at (Optional[str]): The timestamp when the invitation expires.
    """
    id: str
    email: str
    roles: list[str]
    created_at: Optional[str] = None
    expires_at: Optional[str] = None


@dataclass
class UserOrganizationRole:
    """User's role in an organization."""
    user_id: str
    role: UserRoleData
    organization_id: str
