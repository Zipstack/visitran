"""OrganizationMember model for tracking user-organization relationships.

This model is shared between OSS and Cloud deployments:
- OSS: Single user with a personal organization (auto-created on signup)
- Cloud: Multi-tenant with users belonging to multiple organizations
"""

from django.db import models

from backend.core.models.organization_model import Organization
from backend.core.models.user_model import User
from utils.models.base_model import BaseModel
from utils.models.organization_mixin import DefaultOrganizationManagerMixin, DefaultOrganizationMixin


class OrganizationMemberManager(DefaultOrganizationManagerMixin, models.Manager):
    """Manager that filters by current organization context.

    Used in cloud/multi-tenant mode to automatically scope queries to
    the current organization.
    """

    pass


class OrganizationMemberBaseManager(models.Manager):
    """Base manager without organization filtering.

    Used for OSS mode or when querying across organizations.
    """

    pass


class OrganizationMember(DefaultOrganizationMixin, BaseModel):
    """Tracks membership of users in organizations.

    Attributes:
        member_id: Primary key for the membership record
        user: Foreign key to the User model
        organization: Foreign key to the Organization model
        role: User's role within the organization (e.g., 'admin', 'user')
        is_org_admin: Whether the user has admin privileges in the org
        is_login_onboarding_msg: Flag for showing onboarding messages
        starter_projects_created: Flag indicating if starter projects were created
    """

    member_id = models.BigAutoField(primary_key=True)
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="organization_members",
    )
    # Override organization from mixin to add unique related_name
    organization = models.ForeignKey(
        Organization,
        on_delete=models.CASCADE,
        db_comment="Foreign key reference to the Organization model.",
        null=True,
        blank=True,
        default=None,
        related_name="organization_members",
    )
    role = models.CharField(max_length=50, default="admin")
    is_login_onboarding_msg = models.BooleanField(
        default=True,
        db_comment="Flag to indicate whether the onboarding messages are shown",
    )
    is_org_admin = models.BooleanField(default=True)
    starter_projects_created = models.BooleanField(
        default=False,
        db_comment="Flag to indicate whether starter projects have been created",
    )

    # Default manager (no filtering) - used for OSS and cross-org queries
    objects = OrganizationMemberBaseManager()

    # Tenant-scoped manager - used in cloud for auto-filtering by organization
    tenant_objects = OrganizationMemberManager()

    class Meta:
        db_table = "tenant_account_organizationmember"
        unique_together = ("user", "organization")
        verbose_name = "Organization Member"
        verbose_name_plural = "Organization Members"

    def __str__(self) -> str:
        org_name = self.organization.name if self.organization else "No Org"
        return f"OrganizationMember({self.member_id}, role={self.role}, user={self.user.email}, org={org_name})"

    @property
    def is_admin(self) -> bool:
        """Check if member has admin role."""
        return self.role.lower() in ("admin", "visitran_admin", "visitran_super_admin")
