import uuid

from django.db import models

from backend.core.models.project_details import ProjectDetails
from backend.utils.encryption import (
    encrypt_connection_details,
    decrypt_connection_details,
    mask_connection_details,
)
from backend.utils.tenant_context import get_current_user
from utils.models.base_model import BaseModel
from utils.models.organization_mixin import (
    DefaultOrganizationManagerMixin,
    DefaultOrganizationMixin,
)


class GitRepoConfigManager(DefaultOrganizationManagerMixin, models.Manager):
    pass


class GitRepoConfig(DefaultOrganizationMixin, BaseModel):
    """Stores git repository configuration for a project.

    Each project can have at most one active (non-deleted) git config.
    Supports default (managed) and custom (user-provided) repositories.
    """

    REPO_TYPE_CHOICES = [
        ("default", "Default"),
        ("custom", "Custom"),
    ]

    AUTH_TYPE_CHOICES = [
        ("pat", "PAT"),
        ("ssh", "SSH"),
        ("oauth", "OAuth"),
    ]

    CONNECTION_STATUS_CHOICES = [
        ("connected", "Connected"),
        ("error", "Error"),
        ("pending", "Pending"),
    ]

    git_repo_config_id = models.UUIDField(
        primary_key=True, default=uuid.uuid4, editable=False
    )
    project = models.ForeignKey(
        ProjectDetails,
        on_delete=models.CASCADE,
        related_name="git_configs",
    )
    repo_type = models.CharField(max_length=20, choices=REPO_TYPE_CHOICES)
    repo_url = models.CharField(max_length=500)
    auth_type = models.CharField(
        max_length=20, choices=AUTH_TYPE_CHOICES, default="pat"
    )
    encrypted_credentials = models.JSONField(default=dict)
    branch_name = models.CharField(max_length=200, default="main")
    base_path = models.CharField(max_length=500, blank=True, default="")
    is_active = models.BooleanField(default=True)
    last_synced_at = models.DateTimeField(null=True, blank=True)
    connection_status = models.CharField(
        max_length=20,
        choices=CONNECTION_STATUS_CHOICES,
        default="pending",
    )
    error_message = models.TextField(blank=True, default="")

    is_deleted = models.BooleanField(default=False)
    created_by = models.JSONField(default=dict)
    last_modified_by = models.JSONField(default=dict)

    objects = GitRepoConfigManager()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["project"],
                condition=models.Q(is_deleted=False),
                name="unique_active_git_config_per_project",
            )
        ]
        indexes = [
            models.Index(
                fields=["project", "is_active"],
                name="idx_grc_project_active",
            ),
            models.Index(
                fields=["organization", "is_deleted"],
                name="idx_grc_org",
            ),
        ]

    def save(self, *args, **kwargs):
        current_user = get_current_user()
        if self._state.adding:
            self.created_by = current_user
        self.last_modified_by = current_user

        if self.encrypted_credentials:
            self.encrypted_credentials = encrypt_connection_details(
                self.encrypted_credentials
            )

        super().save(*args, **kwargs)

    @property
    def decrypted_credentials(self) -> dict:
        """Get decrypted credentials for internal use (git operations)."""
        return decrypt_connection_details(self.encrypted_credentials)

    @property
    def masked_credentials(self) -> dict:
        """Get masked credentials for API responses."""
        return mask_connection_details(self.decrypted_credentials)

    def __str__(self):
        return f"GitConfig({self.repo_type}) for {self.project}"
