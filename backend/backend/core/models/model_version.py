import hashlib
import json
import logging
import uuid

from django.db import models
from django.db.models import Q

from backend.core.models.config_models import ConfigModels
from backend.core.models.project_details import ProjectDetails
from utils.models.base_model import BaseModel
from utils.models.organization_mixin import (
    DefaultOrganizationManagerMixin,
    DefaultOrganizationMixin,
)

logger = logging.getLogger(__name__)


class ModelVersionManager(DefaultOrganizationManagerMixin, models.Manager):
    pass


class ModelVersion(DefaultOrganizationMixin, BaseModel):
    """Stores immutable versioned snapshots of ConfigModels.model_data.

    Each commit (manual or auto) creates a new ModelVersion record.
    """

    version_id = models.UUIDField(
        primary_key=True, default=uuid.uuid4, editable=False
    )
    config_model = models.ForeignKey(
        ConfigModels,
        on_delete=models.CASCADE,
        related_name="versions",
        null=True,
        blank=True,
    )
    project_instance = models.ForeignKey(
        ProjectDetails,
        on_delete=models.CASCADE,
        related_name="model_versions",
    )
    version_number = models.PositiveIntegerField()
    model_data = models.JSONField(null=True, blank=True, default=None)
    commit_message = models.CharField(max_length=500, blank=True, default="")

    # User attribution — JSONField following this repo's convention
    committed_by = models.JSONField(default=dict)

    # Point-in-time user snapshots (immutable attribution)
    user_name_snapshot = models.TextField(blank=True, default="")
    user_email_snapshot = models.TextField(blank=True, default="")
    user_role_snapshot = models.TextField(blank=True, default="")

    # Tamper detection
    content_hash = models.CharField(max_length=64, blank=True, default="")

    # Commit metadata
    is_auto_commit = models.BooleanField(default=False)

    # Rollback metadata (populated for rollback versions)
    rollback_metadata = models.JSONField(default=dict, blank=True)

    # Git sync tracking
    GIT_SYNC_STATUS_CHOICES = [
        ("not_applicable", "N/A"),
        ("pending", "Pending"),
        ("synced", "Synced"),
        ("failed", "Failed"),
    ]
    git_sync_status = models.CharField(
        max_length=20,
        choices=GIT_SYNC_STATUS_CHOICES,
        default="not_applicable",
    )
    git_commit_sha = models.CharField(max_length=40, blank=True, default="")

    # PR tracking
    git_branch_name = models.CharField(max_length=255, null=True, blank=True)
    pr_number = models.IntegerField(null=True, blank=True)
    pr_url = models.CharField(max_length=500, null=True, blank=True)

    # Project-level model count (set when config_model is None)
    extracted_model_count = models.PositiveIntegerField(default=0)

    # Active version tracking
    is_current = models.BooleanField(default=False, db_index=True)

    objects = ModelVersionManager()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["project_instance", "version_number"],
                condition=Q(config_model__isnull=True),
                name="unique_project_version",
            )
        ]
        ordering = ["-version_number"]
        indexes = [
            models.Index(
                fields=["project_instance", "-version_number"],
                name="idx_mv_proj_version",
            ),
            models.Index(
                fields=["organization", "project_instance", "-created_at"],
                name="idx_mv_org_isolation",
            ),
        ]

    def compute_content_hash(self) -> str:
        """Compute SHA-256 hash of version content for tamper detection."""
        hash_input = (
            json.dumps(self.model_data, sort_keys=True)
            + str(self.version_number)
            + json.dumps(self.committed_by, sort_keys=True)
        )
        return hashlib.sha256(hash_input.encode("utf-8")).hexdigest()

    def verify_content_hash(self) -> bool:
        """Verify the stored hash matches recomputed hash."""
        if not self.content_hash:
            return True
        expected = self.compute_content_hash()
        if expected != self.content_hash:
            label = (
                self.config_model.model_name
                if self.config_model
                else f"project:{self.project_instance_id}"
            )
            logger.warning(
                "Content hash mismatch for ModelVersion %s (v%d). "
                "Expected %s, got %s.",
                label,
                self.version_number,
                expected,
                self.content_hash,
            )
            return False
        return True

    def save(self, *args, **kwargs):
        if self._state.adding:
            if self.config_model is None:
                data = self.model_data or {}
                self.extracted_model_count = len(data)
            self.content_hash = self.compute_content_hash()
        super().save(*args, **kwargs)

    def __str__(self):
        if self.config_model:
            return f"{self.config_model.model_name} v{self.version_number}"
        return f"Project v{self.version_number}"
