import uuid

from django.db import models
from django.utils import timezone

from backend.core.models.config_models import ConfigModels
from backend.core.models.model_version import ModelVersion
from utils.models.base_model import BaseModel
from utils.models.organization_mixin import (
    DefaultOrganizationManagerMixin,
    DefaultOrganizationMixin,
)

DRAFT_STALE_DAYS = 7


class UserDraftManager(DefaultOrganizationManagerMixin, models.Manager):
    pass


class UserDraft(DefaultOrganizationMixin, BaseModel):
    """Per-user isolated draft for a transformation model.

    Each user can have exactly one draft per ConfigModel (enforced by
    unique constraint on config_model + owner_id). Drafts store in-progress
    YAML edits and optionally capture validation errors.
    """

    draft_id = models.UUIDField(
        primary_key=True, default=uuid.uuid4, editable=False
    )
    config_model = models.ForeignKey(
        ConfigModels,
        on_delete=models.CASCADE,
        related_name="drafts",
    )

    # User ownership — scalar ID for constraints, JSONField for full info
    owner_id = models.CharField(max_length=255)
    owner = models.JSONField(default=dict)

    draft_version = models.ForeignKey(
        ModelVersion,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="derived_drafts",
    )
    base_version_number = models.PositiveIntegerField(default=0)

    # Draft content: {"model_data": {...}, "validation_errors": [...]}
    draft_data = models.JSONField(default=dict)

    # Optimistic locking: incremented on each save
    version = models.PositiveIntegerField(default=1)

    # Auto-save tracking
    last_auto_save = models.DateTimeField(null=True, blank=True)

    # Dirty flag: True when draft has unsaved changes pending auto-save
    is_dirty = models.BooleanField(default=False)

    # Editing lock fields for concurrent editing detection
    lock_acquired_at = models.DateTimeField(null=True, blank=True)
    lock_expires_at = models.DateTimeField(null=True, blank=True)

    # Lock token for optimistic concurrency control
    lock_token = models.UUIDField(default=uuid.uuid4)

    objects = UserDraftManager()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["config_model", "owner_id"],
                name="unique_draft_per_user_model",
            )
        ]
        indexes = [
            models.Index(
                fields=["owner_id", "config_model"],
                name="idx_draft_owner_lookup",
            ),
            models.Index(
                fields=["last_auto_save"],
                name="idx_draft_stale_cleanup",
            ),
        ]

    def save(self, *args, **kwargs):
        self.lock_token = uuid.uuid4()
        super().save(*args, **kwargs)

    def is_stale(self) -> bool:
        """Check if this draft is older than the retention period."""
        ref_time = self.last_auto_save or self.modified_at
        cutoff = timezone.now() - timezone.timedelta(days=DRAFT_STALE_DAYS)
        return ref_time < cutoff

    def is_locked(self) -> bool:
        """Check if the editing lock is currently active."""
        if not self.lock_expires_at:
            return False
        return timezone.now() < self.lock_expires_at

    def __str__(self):
        return (
            f"Draft({self.config_model.model_name}, "
            f"owner={self.owner_id}, v{self.version})"
        )
