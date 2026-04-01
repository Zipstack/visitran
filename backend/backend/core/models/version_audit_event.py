import uuid

from django.db import models

from backend.core.models.config_models import ConfigModels
from backend.core.models.model_version import ModelVersion
from backend.core.models.project_details import ProjectDetails
from utils.models.base_model import BaseModel
from utils.models.organization_mixin import (
    DefaultOrganizationManagerMixin,
    DefaultOrganizationMixin,
)


class VersionAuditEventManager(
    DefaultOrganizationManagerMixin, models.Manager
):
    pass


class VersionAuditEvent(DefaultOrganizationMixin, BaseModel):
    """Captures version control operations for compliance audit trail.

    Each event records what happened, who did it (immutable snapshot),
    and contextual metadata. Events are append-only.
    """

    class EventType(models.TextChoices):
        VERSION_COMMITTED = "version_committed", "Version Committed"
        VERSION_ROLLED_BACK = "version_rolled_back", "Version Rolled Back"
        DRAFT_SAVED = "draft_saved", "Draft Saved"
        DRAFT_DISCARDED = "draft_discarded", "Draft Discarded"
        CONFLICT_RESOLVED = "conflict_resolved", "Conflict Resolved"
        CONFLICT_FINALIZED = "conflict_finalized", "Conflict Finalized"
        VERSION_VIEWED = "version_viewed", "Version Viewed"

    event_id = models.UUIDField(
        primary_key=True, default=uuid.uuid4, editable=False
    )
    event_type = models.CharField(
        max_length=30,
        choices=EventType.choices,
        db_index=True,
    )

    # Related entities
    config_model = models.ForeignKey(
        ConfigModels,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="audit_events",
    )
    project_instance = models.ForeignKey(
        ProjectDetails,
        on_delete=models.CASCADE,
        related_name="audit_events",
    )
    version = models.ForeignKey(
        ModelVersion,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="audit_events",
    )
    version_number = models.PositiveIntegerField(null=True, blank=True)

    # User attribution
    actor = models.JSONField(default=dict)

    # Immutable user attribution snapshots
    user_name_snapshot = models.TextField(blank=True, default="")
    user_email_snapshot = models.TextField(blank=True, default="")
    user_role_snapshot = models.TextField(blank=True, default="")

    # Event details
    commit_message = models.CharField(max_length=500, blank=True, default="")
    changes_summary = models.JSONField(default=dict, blank=True)
    metadata = models.JSONField(default=dict, blank=True)

    objects = VersionAuditEventManager()

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(
                fields=["config_model", "-created_at"],
                name="idx_audit_model_timeline",
            ),
            models.Index(
                fields=["project_instance", "event_type", "-created_at"],
                name="idx_audit_project_events",
            ),
            models.Index(
                fields=["organization", "project_instance", "-created_at"],
                name="idx_audit_org_isolation",
            ),
        ]

    def __str__(self):
        target = self.config_model_id or "project"
        return (
            f"{self.event_type} on {target} "
            f"by {self.user_name_snapshot or 'system'}"
        )
