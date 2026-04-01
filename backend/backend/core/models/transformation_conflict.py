import uuid

from django.db import models

from backend.core.models.config_models import ConfigModels
from backend.core.models.model_version import ModelVersion
from backend.core.models.user_draft import UserDraft
from utils.models.base_model import BaseModel
from utils.models.organization_mixin import (
    DefaultOrganizationManagerMixin,
    DefaultOrganizationMixin,
)


class TransformationConflictManager(
    DefaultOrganizationManagerMixin, models.Manager
):
    pass


class TransformationConflict(DefaultOrganizationMixin, BaseModel):
    """Records a transformation-level conflict between a draft and
    the current published version.

    Created by the conflict detection engine when a user's draft is
    based on a version that has since been superseded by another commit.
    """

    RESOLUTION_PENDING = "pending"
    RESOLUTION_ACCEPTED = "accepted"
    RESOLUTION_REJECTED = "rejected"
    RESOLUTION_MERGED = "merged"

    RESOLUTION_CHOICES = [
        (RESOLUTION_PENDING, "Pending"),
        (RESOLUTION_ACCEPTED, "Accepted (Mine)"),
        (RESOLUTION_REJECTED, "Rejected (Theirs)"),
        (RESOLUTION_MERGED, "Merged"),
    ]

    conflict_id = models.UUIDField(
        primary_key=True, default=uuid.uuid4, editable=False
    )
    config_model = models.ForeignKey(
        ConfigModels,
        on_delete=models.CASCADE,
        related_name="conflicts",
    )
    draft = models.ForeignKey(
        UserDraft,
        on_delete=models.CASCADE,
        related_name="conflicts",
    )
    draft_version_number = models.PositiveIntegerField()
    published_version = models.ForeignKey(
        ModelVersion,
        on_delete=models.CASCADE,
        related_name="conflicts",
    )

    # Transformation path identifier (e.g. "type_join_0")
    transformation_path = models.CharField(max_length=300)

    # Snapshots of the conflicting transformation data
    draft_transformation = models.JSONField(default=dict)
    published_transformation = models.JSONField(default=dict)

    # Resolution tracking
    resolution_status = models.CharField(
        max_length=20,
        choices=RESOLUTION_CHOICES,
        default=RESOLUTION_PENDING,
    )
    resolved_data = models.JSONField(default=dict, blank=True)
    resolved_by = models.JSONField(default=dict)
    resolved_at = models.DateTimeField(null=True, blank=True)

    objects = TransformationConflictManager()

    class Meta:
        indexes = [
            models.Index(
                fields=["config_model", "draft"],
                name="idx_conflict_model_draft",
            ),
            models.Index(
                fields=["resolution_status"],
                name="idx_conflict_status",
            ),
        ]

    def __str__(self):
        return (
            f"Conflict({self.transformation_path}, "
            f"draft_v{self.draft_version_number} vs "
            f"published_v{self.published_version.version_number})"
        )
