"""Conflict resolution service.

Handles resolution of transformation-level conflicts. Supports:
  - accepted  (keep draft / "mine")
  - rejected  (keep published / "theirs")
  - merged    (manual merge with user-supplied data)

After all conflicts are resolved, finalize_resolutions builds
a merged model_data and commits a new version atomically.
"""

import logging
from typing import Any

from django.db import transaction
from django.db.models import Max
from django.utils import timezone

from backend.core.models.config_models import ConfigModels
from backend.core.models.model_version import ModelVersion
from backend.core.models.project_details import ProjectDetails
from backend.core.models.transformation_conflict import TransformationConflict
from backend.core.models.user_draft import UserDraft
from backend.errors.exceptions import (
    CommitFailedException,
    VersionNotFoundException,
)
from backend.utils.tenant_context import get_current_user

logger = logging.getLogger(__name__)


def resolve_conflict(
    conflict_id: str,
    strategy: str,
    resolved_data: dict | None = None,
    user_info: dict | None = None,
) -> dict[str, Any]:
    """Resolve a single conflict record."""
    try:
        conflict = TransformationConflict.objects.get(conflict_id=conflict_id)
    except TransformationConflict.DoesNotExist:
        raise VersionNotFoundException(version_number=0)

    if strategy == TransformationConflict.RESOLUTION_ACCEPTED:
        final_data = conflict.draft_transformation
    elif strategy == TransformationConflict.RESOLUTION_REJECTED:
        final_data = conflict.published_transformation
    elif strategy == TransformationConflict.RESOLUTION_MERGED:
        final_data = resolved_data or {}
    else:
        raise CommitFailedException(model_name="unknown")

    resolver = user_info or get_current_user()

    conflict.resolution_status = strategy
    conflict.resolved_data = final_data
    conflict.resolved_by = resolver
    conflict.resolved_at = timezone.now()
    conflict.save(update_fields=["resolution_status", "resolved_data", "resolved_by", "resolved_at"])

    logger.info("Resolved conflict %s (%s) with strategy=%s", conflict_id, conflict.transformation_path, strategy)
    return _serialize_resolved_conflict(conflict)


def finalize_resolutions(
    config_model: ConfigModels,
    draft: UserDraft,
    project_instance: ProjectDetails,
    commit_message: str = "",
    user_info: dict | None = None,
) -> dict[str, Any]:
    """Build merged model_data from resolved conflicts and commit."""
    pending = TransformationConflict.objects.filter(
        draft=draft, resolution_status=TransformationConflict.RESOLUTION_PENDING,
    ).count()
    if pending > 0:
        raise CommitFailedException(model_name=config_model.model_name)

    resolved_conflicts = TransformationConflict.objects.filter(
        draft=draft,
    ).exclude(resolution_status=TransformationConflict.RESOLUTION_PENDING)

    merged_data = dict(draft.draft_data or {})
    merged_transforms = dict(merged_data.get("transform", {}))
    for conflict in resolved_conflicts:
        merged_transforms[conflict.transformation_path] = conflict.resolved_data
    merged_data["transform"] = merged_transforms

    committer = user_info or get_current_user()

    try:
        with transaction.atomic():
            next_version = (
                ModelVersion.objects.filter(config_model=config_model)
                .aggregate(max_v=Max("version_number"))
            )["max_v"] or 0
            next_version += 1

            version = ModelVersion.objects.create(
                config_model=config_model,
                project_instance=project_instance,
                version_number=next_version,
                model_data=merged_data,
                commit_message=commit_message or "Merged conflict resolutions",
                committed_by=committer,
                user_name_snapshot=committer.get("name", ""),
                user_email_snapshot=committer.get("username", ""),
            )

            config_model.model_data = merged_data
            config_model.save(update_fields=["model_data"])

            TransformationConflict.objects.filter(draft=draft).delete()
            draft.delete()

            logger.info(
                "Finalized %d conflict resolution(s) -> version %d for model %s",
                resolved_conflicts.count(), version.version_number, config_model.model_name,
            )
            return _serialize_version_brief(version)

    except Exception:
        logger.exception("Failed to finalize conflict resolutions for model %s", config_model.model_name)
        raise CommitFailedException(model_name=config_model.model_name)


def get_resolution_preview(draft: UserDraft) -> dict[str, Any]:
    """Preview the merged model_data without committing."""
    conflicts = TransformationConflict.objects.filter(draft=draft).order_by("transformation_path")
    merged_data = dict(draft.draft_data or {})
    merged_transforms = dict(merged_data.get("transform", {}))
    pending_count = 0
    resolved_count = 0
    for conflict in conflicts:
        tid = conflict.transformation_path
        if conflict.resolution_status == TransformationConflict.RESOLUTION_PENDING:
            merged_transforms[tid] = conflict.draft_transformation
            pending_count += 1
        else:
            merged_transforms[tid] = conflict.resolved_data
            resolved_count += 1
    merged_data["transform"] = merged_transforms
    return {
        "merged_data": merged_data,
        "pending_conflicts": pending_count,
        "resolved_conflicts": resolved_count,
        "is_ready_to_finalize": pending_count == 0,
    }


def _serialize_resolved_conflict(conflict: TransformationConflict) -> dict[str, Any]:
    return {
        "conflict_id": str(conflict.conflict_id),
        "transformation_path": conflict.transformation_path,
        "resolution_status": conflict.resolution_status,
        "resolved_data": conflict.resolved_data,
        "resolved_by": conflict.resolved_by,
        "resolved_at": conflict.resolved_at.isoformat() if conflict.resolved_at else None,
    }


def _serialize_version_brief(version: ModelVersion) -> dict[str, Any]:
    return {
        "version_id": str(version.version_id),
        "version_number": version.version_number,
        "commit_message": version.commit_message,
        "created_at": version.created_at.isoformat(),
    }
