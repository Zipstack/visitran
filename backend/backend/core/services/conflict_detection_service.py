"""Conflict detection engine.

Detects transformation-level conflicts when a user's draft is based on
a version that has been superseded by another commit.
"""

import logging
import time
from typing import Any

from django.db import transaction

from backend.core.models.config_models import ConfigModels
from backend.core.models.model_version import ModelVersion
from backend.core.models.transformation_conflict import TransformationConflict
from backend.core.models.user_draft import UserDraft

logger = logging.getLogger(__name__)


def detect_conflicts(
    config_model: ConfigModels,
    draft: UserDraft,
) -> dict[str, Any]:
    """Run conflict detection for a draft against the current published state."""
    start = time.monotonic()
    draft_base = draft.base_version_number or 0

    # Get current max version for this model
    latest = (
        ModelVersion.objects.filter(config_model=config_model)
        .order_by("-version_number")
        .first()
    )
    current_version = latest.version_number if latest else 0

    if draft_base >= current_version:
        return _no_conflict_result(start)

    published_version = latest
    if published_version is None:
        return _no_conflict_result(start)

    draft_data = draft.draft_data or {}
    published_data = published_version.model_data or {}
    draft_transforms = draft_data.get("transform", {})
    published_transforms = published_data.get("transform", {})

    base_transforms = {}
    if draft_base > 0:
        base_version = ModelVersion.objects.filter(
            config_model=config_model, version_number=draft_base,
        ).first()
        if base_version:
            base_transforms = (base_version.model_data or {}).get("transform", {})

    conflicts_data = _find_overlapping_modifications(
        base_transforms=base_transforms,
        draft_transforms=draft_transforms,
        published_transforms=published_transforms,
    )

    if not conflicts_data:
        return _no_conflict_result(start)

    conflict_ids = _create_conflict_records(
        config_model=config_model, draft=draft,
        published_version=published_version, conflicts_data=conflicts_data,
    )

    elapsed_ms = round((time.monotonic() - start) * 1000, 2)
    logger.info(
        "Detected %d conflict(s) for model %s (draft base v%d vs published v%d)",
        len(conflict_ids), config_model.model_name, draft_base, current_version,
    )
    return {
        "conflict_exists": True,
        "conflict_count": len(conflict_ids),
        "conflict_ids": conflict_ids,
        "base_version": draft_base,
        "current_version": current_version,
        "detection_time_ms": elapsed_ms,
    }


def get_conflicts_for_draft(draft: UserDraft) -> list[dict[str, Any]]:
    conflicts = TransformationConflict.objects.filter(
        draft=draft, resolution_status=TransformationConflict.RESOLUTION_PENDING,
    ).order_by("transformation_path")
    return [_serialize_conflict(c) for c in conflicts]


def _find_overlapping_modifications(
    base_transforms: dict, draft_transforms: dict, published_transforms: dict,
) -> list[dict[str, Any]]:
    conflicts = []
    common_keys = set(draft_transforms.keys()) & set(published_transforms.keys())
    for tid in sorted(common_keys):
        draft_t = draft_transforms[tid]
        published_t = published_transforms[tid]
        if draft_t == published_t:
            continue
        base_t = base_transforms.get(tid)
        if (draft_t != base_t) and (published_t != base_t):
            conflicts.append({
                "transformation_path": tid,
                "draft_transformation": draft_t,
                "published_transformation": published_t,
            })
    return conflicts


def _create_conflict_records(
    config_model: ConfigModels, draft: UserDraft,
    published_version: ModelVersion, conflicts_data: list[dict[str, Any]],
) -> list[str]:
    records = [
        TransformationConflict(
            config_model=config_model, draft=draft,
            draft_version_number=draft.base_version_number or 0,
            published_version=published_version,
            transformation_path=cd["transformation_path"],
            draft_transformation=cd["draft_transformation"],
            published_transformation=cd["published_transformation"],
        )
        for cd in conflicts_data
    ]
    with transaction.atomic():
        TransformationConflict.objects.filter(
            draft=draft, resolution_status=TransformationConflict.RESOLUTION_PENDING,
        ).delete()
        TransformationConflict.objects.bulk_create(records)
    return [str(r.conflict_id) for r in records]


def _serialize_conflict(conflict: TransformationConflict) -> dict[str, Any]:
    return {
        "conflict_id": str(conflict.conflict_id),
        "transformation_path": conflict.transformation_path,
        "draft_transformation": conflict.draft_transformation,
        "published_transformation": conflict.published_transformation,
        "resolution_status": conflict.resolution_status,
        "draft_version_number": conflict.draft_version_number,
        "published_version_number": conflict.published_version.version_number,
        "created_at": conflict.created_at.isoformat(),
    }


def _no_conflict_result(start: float) -> dict[str, Any]:
    return {
        "conflict_exists": False,
        "conflict_count": 0,
        "conflict_ids": [],
        "detection_time_ms": round((time.monotonic() - start) * 1000, 2),
    }
