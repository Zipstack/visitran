"""Draft service — merged from draft_write_session + user_draft_session.

Handles creating, fetching, saving, and discarding user drafts.
"""

import logging
from typing import Any

from django.db import transaction
from django.utils import timezone

from backend.core.models.config_models import ConfigModels
from backend.core.models.project_details import ProjectDetails
from backend.core.models.user_draft import UserDraft
from backend.core.models.model_version import ModelVersion
from backend.errors.exceptions import ModelNotExists
from backend.utils.pagination import CustomPaginator
from backend.utils.tenant_context import get_current_user

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Write operations
# ------------------------------------------------------------------

def get_or_create_draft(
    config_model: ConfigModels,
    user_info: dict | None = None,
) -> UserDraft:
    """Get the user's existing draft or create one from live model_data."""
    if not user_info:
        user_info = get_current_user()
    owner_id = user_info.get("username", "")

    draft = UserDraft.objects.filter(
        owner_id=owner_id, config_model=config_model,
    ).first()
    if draft is not None:
        return draft

    # Get base version number from latest model-scoped version
    latest = (
        ModelVersion.objects.filter(config_model=config_model)
        .order_by("-version_number").first()
    )
    base_version = latest.version_number if latest else 0

    draft = UserDraft(
        config_model=config_model,
        owner_id=owner_id,
        owner=user_info,
        base_version_number=base_version,
        draft_data=config_model.model_data or {},
        version=1,
        is_dirty=False,
    )
    draft.save()
    logger.info("Created new draft for user=%s model=%s", owner_id, config_model.model_name)
    return draft


def save_draft_data(
    draft: UserDraft,
    model_data: dict[str, Any],
) -> UserDraft:
    """Save model_data to draft, set is_dirty=True, bump version."""
    with transaction.atomic():
        draft.draft_data = model_data
        draft.is_dirty = True
        draft.version += 1
        draft.save(update_fields=["draft_data", "is_dirty", "version", "modified_at"])

    logger.info("Saved draft for model=%s user=%s (v%d, dirty=True)",
                draft.config_model.model_name, draft.owner_id, draft.version)

    # Queue event-based auto-save
    try:
        from backend.core.scheduler.version_celery_tasks import auto_save_single_draft
        auto_save_single_draft.delay(str(draft.draft_id))
    except Exception:
        logger.debug("Could not queue event auto-save; periodic task will handle it.")

    return draft


def fetch_draft_model_data(
    project_instance: ProjectDetails,
    model_name: str,
    user_info: dict | None = None,
) -> dict[str, Any]:
    """Get model_data from user's draft, or fall back to live ConfigModels."""
    if not user_info:
        user_info = get_current_user()
    owner_id = user_info.get("username", "")

    config_model = ConfigModels.objects.filter(
        project_instance=project_instance, model_name=model_name,
    ).first()
    if config_model is None:
        raise ModelNotExists(model_name=model_name)

    draft = UserDraft.objects.filter(owner_id=owner_id, config_model=config_model).first()
    if draft is not None:
        return draft.draft_data or {}
    return config_model.model_data or {}


# ------------------------------------------------------------------
# Read operations
# ------------------------------------------------------------------

def get_user_drafts(
    project_instance: ProjectDetails,
    page: int = 1,
    limit: int = 10,
    user_info: dict | None = None,
) -> dict[str, Any]:
    if not user_info:
        user_info = get_current_user()
    owner_id = user_info.get("username", "")

    config_model_ids = ConfigModels.objects.filter(
        project_instance=project_instance,
    ).values_list("model_id", flat=True)

    queryset = UserDraft.objects.filter(
        owner_id=owner_id, config_model_id__in=config_model_ids,
    ).select_related("config_model").order_by("-modified_at")

    paginator = CustomPaginator(queryset=queryset, limit=limit, page=page)
    result = paginator.paginate()
    result["page_items"] = [_serialize_draft(d) for d in result["page_items"]]
    return result


def get_draft(
    config_model: ConfigModels,
    user_info: dict | None = None,
) -> UserDraft | None:
    if not user_info:
        user_info = get_current_user()
    owner_id = user_info.get("username", "")
    return UserDraft.objects.filter(owner_id=owner_id, config_model=config_model).first()


def get_draft_or_published(
    project_instance: ProjectDetails,
    model_name: str,
    user_info: dict | None = None,
) -> dict[str, Any]:
    if not user_info:
        user_info = get_current_user()
    owner_id = user_info.get("username", "")

    config_model = ConfigModels.objects.filter(
        project_instance=project_instance, model_name=model_name,
    ).first()
    if config_model is None:
        raise ModelNotExists(model_name=model_name)

    draft = UserDraft.objects.filter(owner_id=owner_id, config_model=config_model).first()

    if draft is not None:
        return {
            "draft_exists": True,
            "model_data": draft.draft_data or {},
            "draft_id": str(draft.draft_id),
            "model_name": config_model.model_name,
            "model_id": str(config_model.model_id),
            "base_version_number": draft.base_version_number,
            "version": draft.version,
            "lock_token": str(draft.lock_token),
            "is_dirty": draft.is_dirty,
            "last_auto_save": draft.last_auto_save.isoformat() if draft.last_auto_save else None,
            "is_locked": draft.is_locked(),
            "is_stale": draft.is_stale(),
            "created_at": draft.created_at.isoformat(),
            "modified_at": draft.modified_at.isoformat(),
        }

    return {
        "draft_exists": False,
        "model_data": config_model.model_data or {},
        "draft_id": None,
        "model_name": config_model.model_name,
        "model_id": str(config_model.model_id),
        "base_version_number": 0,
        "version": 0,
        "is_dirty": False,
        "last_auto_save": None,
        "is_locked": False,
        "is_stale": False,
        "created_at": config_model.created_at.isoformat(),
        "modified_at": config_model.modified_at.isoformat(),
    }


def discard_draft(
    project_instance: ProjectDetails,
    model_name: str,
    user_info: dict | None = None,
) -> dict[str, Any]:
    if not user_info:
        user_info = get_current_user()
    owner_id = user_info.get("username", "")

    config_model = ConfigModels.objects.filter(
        project_instance=project_instance, model_name=model_name,
    ).first()
    if config_model is None:
        raise ModelNotExists(model_name=model_name)

    deleted_count, _ = UserDraft.objects.filter(
        owner_id=owner_id, config_model=config_model,
    ).delete()
    return {
        "deleted": deleted_count > 0,
        "model_name": config_model.model_name,
        "model_id": str(config_model.model_id),
    }


# ------------------------------------------------------------------
# Serialization
# ------------------------------------------------------------------

def _serialize_draft(draft: UserDraft) -> dict[str, Any]:
    return {
        "draft_id": str(draft.draft_id),
        "model_name": draft.config_model.model_name,
        "model_id": str(draft.config_model.model_id),
        "base_version_number": draft.base_version_number,
        "version": draft.version,
        "lock_token": str(draft.lock_token),
        "is_dirty": draft.is_dirty,
        "last_auto_save": draft.last_auto_save.isoformat() if draft.last_auto_save else None,
        "is_locked": draft.is_locked(),
        "is_stale": draft.is_stale(),
        "created_at": draft.created_at.isoformat(),
        "modified_at": draft.modified_at.isoformat(),
    }
