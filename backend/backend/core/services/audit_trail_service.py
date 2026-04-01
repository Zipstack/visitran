"""Audit trail service — event logging + query layer.

Merges the old audit_trail_service (write) and audit_trail_session (read)
into a single flat service module.
"""

import csv
import io
import logging
from typing import Any

from django.db import transaction
from django.db.models import QuerySet

from backend.core.models.config_models import ConfigModels
from backend.core.models.model_version import ModelVersion
from backend.core.models.project_details import ProjectDetails
from backend.core.models.version_audit_event import VersionAuditEvent
from backend.utils.pagination import CustomPaginator
from backend.utils.tenant_context import get_current_user

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# User attribution snapshot
# ------------------------------------------------------------------

def _build_user_snapshot(user_info: dict | None = None) -> dict[str, str]:
    """Capture immutable user context at event time."""
    if not user_info:
        user_info = get_current_user()
    return {
        "user_name": user_info.get("name", ""),
        "user_email": user_info.get("username", ""),
        "user_role": "",
    }


# ------------------------------------------------------------------
# Core event logging
# ------------------------------------------------------------------

def log_event(
    event_type: str,
    project_instance: ProjectDetails,
    config_model: ConfigModels | None = None,
    version: ModelVersion | None = None,
    version_number: int | None = None,
    user_info: dict | None = None,
    commit_message: str = "",
    changes_summary: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Log a version control event with user attribution.

    Uses transaction.on_commit so the audit record is only created
    after the surrounding DB transaction succeeds.
    """
    snapshot = _build_user_snapshot(user_info)
    actor = user_info or get_current_user()

    def _create_event():
        try:
            VersionAuditEvent.objects.create(
                event_type=event_type,
                config_model=config_model,
                project_instance=project_instance,
                version=version,
                version_number=version_number,
                actor=actor,
                user_name_snapshot=snapshot["user_name"],
                user_email_snapshot=snapshot["user_email"],
                user_role_snapshot=snapshot["user_role"],
                commit_message=commit_message,
                changes_summary=changes_summary or {},
                metadata=metadata or {},
            )
            label = config_model.model_name if config_model else "project"
            logger.debug("Audit event: %s for %s by %s", event_type, label, snapshot["user_name"] or "system")
        except Exception:
            label = config_model.model_name if config_model else "project"
            logger.exception("Failed to create audit event %s for %s", event_type, label)

    transaction.on_commit(_create_event)


# ------------------------------------------------------------------
# Convenience helpers
# ------------------------------------------------------------------

def log_version_committed(
    project_instance: ProjectDetails,
    version: ModelVersion,
    config_model: ConfigModels | None = None,
    user_info: dict | None = None,
    commit_message: str = "",
    is_auto_commit: bool = False,
) -> None:
    log_event(
        event_type=VersionAuditEvent.EventType.VERSION_COMMITTED,
        config_model=config_model, project_instance=project_instance,
        version=version, version_number=version.version_number, user_info=user_info,
        commit_message=commit_message,
        metadata={"is_auto_commit": is_auto_commit, "content_hash": version.content_hash},
    )


def log_version_rolled_back(
    project_instance: ProjectDetails,
    version: ModelVersion,
    config_model: ConfigModels | None = None,
    user_info: dict | None = None,
    rollback_metadata: dict[str, Any] | None = None,
) -> None:
    log_event(
        event_type=VersionAuditEvent.EventType.VERSION_ROLLED_BACK,
        config_model=config_model, project_instance=project_instance,
        version=version, version_number=version.version_number, user_info=user_info,
        commit_message=version.commit_message,
        metadata=rollback_metadata or version.rollback_metadata or {},
    )


def log_draft_saved(
    project_instance: ProjectDetails,
    config_model: ConfigModels,
    user_info: dict | None = None,
) -> None:
    log_event(
        event_type=VersionAuditEvent.EventType.DRAFT_SAVED,
        config_model=config_model, project_instance=project_instance, user_info=user_info,
    )


def log_draft_discarded(
    project_instance: ProjectDetails,
    config_model: ConfigModels,
    user_info: dict | None = None,
) -> None:
    log_event(
        event_type=VersionAuditEvent.EventType.DRAFT_DISCARDED,
        config_model=config_model, project_instance=project_instance, user_info=user_info,
    )


def log_conflict_resolved(
    project_instance: ProjectDetails,
    config_model: ConfigModels,
    user_info: dict | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    log_event(
        event_type=VersionAuditEvent.EventType.CONFLICT_RESOLVED,
        config_model=config_model, project_instance=project_instance,
        user_info=user_info, metadata=metadata,
    )


def log_conflict_finalized(
    project_instance: ProjectDetails,
    config_model: ConfigModels,
    version: ModelVersion,
    user_info: dict | None = None,
    commit_message: str = "",
) -> None:
    log_event(
        event_type=VersionAuditEvent.EventType.CONFLICT_FINALIZED,
        config_model=config_model, project_instance=project_instance,
        version=version, version_number=version.version_number,
        user_info=user_info, commit_message=commit_message,
    )


# ------------------------------------------------------------------
# Query layer (merged from audit_trail_session)
# ------------------------------------------------------------------

def get_events(
    project_instance: ProjectDetails,
    page: int = 1,
    limit: int = 20,
    model_id: str | None = None,
    event_type: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    qs = _base_queryset(project_instance)
    qs = _apply_filters(qs, model_id, event_type, start_date, end_date)
    paginator = CustomPaginator(queryset=qs, limit=limit, page=page)
    result = paginator.paginate()
    result["page_items"] = [_serialize_event(e) for e in result["page_items"]]
    return result


def get_model_events(
    project_instance: ProjectDetails,
    model_id: str,
    page: int = 1,
    limit: int = 20,
    event_type: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    qs = _base_queryset(project_instance).filter(config_model_id=model_id)
    qs = _apply_filters(qs, None, event_type, start_date, end_date)
    paginator = CustomPaginator(queryset=qs, limit=limit, page=page)
    result = paginator.paginate()
    result["page_items"] = [_serialize_event(e) for e in result["page_items"]]
    return result


def export_events_csv(
    project_instance: ProjectDetails,
    model_id: str | None = None,
    event_type: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> str:
    qs = _base_queryset(project_instance)
    qs = _apply_filters(qs, model_id, event_type, start_date, end_date)
    qs = qs[:10000]
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "event_id", "event_type", "model_name", "version_number",
        "user_name", "user_email", "user_role", "commit_message", "timestamp",
    ])
    for event in qs.select_related("config_model"):
        writer.writerow([
            str(event.event_id), event.event_type,
            event.config_model.model_name if event.config_model else "",
            event.version_number or "", event.user_name_snapshot,
            event.user_email_snapshot, event.user_role_snapshot,
            event.commit_message, event.created_at.isoformat(),
        ])
    return output.getvalue()


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

def _base_queryset(project_instance: ProjectDetails) -> QuerySet:
    return VersionAuditEvent.objects.filter(
        project_instance=project_instance,
    ).order_by("-created_at")


def _apply_filters(
    qs: QuerySet,
    model_id: str | None,
    event_type: str | None,
    start_date: str | None,
    end_date: str | None,
) -> QuerySet:
    if model_id:
        qs = qs.filter(config_model_id=model_id)
    if event_type:
        qs = qs.filter(event_type=event_type)
    if start_date:
        qs = qs.filter(created_at__gte=start_date)
    if end_date:
        qs = qs.filter(created_at__lte=end_date)
    return qs


def _serialize_event(event: VersionAuditEvent) -> dict[str, Any]:
    return {
        "event_id": str(event.event_id),
        "event_type": event.event_type,
        "config_model_id": str(event.config_model_id) if event.config_model_id else None,
        "version_number": event.version_number,
        "version_id": str(event.version_id) if event.version_id else None,
        "user": {
            "name": event.user_name_snapshot,
            "email": event.user_email_snapshot,
            "role": event.user_role_snapshot,
        },
        "commit_message": event.commit_message,
        "changes_summary": event.changes_summary,
        "metadata": event.metadata,
        "created_at": event.created_at.isoformat(),
    }
