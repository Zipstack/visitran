"""Version history API views — function-based with @api_view + @handle_http_request."""

import logging

from django.http import HttpResponse
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response

from backend.core.models.config_models import ConfigModels
from backend.core.models.model_version import ModelVersion
from backend.core.models.project_details import ProjectDetails
from backend.core.routers.version_history.serializers import (
    CommitFromDraftSerializer,
    CommitProjectSerializer,
    FinalizeResolutionsSerializer,
    ResolveConflictSerializer,
    RetryGitSyncSerializer,
    RollbackSerializer,
)
from backend.core.services import audit_trail_service
from backend.core.services import conflict_detection_service
from backend.core.services import conflict_resolution_service
from backend.core.services import draft_service
from backend.core.services import model_version_service
from backend.core.services import rollback_validation_service
from backend.core.utils import handle_http_request
from backend.utils.constants import HTTPMethods

logger = logging.getLogger(__name__)

RESOURCE_NAME = "versionhistory"


def _get_project(project_id: str) -> ProjectDetails:
    return ProjectDetails.objects.get(project_uuid=project_id)


# ==================================================================
# Project-level version control
# ==================================================================


@api_view([HTTPMethods.POST])
@handle_http_request
def commit_project(request: Request, project_id: str) -> Response:
    """Create a project-level version snapshot of ALL models."""
    serializer = CommitProjectSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)

    project_instance = _get_project(project_id)
    data = model_version_service.commit_project(
        project_instance=project_instance,
        commit_message=serializer.validated_data["commit_message"],
    )
    return Response(data={"status": "success", "data": data}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def preview_pending_changes(request: Request, project_id: str) -> Response:
    """Preview per-model YAML diffs of uncommitted changes."""
    project_instance = _get_project(project_id)
    data = model_version_service.preview_pending_changes(project_instance)
    return Response(data={"status": "success", "data": data}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_version_history(request: Request, project_id: str) -> Response:
    """Get paginated project-level version history."""
    page = int(request.GET.get("page", 1))
    limit = int(request.GET.get("limit", 10))

    project_instance = _get_project(project_id)
    data = model_version_service.get_versions(project_instance, page=page, limit=limit)
    return Response(data={"status": "success", "data": data}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_version_detail(request: Request, project_id: str, version_number: int) -> Response:
    """Get full details of a specific version by version number."""
    project_instance = _get_project(project_id)
    data = model_version_service.get_version_detail(project_instance, version_number=version_number)
    return Response(data={"status": "success", "data": data}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_version_by_id(request: Request, project_id: str, version_id: str) -> Response:
    """Get full details of a specific version by UUID."""
    project_instance = _get_project(project_id)
    data = model_version_service.get_version_detail(project_instance, version_id=version_id)
    return Response(data={"status": "success", "data": data}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_current_version(request: Request, project_id: str) -> Response:
    """Get the version marked as current, falling back to latest."""
    project_instance = _get_project(project_id)
    current = ModelVersion.objects.filter(
        project_instance=project_instance, config_model__isnull=True, is_current=True,
    ).first()
    if not current:
        current = model_version_service.get_latest_version(project_instance)
    if not current:
        return Response(
            data={"status": "success", "data": None},
            status=status.HTTP_200_OK,
        )
    data = model_version_service.serialize_version_detail(current)
    return Response(data={"status": "success", "data": data}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def compare_versions(request: Request, project_id: str) -> Response:
    """Compare two project-level versions. Query params: version_a, version_b."""
    version_a = request.GET.get("version_a")
    version_b = request.GET.get("version_b")
    if version_a is None or version_b is None:
        return Response(
            data={"status": "failed", "error_message": "version_a and version_b are required"},
            status=status.HTTP_400_BAD_REQUEST,
        )
    project_instance = _get_project(project_id)
    data = model_version_service.compare_versions(project_instance, int(version_a), int(version_b))
    return Response(data={"status": "success", "data": data}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST])
@handle_http_request
def rollback_to_version(request: Request, project_id: str) -> Response:
    """Execute a rollback to a specific version."""
    serializer = RollbackSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)

    project_instance = _get_project(project_id)
    data = model_version_service.rollback_to_version(
        project_instance=project_instance,
        version_number=serializer.validated_data["version_number"],
        reason=serializer.validated_data["reason"],
    )
    return Response(data={"status": "success", "data": data}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST])
@handle_http_request
def retry_git_sync(request: Request, project_id: str) -> Response:
    """Manually retry git sync for a version with failed status."""
    serializer = RetryGitSyncSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)
    version_id = str(serializer.validated_data["version_id"])

    try:
        version = ModelVersion.objects.get(version_id=version_id, project_instance_id=project_id)
    except ModelVersion.DoesNotExist:
        return Response(
            data={"status": "failed", "error_message": "Version not found for this project"},
            status=status.HTTP_404_NOT_FOUND,
        )
    if version.git_sync_status != "failed":
        return Response(
            data={"status": "failed", "error_message": f"Cannot retry — sync status is '{version.git_sync_status}', expected 'failed'"},
            status=status.HTTP_400_BAD_REQUEST,
        )
    model_version_service._sync_to_git(version)
    return Response(
        data={"status": "success", "data": {"message": "Git sync completed", "git_sync_status": version.git_sync_status}},
        status=status.HTTP_200_OK,
    )


@api_view([HTTPMethods.GET])
@handle_http_request
def get_draft_status(request: Request, project_id: str) -> Response:
    """Return draft status summary for the current user in this project."""
    from backend.core.models.user_draft import UserDraft
    from backend.utils.tenant_context import get_current_user

    project_instance = _get_project(project_id)
    user_info = get_current_user()
    owner_id = user_info.get("username", "")

    config_model_ids = ConfigModels.objects.filter(
        project_instance=project_instance,
    ).values_list("model_id", flat=True)

    drafts = UserDraft.objects.filter(
        owner_id=owner_id, config_model_id__in=config_model_ids,
    ).select_related("config_model")

    # Only flag models where draft content actually differs from the
    # latest committed version — avoids false positives after a commit.
    models_with_drafts = []
    for d in drafts:
        latest_version = ModelVersion.objects.filter(
            project_instance=d.config_model.project_instance,
            config_model=None,
        ).order_by("-version_number").first()
        committed_data = (
            latest_version.model_data.get(d.config_model.model_name, {})
            if latest_version else {}
        )
        if d.draft_data != committed_data:
            models_with_drafts.append(d.config_model.model_name)

    data = {
        "has_draft": len(models_with_drafts) > 0,
        "draft_count": len(models_with_drafts),
        "models_with_drafts": sorted(models_with_drafts),
    }
    return Response(data={"status": "success", "data": data}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def verify_version_integrity(request: Request, project_id: str, version_number: int) -> Response:
    """Verify the integrity of a stored version snapshot."""
    project_instance = _get_project(project_id)
    version = model_version_service.get_version(project_instance, version_number)

    if not version.content_hash:
        data = {"verification_status": "unverified", "stored_hash": "", "computed_hash": "",
                "message": "No content hash stored for this version."}
    else:
        computed_hash = version.compute_content_hash()
        is_valid = computed_hash == version.content_hash
        data = {
            "verification_status": "valid" if is_valid else "invalid",
            "stored_hash": version.content_hash, "computed_hash": computed_hash,
            "message": "Content integrity verified." if is_valid else "Content hash mismatch — possible tampering detected.",
        }
    data["version_number"] = version.version_number
    data["version_id"] = str(version.version_id)
    return Response(data={"status": "success", "data": data}, status=status.HTTP_200_OK)


# ==================================================================
# Model-scoped endpoints (drafts, conflicts)
# ==================================================================


@api_view([HTTPMethods.GET])
@handle_http_request
def list_user_drafts(request: Request, project_id: str) -> Response:
    """List the current user's drafts for a project."""
    page = int(request.GET.get("page", 1))
    limit = int(request.GET.get("limit", 10))
    project_instance = _get_project(project_id)
    data = draft_service.get_user_drafts(project_instance, page=page, limit=limit)
    return Response(data={"status": "success", "data": data}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_draft(request: Request, project_id: str, model_name: str) -> Response:
    """Get the user's draft for a model, or fall back to published."""
    model_name = model_name.replace(" ", "_")
    project_instance = _get_project(project_id)
    data = draft_service.get_draft_or_published(project_instance, model_name=model_name)
    return Response(data={"status": "success", "data": data}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.DELETE])
@handle_http_request
def discard_draft(request: Request, project_id: str, model_name: str) -> Response:
    """Discard (delete) the user's draft for a model. Idempotent."""
    model_name = model_name.replace(" ", "_")
    project_instance = _get_project(project_id)
    data = draft_service.discard_draft(project_instance, model_name=model_name)
    return Response(data={"status": "success", "data": data}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST])
@handle_http_request
def commit_from_draft(request: Request, project_id: str, model_name: str) -> Response:
    """Commit a user's draft as a new immutable version."""
    serializer = CommitFromDraftSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)
    model_name = model_name.replace(" ", "_")

    project_instance = _get_project(project_id)
    config_model = ConfigModels.objects.get(project_instance=project_instance, model_name=model_name)
    draft = draft_service.get_draft(config_model=config_model)

    if draft is None:
        # No draft — fall back to project commit
        data = model_version_service.commit_project(
            project_instance=project_instance,
            commit_message=serializer.validated_data["commit_message"],
        )
    else:
        lock_token = serializer.validated_data.get("lock_token") or None
        data = model_version_service.serialize_version_detail(
            model_version_service.commit_from_draft(
                config_model=config_model, draft=draft, project_instance=project_instance,
                commit_message=serializer.validated_data["commit_message"], lock_token=lock_token,
            )
        )
    return Response(data={"status": "success", "data": data}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST])
@handle_http_request
def check_conflicts(request: Request, project_id: str, model_name: str) -> Response:
    """Detect transformation-level conflicts before committing a draft."""
    model_name = model_name.replace(" ", "_")
    project_instance = _get_project(project_id)
    config_model = ConfigModels.objects.get(project_instance=project_instance, model_name=model_name)
    draft = draft_service.get_draft(config_model=config_model)

    if draft is None:
        return Response(
            data={"status": "success", "data": {"conflict_exists": False, "conflict_count": 0, "conflict_ids": []}},
            status=status.HTTP_200_OK,
        )
    data = conflict_detection_service.detect_conflicts(config_model=config_model, draft=draft)
    return Response(data={"status": "success", "data": data}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_conflicts(request: Request, project_id: str, model_name: str) -> Response:
    """Get pending conflicts for the user's draft."""
    model_name = model_name.replace(" ", "_")
    project_instance = _get_project(project_id)
    config_model = ConfigModels.objects.get(project_instance=project_instance, model_name=model_name)
    draft = draft_service.get_draft(config_model=config_model)

    if draft is None:
        return Response(data={"status": "success", "data": []}, status=status.HTTP_200_OK)
    data = conflict_detection_service.get_conflicts_for_draft(draft=draft)
    return Response(data={"status": "success", "data": data}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST])
@handle_http_request
def resolve_single_conflict(request: Request, project_id: str, model_name: str) -> Response:
    """Resolve a single transformation conflict."""
    serializer = ResolveConflictSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)
    d = serializer.validated_data
    data = conflict_resolution_service.resolve_conflict(
        conflict_id=str(d["conflict_id"]), strategy=d["strategy"], resolved_data=d.get("resolved_data"),
    )
    return Response(data={"status": "success", "data": data}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST])
@handle_http_request
def finalize_conflict_resolutions(request: Request, project_id: str, model_name: str) -> Response:
    """Finalize all resolved conflicts and create a merged version."""
    serializer = FinalizeResolutionsSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)
    model_name = model_name.replace(" ", "_")

    project_instance = _get_project(project_id)
    config_model = ConfigModels.objects.get(project_instance=project_instance, model_name=model_name)
    draft = draft_service.get_draft(config_model=config_model)

    if draft is None:
        return Response(
            data={"status": "failed", "error_message": "No draft found for this model"},
            status=status.HTTP_400_BAD_REQUEST,
        )
    data = conflict_resolution_service.finalize_resolutions(
        config_model=config_model, draft=draft, project_instance=project_instance,
        commit_message=serializer.validated_data["commit_message"],
    )
    return Response(data={"status": "success", "data": data}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def preview_resolution(request: Request, project_id: str, model_name: str) -> Response:
    """Preview the merged model_data from current resolution choices."""
    model_name = model_name.replace(" ", "_")
    project_instance = _get_project(project_id)
    config_model = ConfigModels.objects.get(project_instance=project_instance, model_name=model_name)
    draft = draft_service.get_draft(config_model=config_model)

    if draft is None:
        return Response(
            data={"status": "failed", "error_message": "No draft found for this model"},
            status=status.HTTP_400_BAD_REQUEST,
        )
    data = conflict_resolution_service.get_resolution_preview(draft=draft)
    return Response(data={"status": "success", "data": data}, status=status.HTTP_200_OK)


# ==================================================================
# Audit trail
# ==================================================================


@api_view([HTTPMethods.GET])
@handle_http_request
def get_audit_events(request: Request, project_id: str) -> Response:
    """Query paginated audit trail events with flexible filtering."""
    page = int(request.GET.get("page", 1))
    limit = int(request.GET.get("limit", 20))
    model_id = request.GET.get("model_id")
    event_type = request.GET.get("event_type")
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")
    export_format = request.GET.get("format")

    project_instance = _get_project(project_id)

    if export_format == "csv":
        csv_data = audit_trail_service.export_events_csv(
            project_instance, model_id=model_id, event_type=event_type,
            start_date=start_date, end_date=end_date,
        )
        response = HttpResponse(csv_data, content_type="text/csv")
        response["Content-Disposition"] = 'attachment; filename="audit_trail.csv"'
        return response

    data = audit_trail_service.get_events(
        project_instance, page=page, limit=limit, model_id=model_id,
        event_type=event_type, start_date=start_date, end_date=end_date,
    )
    return Response(data={"status": "success", "data": data}, status=status.HTTP_200_OK)


# ==================================================================
# Previously deferred views (Phase 5)
# ==================================================================


@api_view([HTTPMethods.GET])
@handle_http_request
def validate_rollback(request: Request, project_id: str) -> Response:
    """Validate a rollback before executing it. Query param: version_number."""
    version_number = request.GET.get("version_number")
    if version_number is None:
        return Response(
            data={"status": "failed", "error_message": "version_number query param is required"},
            status=status.HTTP_400_BAD_REQUEST,
        )
    project_instance = _get_project(project_id)
    target_version = model_version_service.get_version(project_instance, int(version_number))

    from backend.application.context.application import ApplicationContext
    app_ctx = ApplicationContext(project_id=project_id)
    model_dict = app_ctx.get_model_references()

    target_data = target_version.model_data or {}
    warnings = []
    can_rollback = True
    for model_name, model_data in target_data.items():
        try:
            config_model = ConfigModels.objects.get(project_instance=project_instance, model_name=model_name)
            result = rollback_validation_service.validate_rollback(
                config_model=config_model, target_version=target_version, model_dict=model_dict,
            )
            if not result.get("can_rollback", True):
                can_rollback = False
            if result.get("issues"):
                warnings.extend([i["message"] for i in result["issues"]])
        except ConfigModels.DoesNotExist:
            warnings.append(f"Model '{model_name}' no longer exists in the project")

    data = {"can_rollback": can_rollback, "warnings": warnings, "target_version": int(version_number), "model_count": len(target_data)}
    return Response(data={"status": "success", "data": data}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def preview_rollback(request: Request, project_id: str) -> Response:
    """Preview a rollback: diff + dependency impact. Query param: version_number."""
    version_number = request.GET.get("version_number")
    if version_number is None:
        return Response(
            data={"status": "failed", "error_message": "version_number query param is required"},
            status=status.HTTP_400_BAD_REQUEST,
        )
    import yaml
    from backend.core.services.version_diff_service import compare_model_data

    project_instance = _get_project(project_id)
    target_version = model_version_service.get_version(project_instance, int(version_number))
    latest = model_version_service.get_latest_version(project_instance)

    diff = {}
    if latest:
        current_data = latest.model_data or {}
        target_data = target_version.model_data or {}
        all_models = sorted(set(list(current_data.keys()) + list(target_data.keys())))
        for m in all_models:
            old = current_data.get(m, {})
            new = target_data.get(m, {})
            if old != new:
                diff[m] = compare_model_data(old_data=old, new_data=new)

    current_yaml = yaml.dump(latest.model_data if latest else {}, indent=4, default_flow_style=False)
    target_yaml = yaml.dump(target_version.model_data or {}, indent=4, default_flow_style=False)

    data = {
        "diff": diff,
        "current_version": latest.version_number if latest else 0,
        "target_version": int(version_number),
        "current_yaml": current_yaml,
        "target_yaml": target_yaml,
    }
    return Response(data={"status": "success", "data": data}, status=status.HTTP_200_OK)


EXECUTE_VERSION_LOCK_TTL = 300  # seconds


@api_view([HTTPMethods.POST])
@handle_http_request
def execute_version(request: Request, project_id: str) -> Response:
    """Execute transformations using a specific version's model_data.

    Concurrency-safe: a per-project Redis lock prevents interleaved
    swap-execute-restore cycles, and select_for_update() serialises
    DB access to the affected ConfigModels rows.

    Long-term fix: refactor execute_visitran_run_command() to accept
    a model_data_override dict so DB writes are not needed at all.
    """
    payload = request.data
    version_number = payload.get("version_number")
    if version_number is None:
        return Response(
            data={"status": "failed", "error_message": "version_number is required"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    from backend.application.context.application import ApplicationContext
    from backend.core.redis_client import RedisClient
    from django.db import transaction
    from visitran.singleton import Singleton

    project_instance = _get_project(project_id)
    target_version = model_version_service.get_version(project_instance, int(version_number))
    target_data = target_version.model_data or {}

    if not target_data:
        return Response(
            data={"status": "failed", "error_message": "Version has no model data to execute."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    environment_id = payload.get("environment", {}).get("id", "")

    # ── Acquire per-project distributed lock ──
    redis = RedisClient()
    lock_key = f"execute_version:{project_id}"
    lock_acquired = False
    if redis.redis_client:
        lock_acquired = redis.redis_client.set(
            lock_key, "1", nx=True, ex=EXECUTE_VERSION_LOCK_TTL,
        )
    else:
        # Redis unavailable — allow execution but log the risk
        lock_acquired = True
        logger.warning("Redis unavailable — execute_version running without distributed lock for project %s", project_id)

    if not lock_acquired:
        return Response(
            data={"status": "failed", "error_message": "Another execution is already in progress for this project. Please wait and try again."},
            status=status.HTTP_409_CONFLICT,
        )

    try:
        return _execute_version_locked(
            project_instance, project_id, target_data,
            version_number, environment_id,
        )
    finally:
        if redis.redis_client:
            redis.delete(lock_key)


def _execute_version_locked(project_instance, project_id, target_data, version_number, environment_id):
    """Inner logic — runs while the Redis lock is held."""
    from backend.application.context.application import ApplicationContext
    from django.db import transaction
    from visitran.singleton import Singleton

    model_names = list(target_data.keys())

    with transaction.atomic():
        # Lock the rows we are about to mutate
        locked_models = list(
            ConfigModels.objects.select_for_update()
            .filter(project_instance=project_instance, model_name__in=model_names)
        )

        # Capture originals from the locked snapshot
        original_data = {cm.model_name: cm.model_data for cm in locked_models}

        succeeded = False
        try:
            # Apply version snapshot
            for cm in locked_models:
                if cm.model_name in target_data:
                    cm.model_data = target_data[cm.model_name]
                    cm.save(update_fields=["model_data"])

            Singleton.reset_cache()
            app = ApplicationContext(project_id=project_id)
            app.execute_visitran_run_command(current_model="", environment_id=environment_id)
            app.visitran_context.close_db_connection()

            model_version_service.set_current_version(project_instance, int(version_number))
            succeeded = True

            result = Response(
                data={"status": "success", "data": {
                    "message": f"Execution of version {version_number} completed successfully.",
                    "version_number": int(version_number),
                }},
                status=status.HTTP_200_OK,
            )
        except Exception as exc:
            logger.exception("Execute version v%s failed", version_number)
            # Restore original model_data only on failure
            for cm in locked_models:
                if cm.model_name in original_data:
                    cm.model_data = original_data[cm.model_name]
                    cm.save(update_fields=["model_data"])
            result = Response(
                data={"status": "failed", "error_message": str(exc)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    return result


@api_view([HTTPMethods.POST])
@handle_http_request
def validate_draft(request: Request, project_id: str, model_name: str) -> Response:
    """Validate draft content without persisting."""
    from backend.application.model_validator.draft_validator import DraftValidator

    payload = request.data
    content = payload.get("model_data") or payload.get("yaml_content", {})
    result = DraftValidator(content=content).validate()
    return Response(data={"status": "success", "data": result}, status=status.HTTP_200_OK)
