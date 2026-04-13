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
    CommitProjectSerializer,
    RetryGitSyncSerializer,
    RollbackSerializer,
)
from backend.core.services import audit_trail_service
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
    """Queue a manual commit — pushes combined YAML to git asynchronously."""
    title = (request.data.get("title") or "").strip()
    description = (request.data.get("description") or "").strip()
    if not title:
        return Response(data={"status": "failed", "error_message": "Commit title is required."}, status=status.HTTP_400_BAD_REQUEST)

    project_instance = _get_project(project_id)

    import threading
    from backend.core.scheduler.version_celery_tasks import _run_manual_commit
    from backend.utils.tenant_context import get_current_user, get_current_tenant, _get_tenant_context
    user_info = get_current_user() or {}
    tenant_id = get_current_tenant()
    pid = str(project_instance.project_uuid)

    def _commit():
        from django.db import connection
        _get_tenant_context().set_tenant(tenant_id)
        try:
            _run_manual_commit(
                project_id=pid, title=title,
                description=description, author_info=user_info,
            )
        except Exception as ex:
            logger.warning("Manual commit thread failed for %s: %s", pid, str(ex))
        finally:
            connection.close()

    threading.Thread(target=_commit, daemon=True).start()
    return Response(
        data={"status": "pending", "message": "Commit queued — version history will update shortly."},
        status=status.HTTP_200_OK,
    )


@api_view([HTTPMethods.POST])
@handle_http_request
def import_from_branch(request: Request, project_id: str) -> Response:
    """Import models from a source project folder on the configured branch."""
    from backend.core.models.git_repo_config import GitRepoConfig

    project_instance = _get_project(project_id)
    source_folder = request.data.get("source_folder")
    source_branch = request.data.get("source_branch", "")
    if not source_folder:
        return Response(
            data={"status": "failed", "error_message": "source_folder is required"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    config = GitRepoConfig.objects.filter(
        project_id=project_id, is_deleted=False, is_active=True,
    ).first()
    if not config:
        return Response(
            data={"status": "failed", "error_message": "Git is not configured for this project."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    result = model_version_service.import_from_branch(
        project_instance=project_instance,
        git_config=config,
        source_folder=source_folder,
        source_branch=source_branch,
    )
    return Response(data={"status": "success", "data": result}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_version_history(request: Request, project_id: str) -> Response:
    """Get paginated version history — from GitHub commits if git configured, else DB."""
    page = int(request.GET.get("page", 1))
    limit = int(request.GET.get("limit", 10))

    project_instance = _get_project(project_id)

    from backend.core.models.git_repo_config import GitRepoConfig
    git_config = GitRepoConfig.objects.filter(
        project_id=project_id, is_deleted=False, is_active=True,
    ).first()
    if git_config:
        try:
            versions = model_version_service.get_versions_from_github(
                project_instance, git_config, page=page, per_page=limit,
            )
            return Response(data={"status": "success", "data": {
                "page_items": versions,
                "total_count": len(versions) if len(versions) < limit else len(versions) + 1,
                "source": "git",
            }}, status=status.HTTP_200_OK)
        except Exception:
            logger.warning("GitHub version fetch failed for project %s — falling back to DB", project_id, exc_info=True)

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
def get_version_detail_by_sha(request: Request, project_id: str, commit_sha: str) -> Response:
    """Get version detail by git commit SHA — fetches from GitHub/GitLab."""
    from backend.core.models.git_repo_config import GitRepoConfig
    from backend.core.services.git_service import get_git_service

    project_instance = _get_project(project_id)
    git_config = GitRepoConfig.objects.filter(
        project_id=project_id, is_deleted=False, is_active=True,
    ).first()
    if not git_config:
        return Response(data={"status": "failed", "error_message": "Git is not configured for this project."}, status=status.HTTP_400_BAD_REQUEST)

    git_svc = get_git_service(git_config)
    commit = git_svc.get_commit_detail(commit_sha)
    if not commit:
        return Response(data={"status": "failed", "error_message": f"Commit {commit_sha} not found."}, status=status.HTTP_404_NOT_FOUND)

    db_version = ModelVersion.objects.filter(project_instance=project_instance, git_commit_sha=commit_sha).first()
    data = {
        "commit_sha": commit["sha"],
        "commit_message": commit["message"],
        "author_name": commit["author_name"],
        "author_email": commit["author_email"],
        "committed_at": commit["date"],
        "commit_url": commit["html_url"],
        "files_changed": commit["files_changed"],
        "version_number": db_version.version_number if db_version else None,
        "is_auto_commit": db_version.is_auto_commit if db_version else None,
        "is_current": db_version.is_current if db_version else False,
        "pr_number": db_version.pr_number if db_version else None,
        "pr_url": db_version.pr_url or "" if db_version else None,
    }
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
def get_version_pr(request: Request, project_id: str, version_number: int) -> Response:
    """Get PR info associated with a version."""
    project_instance = _get_project(project_id)
    version = model_version_service.get_version(project_instance, version_number)
    if not version.pr_number:
        return Response(
            data={"status": "failed", "error_message": "No PR associated with this version."},
            status=status.HTTP_404_NOT_FOUND,
        )
    return Response(data={"status": "success", "data": {
        "pr_number": version.pr_number,
        "pr_url": version.pr_url or "",
        "version_number": version.version_number,
    }}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST])
@handle_http_request
def create_version_pr(request: Request, project_id: str, version_number: int) -> Response:
    """Create a PR from the working branch to the base branch."""
    from backend.core.models.git_repo_config import GitRepoConfig
    from backend.core.services import git_pr_service
    from backend.errors.exceptions import GitPRAlreadyExistsException

    project_instance = _get_project(project_id)
    try:
        version = ModelVersion.objects.get(project_instance=project_instance, version_number=version_number)
    except ModelVersion.DoesNotExist:
        return Response(data={"status": "failed", "error_message": f"Version {version_number} not found."}, status=status.HTTP_404_NOT_FOUND)

    if version.pr_number:
        return Response(data={"status": "failed", "data": {
            "pr_number": version.pr_number, "pr_url": version.pr_url or "",
            "message": "PR already exists for this version",
        }}, status=status.HTTP_409_CONFLICT)

    config = GitRepoConfig.objects.filter(project_id=project_id, is_deleted=False, is_active=True).first()
    if not config:
        return Response(data={"status": "failed", "error_message": "Git is not configured for this project."}, status=status.HTTP_400_BAD_REQUEST)

    if config.pr_mode == "disabled":
        return Response(data={"status": "failed", "error_message": "PR workflow is not enabled for this project."}, status=status.HTTP_400_BAD_REQUEST)

    try:
        pr_result = git_pr_service.create_pr_for_version(project_instance, version, config)
    except GitPRAlreadyExistsException:
        return Response(data={"status": "failed", "data": {
            "pr_number": version.pr_number, "pr_url": version.pr_url or "",
            "message": f"A PR already exists from {config.branch_name} to {config.pr_base_branch}",
        }}, status=status.HTTP_409_CONFLICT)

    return Response(data={"status": "success", "data": {
        "pr_number": pr_result["pr_number"],
        "pr_url": pr_result["pr_url"],
        "version_number": version.version_number,
        "message": "PR created successfully",
    }}, status=status.HTTP_200_OK)


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
    commit_sha = payload.get("commit_sha")
    if version_number is None and commit_sha is None:
        return Response(
            data={"status": "failed", "error_message": "version_number or commit_sha is required"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    from backend.application.context.application import ApplicationContext
    from backend.core.redis_client import RedisClient
    from django.db import transaction
    from visitran.singleton import Singleton

    project_instance = _get_project(project_id)

    # Try DB lookup first; fall back to GitHub for git-sourced versions
    target_version = None
    target_data = None
    if version_number is not None:
        try:
            target_version = model_version_service.get_version(project_instance, int(version_number))
        except Exception:
            pass

    if target_version:
        target_data = target_version.model_data
        if not target_data and target_version.git_commit_sha:
            from backend.core.services.model_version_service import _fetch_models_data_from_github
            target_data = _fetch_models_data_from_github(project_instance, target_version)
    elif commit_sha:
        # Git-sourced version: fetch directly from GitHub at this SHA
        from backend.core.models.git_repo_config import GitRepoConfig
        from backend.core.services.git_service import get_git_service
        from backend.core.services.yaml_serializer import deserialize_project_yaml, get_project_yaml_path

        git_config = GitRepoConfig.objects.filter(
            project_id=project_id, is_deleted=False, is_active=True,
        ).first()
        if git_config:
            folder_name = git_config.git_project_folder or project_instance.project_name
            file_path = get_project_yaml_path(folder_name)
            git_svc = get_git_service(git_config)
            raw, _ = git_svc.get_file(file_path, ref=commit_sha)
            if raw:
                target_data = deserialize_project_yaml(raw)

    target_data = target_data or {}

    if not target_data:
        return Response(
            data={"status": "failed", "error_message": "Could not load version data from GitHub or DB."},
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
    """Inner logic — runs while the Redis lock is held.

    Makes the project state match the executed version exactly:
    - Models in the version get their model_data set to the version snapshot
    - Models NOT in the version but in the current project are deleted on success
    - Models in the version but NOT in the current project are created

    On failure everything is rolled back to the pre-execution state.
    """
    from backend.application.context.application import ApplicationContext
    from django.db import transaction
    from visitran.singleton import Singleton

    historical_models = set(target_data.keys())
    current_model_names = set(
        ConfigModels.objects.filter(
            project_instance=project_instance,
        ).values_list("model_name", flat=True)
    )

    models_to_create = historical_models - current_model_names
    models_to_remove = current_model_names - historical_models

    created_models = []
    removed_snapshots = []

    with transaction.atomic():
        # Create ConfigModels for models in the version that don't exist yet
        for model_name in models_to_create:
            cm = ConfigModels(
                project_instance=project_instance,
                model_name=model_name,
                model_data=target_data[model_name],
                model_py_content="",
            )
            cm.save()
            created_models.append(cm)

        # Snapshot models that will be removed on success (for rollback on failure)
        if models_to_remove:
            remove_qs = ConfigModels.objects.select_for_update().filter(
                project_instance=project_instance, model_name__in=models_to_remove,
            )
            for cm in remove_qs:
                removed_snapshots.append({
                    "model_name": cm.model_name,
                    "model_data": cm.model_data,
                    "model_py_content_name": cm.model_py_content.name if cm.model_py_content else "",
                })

        # Lock and swap model_data for all historical models
        locked_models = list(
            ConfigModels.objects.select_for_update()
            .filter(project_instance=project_instance, model_name__in=historical_models)
        )
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
            # Execute only the historical models
            app.execute_visitran_run_command(
                current_models=sorted(historical_models),
                environment_id=environment_id,
            )
            app.visitran_context.close_db_connection()

            model_version_service.set_current_version(project_instance, int(version_number))
            succeeded = True

            # Success: delete models not in this version so the project
            # reflects the executed version state exactly
            if models_to_remove:
                ConfigModels.objects.filter(
                    project_instance=project_instance, model_name__in=models_to_remove,
                ).delete()
                logger.info(
                    "Execute v%s: removed %d model(s) not in version: %s",
                    version_number, len(models_to_remove), sorted(models_to_remove),
                )

            result = Response(
                data={"status": "success", "data": {
                    "message": f"Execution of version {version_number} completed successfully.",
                    "version_number": int(version_number),
                    "models_created": sorted(models_to_create),
                    "models_removed": sorted(models_to_remove),
                }},
                status=status.HTTP_200_OK,
            )
        except Exception as exc:
            logger.exception("Execute version v%s failed", version_number)
            # Restore original model_data for historical models
            for cm in locked_models:
                if cm.model_name in original_data:
                    cm.model_data = original_data[cm.model_name]
                    cm.save(update_fields=["model_data"])
            # Delete models we created for this version
            for cm in created_models:
                cm.delete()
            result = Response(
                data={"status": "failed", "error_message": str(exc)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    return result
