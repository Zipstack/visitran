"""Model version service — merged from model_version_context + model_version_session.

Handles project-level and model-scoped version CRUD, commit, rollback,
diff, and git sync. Flat service layer — no session/context split.
"""

import hashlib
import json
import logging
from typing import Any

import yaml
from django.db import transaction
from django.db.models import Max

from backend.core.models.config_models import ConfigModels
from backend.core.models.model_version import ModelVersion
from backend.core.models.project_details import ProjectDetails
from backend.core.services import version_cache_service as vcache
from backend.core.services.audit_trail_service import (
    log_version_committed,
    log_version_rolled_back,
)
from backend.core.services.version_diff_service import compare_model_data
from backend.errors.exceptions import (
    CommitFailedException,
    DuplicateContentCommitException,
    NoChangesToCommitException,
    VersionNotFoundException,
)
from backend.utils.pagination import CustomPaginator
from backend.utils.tenant_context import get_current_user

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

def _get_next_version_number(project_instance: ProjectDetails) -> int:
    result = ModelVersion.objects.filter(
        project_instance=project_instance, config_model__isnull=True,
    ).aggregate(max_version=Max("version_number"))
    return (result["max_version"] or 0) + 1


def _get_next_model_version_number(config_model: ConfigModels) -> int:
    result = ModelVersion.objects.filter(
        config_model=config_model,
    ).aggregate(max_version=Max("version_number"))
    return (result["max_version"] or 0) + 1


def _compute_data_hash(model_data: dict[str, Any]) -> str:
    canonical = json.dumps(model_data, sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _build_user_attribution(user_info: dict | None = None) -> dict:
    if not user_info:
        user_info = get_current_user()
    return {
        "committed_by": user_info,
        "user_name_snapshot": user_info.get("name", ""),
        "user_email_snapshot": user_info.get("username", ""),
        "user_role_snapshot": "",
    }


def set_current_version(project_instance: ProjectDetails, version_number: int) -> None:
    """Mark a single project-level version as current, clearing all others."""
    with transaction.atomic():
        ModelVersion.objects.filter(
            project_instance=project_instance, config_model__isnull=True,
        ).update(is_current=False)
        ModelVersion.objects.filter(
            project_instance=project_instance, config_model__isnull=True,
            version_number=version_number,
        ).update(is_current=True)
    vcache.invalidate_model_versions(str(project_instance.project_uuid))


def _build_all_model_data(project_instance: ProjectDetails) -> dict[str, dict]:
    models = ConfigModels.objects.filter(project_instance=project_instance)
    return {m.model_name: m.model_data for m in models if m.model_data}


# ------------------------------------------------------------------
# Core version creation (project-level)
# ------------------------------------------------------------------

def create_version(
    project_instance: ProjectDetails,
    all_model_data: dict[str, dict],
    commit_message: str = "",
    is_auto_commit: bool = False,
    user_info: dict | None = None,
    rollback_metadata: dict[str, Any] | None = None,
) -> ModelVersion:
    """Create a new project-level version snapshot atomically."""
    try:
        with transaction.atomic():
            data_hash = _compute_data_hash(all_model_data)

            # Check duplicate content
            latest = get_latest_version(project_instance)
            if latest is not None:
                if _compute_data_hash(latest.model_data) == data_hash:
                    raise DuplicateContentCommitException(existing_version=latest.version_number)

            version_number = _get_next_version_number(project_instance)
            attrs = _build_user_attribution(user_info)

            version = ModelVersion.objects.create(
                config_model=None,
                project_instance=project_instance,
                version_number=version_number,
                model_data=all_model_data,
                commit_message=commit_message,
                committed_by=attrs["committed_by"],
                user_name_snapshot=attrs["user_name_snapshot"],
                user_email_snapshot=attrs["user_email_snapshot"],
                user_role_snapshot=attrs["user_role_snapshot"],
                is_auto_commit=is_auto_commit,
                rollback_metadata=rollback_metadata or {},
            )

            # Mark this version as current
            ModelVersion.objects.filter(
                project_instance=project_instance, config_model__isnull=True,
            ).exclude(version_id=version.version_id).update(is_current=False)
            version.is_current = True
            version.save(update_fields=["is_current"])

            logger.info("Created project version %d (models=%d, hash=%s)",
                        version_number, len(all_model_data), data_hash[:12])

            project_id = str(project_instance.project_uuid)
            detail = serialize_version_detail(version)
            vcache.invalidate_on_commit(project_id)
            vcache.warm_after_commit(project_id, detail, version_number)

            if rollback_metadata:
                log_version_rolled_back(
                    project_instance=project_instance, version=version,
                    user_info=user_info, rollback_metadata=rollback_metadata,
                )
            else:
                log_version_committed(
                    project_instance=project_instance, version=version,
                    user_info=user_info, commit_message=commit_message,
                    is_auto_commit=is_auto_commit,
                )
            return version

    except (DuplicateContentCommitException,):
        raise
    except Exception:
        logger.exception("Unexpected error creating project version for %s", project_instance.project_uuid)
        raise CommitFailedException()


# ------------------------------------------------------------------
# Project-level commit orchestration
# ------------------------------------------------------------------

def commit_project(
    project_instance: ProjectDetails,
    commit_message: str = "",
    user_info: dict | None = None,
) -> dict[str, Any]:
    """Create a project-level version snapshot of ALL models."""
    all_data = _build_all_model_data(project_instance)

    latest = get_latest_version(project_instance)
    if latest and latest.model_data == all_data:
        raise NoChangesToCommitException()

    version = create_version(
        project_instance=project_instance,
        all_model_data=all_data,
        commit_message=commit_message,
        user_info=user_info,
    )
    _sync_to_git(version)
    logger.info("Committed project version %d (%d models)", version.version_number, len(all_data))
    return serialize_version_detail(version)


def commit_all_models(
    project_instance: ProjectDetails,
    commit_message: str = "",
    is_auto_commit: bool = False,
    user_info: dict | None = None,
) -> list[dict[str, Any]]:
    """Create ONE project-level version for all models (used by execute_run)."""
    all_data = _build_all_model_data(project_instance)
    if not all_data:
        return []

    latest = get_latest_version(project_instance)
    if latest and latest.model_data == all_data:
        return []

    version = create_version(
        project_instance=project_instance, all_model_data=all_data,
        commit_message=commit_message, is_auto_commit=is_auto_commit, user_info=user_info,
    )
    _sync_to_git(version)
    return [{"version_number": version.version_number, "model_count": len(all_data), "model_names": sorted(all_data.keys())}]


# ------------------------------------------------------------------
# Read operations
# ------------------------------------------------------------------

def get_versions(
    project_instance: ProjectDetails,
    page: int = 1,
    limit: int = 10,
) -> dict[str, Any]:
    project_id = str(project_instance.project_uuid)
    cached = vcache.get_version_history(project_id, page, limit)
    if cached is not None:
        return cached

    queryset = ModelVersion.objects.filter(
        project_instance=project_instance, config_model__isnull=True,
    ).order_by("-version_number")

    paginator = CustomPaginator(queryset=queryset, limit=limit, page=page)
    result = paginator.paginate()
    result["page_items"] = [_serialize_version(v) for v in result["page_items"]]
    vcache.set_version_history(project_id, page, limit, result)
    return result


def get_versions_from_github(
    project_instance: ProjectDetails,
    git_config,
    page: int = 1,
    per_page: int = 20,
) -> list[dict[str, Any]]:
    """Fetch version history from GitHub/GitLab commits, merged with DB metadata."""
    from backend.core.services.git_service import get_git_service
    from backend.core.services.yaml_serializer import get_project_yaml_path

    folder_name = getattr(git_config, "git_project_folder", "") or project_instance.project_name
    file_path = get_project_yaml_path(folder_name)
    git_svc = get_git_service(git_config)

    # Try with file path filter first; fall back to directory-level if empty
    github_commits = git_svc.list_commits(path=file_path, page=page, per_page=per_page)
    if not github_commits:
        # Try directory only (project slug folder)
        dir_path = file_path.rsplit("/", 1)[0] if "/" in file_path else file_path
        github_commits = git_svc.list_commits(path=dir_path, page=page, per_page=per_page)

    shas = [c["sha"] for c in github_commits]
    db_versions = ModelVersion.objects.filter(
        project_instance=project_instance, git_commit_sha__in=shas,
    )
    db_map = {v.git_commit_sha: v for v in db_versions}

    versions = []
    for idx, commit in enumerate(github_commits):
        db = db_map.get(commit["sha"])
        versions.append({
            "version_id": str(db.version_id) if db else commit["sha"],
            "version_number": db.version_number if db else len(github_commits) - idx,
            "commit_sha": commit["sha"],
            "commit_message": commit.get("message", ""),
            "committed_by": {"name": commit.get("author", "")},
            "created_at": commit.get("date", ""),
            "commit_url": commit.get("html_url", ""),
            "is_auto_commit": db.is_auto_commit if db else False,
            "is_current": db.is_current if db else (idx == 0),
            "git_sync_status": "synced",
            "pr_number": db.pr_number if db else None,
            "pr_url": db.pr_url or "" if db else "",
        })
    return versions


def get_version(project_instance: ProjectDetails, version_number: int) -> ModelVersion:
    try:
        return ModelVersion.objects.get(
            project_instance=project_instance, config_model__isnull=True, version_number=version_number,
        )
    except ModelVersion.DoesNotExist:
        raise VersionNotFoundException(version_number=version_number)


def get_version_by_id(project_instance: ProjectDetails, version_id: str) -> ModelVersion:
    try:
        return ModelVersion.objects.get(project_instance=project_instance, version_id=version_id)
    except ModelVersion.DoesNotExist:
        raise VersionNotFoundException(version_number=0)


def get_latest_version(project_instance: ProjectDetails) -> ModelVersion | None:
    return (
        ModelVersion.objects.filter(project_instance=project_instance, config_model__isnull=True)
        .order_by("-version_number").first()
    )


def get_version_detail(
    project_instance: ProjectDetails,
    version_number: int | None = None,
    version_id: str | None = None,
) -> dict[str, Any]:
    if version_id is not None:
        version = get_version_by_id(project_instance, version_id)
    else:
        version = get_version(project_instance, version_number)
    return serialize_version_detail(version)


# ------------------------------------------------------------------
# Version comparison
# ------------------------------------------------------------------

def compare_versions(
    project_instance: ProjectDetails,
    version_a: int,
    version_b: int,
) -> dict[str, Any]:
    ver_a = get_version(project_instance, version_a)
    ver_b = get_version(project_instance, version_b)
    data_a = ver_a.model_data or {}
    data_b = ver_b.model_data or {}
    all_models = sorted(set(list(data_a.keys()) + list(data_b.keys())))

    return {
        "changes_by_model": {
            m: compare_model_data(data_a.get(m, {}), data_b.get(m, {}))
            for m in all_models if data_a.get(m, {}) != data_b.get(m, {})
        },
        "models_added": [m for m in all_models if m not in data_a],
        "models_removed": [m for m in all_models if m not in data_b],
        "version_a": {
            "version_number": ver_a.version_number, "version_id": str(ver_a.version_id),
            "committed_by": ver_a.user_name_snapshot, "created_at": ver_a.created_at.isoformat(),
        },
        "version_b": {
            "version_number": ver_b.version_number, "version_id": str(ver_b.version_id),
            "committed_by": ver_b.user_name_snapshot, "created_at": ver_b.created_at.isoformat(),
        },
    }


def preview_pending_changes(project_instance: ProjectDetails) -> dict[str, Any]:
    current_data = _build_all_model_data(project_instance)
    latest = get_latest_version(project_instance)
    committed_data = latest.model_data if latest else {}
    all_models = sorted(set(list(current_data.keys()) + list(committed_data.keys())))
    changes = []
    for model_name in all_models:
        old = committed_data.get(model_name)
        new = current_data.get(model_name)
        if old == new:
            continue
        if old is None:
            change_type = "added"
        elif new is None:
            change_type = "removed"
        else:
            change_type = "modified"
        changes.append({
            "model_name": model_name, "change_type": change_type,
            "old_yaml": yaml.dump(old, indent=4, default_flow_style=False) if old else "",
            "new_yaml": yaml.dump(new, indent=4, default_flow_style=False) if new else "",
        })
    return {
        "has_changes": len(changes) > 0,
        "total_models_changed": len(changes),
        "latest_version": latest.version_number if latest else 0,
        "changes": changes,
    }


# ------------------------------------------------------------------
# Rollback
# ------------------------------------------------------------------

def rollback_to_version(
    project_instance: ProjectDetails,
    version_number: int,
    reason: str = "",
    user_info: dict | None = None,
) -> dict[str, Any]:
    """Rollback the project to a specific version.

    Fetches model data from GitHub at the exact commit SHA if available,
    falls back to DB-stored model_data for legacy versions.
    """
    target = get_version(project_instance, version_number)

    # Determine content source: GitHub first, DB fallback
    models_data = None

    if target.git_commit_sha:
        try:
            from backend.core.models.git_repo_config import GitRepoConfig
            from backend.core.services.git_service import get_git_service
            from backend.core.services.yaml_serializer import (
                deserialize_project_yaml, get_project_yaml_path,
            )

            git_config = GitRepoConfig.objects.filter(
                project_id=str(project_instance.project_uuid),
                is_deleted=False, is_active=True,
            ).first()
            if git_config:
                file_path = get_project_yaml_path(
                    project_instance.project_name, git_config.base_path,
                )
                git_svc = get_git_service(git_config)
                raw_content, _sha = git_svc.get_file(
                    path=file_path, ref=target.git_commit_sha,
                )
                if raw_content:
                    models_data = deserialize_project_yaml(raw_content)
        except Exception:
            logger.warning(
                "Failed to fetch version %d from git — falling back to DB",
                version_number, exc_info=True,
            )

    if not models_data and target.model_data:
        models_data = target.model_data

    if not models_data:
        raise VersionNotFoundException(version_number=version_number)

    # Restore each model
    with transaction.atomic():
        for model_name, model_data in models_data.items():
            cm = ConfigModels.objects.filter(
                project_instance=project_instance, model_name=model_name,
            ).first()
            if cm:
                cm.model_data = model_data
                cm.save(update_fields=["model_data"])

    # Trigger auto-commit to record the rollback on GitHub
    try:
        import threading
        from backend.core.scheduler.version_celery_tasks import _run_auto_commit
        from backend.utils.tenant_context import get_current_tenant, _get_tenant_context
        pid = str(project_instance.project_uuid)
        action = f"rollback_to_v{version_number}"
        author = user_info or get_current_user() or {}
        tenant_id = get_current_tenant()

        def _commit():
            from django.db import connection
            _get_tenant_context().set_tenant(tenant_id)
            try:
                _run_auto_commit(project_id=pid, trigger_action=action, author_info=author)
            except Exception:
                pass
            finally:
                connection.close()

        threading.Thread(target=_commit, daemon=True).start()
    except Exception:
        logger.debug("Rollback auto-commit trigger failed — non-blocking")

    logger.info("Rolled back project to v%d", version_number)
    return {
        "rolled_back": True,
        "rolled_back_to_version": version_number,
        "model_count": len(models_data),
        "model_names": sorted(models_data.keys()),
    }


# ------------------------------------------------------------------
# Git sync (inline, project-level)
# ------------------------------------------------------------------

def _sync_to_git(version: ModelVersion) -> None:
    """Push all model YAML files to git under a version folder."""
    if getattr(version, "is_auto_commit", False):
        return

    from django.utils import timezone as dj_timezone
    from backend.core.models.git_repo_config import GitRepoConfig
    from backend.core.services.git_service import get_git_service
    from backend.core.services.yaml_serializer import (
        build_git_version_folder_path,
        serialize_model_to_yaml,
    )

    config = GitRepoConfig.objects.filter(
        project_id=version.project_instance_id, is_deleted=False, is_active=True,
    ).first()
    if not config:
        return

    version.git_sync_status = "pending"
    version.save(update_fields=["git_sync_status"])

    # PR workflow is manual — commits are pushed to working branch by celery
    # tasks, user creates PRs via the version timeline. Nothing to do here.
    if config.pr_mode != "disabled":
        return

    project = version.project_instance
    project_name = project.project_name if project else ""
    org_id = str(project.organization_id) if project and project.organization_id else ""

    try:
        service = get_git_service(config)
        model_data = version.model_data or {}
        last_commit_sha = ""

        for model_name, single_model_data in model_data.items():
            yaml_content = serialize_model_to_yaml(
                model_data=single_model_data, model_name=model_name,
                project_name=project_name, version_number=version.version_number,
            )
            file_path = build_git_version_folder_path(
                config=config, project_name=project_name,
                version_number=version.version_number,
                commit_message=version.commit_message,
                model_name=model_name, org_id=org_id,
            )
            _existing, existing_sha = service.get_file(file_path)
            commit_msg = (
                f"v{version.version_number}: {version.commit_message}"
                if version.commit_message
                else f"v{version.version_number}: {model_name}"
            )
            result = service.put_file(path=file_path, content=yaml_content, message=commit_msg, sha=existing_sha)
            last_commit_sha = result.get("commit_sha", "")

        version.git_sync_status = "synced"
        version.git_commit_sha = last_commit_sha[:40]
        version.save(update_fields=["git_sync_status", "git_commit_sha"])
        config.connection_status = "connected"
        config.last_synced_at = dj_timezone.now()
        config.error_message = ""
        config.save(update_fields=["connection_status", "last_synced_at", "error_message"])
        logger.info("Git sync SUCCESS: project v%d -> %d models", version.version_number, len(model_data))

    except Exception as exc:
        logger.warning("Git sync FAILED for project v%d: %s", version.version_number, str(exc)[:200], exc_info=True)
        version.git_sync_status = "failed"
        version.save(update_fields=["git_sync_status"])
        config.connection_status = "error"
        config.error_message = str(exc)[:500]
        config.save(update_fields=["connection_status", "error_message"])


# ------------------------------------------------------------------
# Import from branch
# ------------------------------------------------------------------

def import_from_branch(
    project_instance: ProjectDetails,
    git_config,
    source_folder: str,
    source_branch: str = "",
) -> dict[str, Any]:
    """Import models from a source folder into the project.

    Reads from {source_folder}/models.yaml on the working branch (created
    from source_branch), creates ConfigModels + ModelVersion in DB, and sets
    git_project_folder so all future reads/writes target the same folder.
    No new folder is created — the project works directly on the source data.
    """
    from backend.core.services.git_service import get_git_service
    from backend.core.services.yaml_serializer import deserialize_project_yaml

    git_svc = get_git_service(git_config)

    # Read from source folder on the working branch (which was created from source branch)
    source_path = f"{source_folder}/models.yaml"
    raw_content, _sha = git_svc.get_file(source_path)
    if not raw_content:
        raise CommitFailedException()

    models_data = deserialize_project_yaml(raw_content)
    if not models_data:
        raise CommitFailedException()

    # Create ConfigModels in DB for each model
    for model_name, model_data in models_data.items():
        existing = ConfigModels.objects.filter(
            project_instance=project_instance, model_name=model_name,
        ).first()
        if existing:
            existing.model_data = model_data
            existing.save()
        else:
            ConfigModels.objects.create(
                project_instance=project_instance,
                model_name=model_name,
                model_data=model_data,
            )

    # Set git_project_folder to the source folder so all future YAML
    # reads/writes (commits, version history, execute) use the same path.
    git_config.git_project_folder = source_folder
    git_config.save(update_fields=["git_project_folder"])

    # Get the latest commit SHA for the source file on this branch
    commits = git_svc.list_commits(path=source_path, per_page=1)
    commit_sha = commits[0]["sha"][:40] if commits else ""

    src_branch_label = f" on branch '{source_branch}'" if source_branch else ""
    commit_msg = (
        f"Imported from '{source_folder}'{src_branch_label}"
    )

    # Create ModelVersion in DB
    with transaction.atomic():
        last = ModelVersion.objects.filter(
            project_instance=project_instance,
        ).aggregate(max_v=Max("version_number"))["max_v"] or 0
        next_version = last + 1

        version = ModelVersion.objects.create(
            project_instance=project_instance,
            version_number=next_version,
            model_data=models_data,
            commit_message=commit_msg,
            is_auto_commit=True,
            is_current=True,
            git_commit_sha=commit_sha,
            git_sync_status="synced",
            organization=project_instance.organization,
        )

    # Extract schema warnings from model data
    schemas = set()
    for md in models_data.values():
        if not isinstance(md, dict):
            continue
        src_schema = (md.get("source") or {}).get("schema_name", "")
        if src_schema:
            schemas.add(src_schema)
        dst_schema = (md.get("model") or {}).get("schema_name", "")
        if dst_schema:
            schemas.add(dst_schema)

    return {
        "models_imported": len(models_data),
        "model_names": sorted(models_data.keys()),
        "version_number": version.version_number,
        "schemas_required": sorted(schemas),
    }


# ------------------------------------------------------------------
# GitHub content fallback
# ------------------------------------------------------------------

def _fetch_models_data_from_github(
    project_instance: ProjectDetails,
    version: ModelVersion,
) -> dict[str, Any]:
    """Fetch the combined YAML at version.git_commit_sha from GitHub/GitLab.

    Returns { model_name: model_data_dict } or {} on failure.
    """
    if not version.git_commit_sha:
        return {}
    try:
        from backend.core.models.git_repo_config import GitRepoConfig
        from backend.core.services.git_service import get_git_service
        from backend.core.services.yaml_serializer import (
            deserialize_project_yaml,
            get_project_yaml_path,
        )

        git_config = GitRepoConfig.objects.filter(
            project_id=str(project_instance.project_uuid),
            is_deleted=False,
            is_active=True,
        ).first()
        if not git_config:
            return {}

        folder_name = git_config.git_project_folder or project_instance.project_name
        file_path = get_project_yaml_path(folder_name)
        git_svc = get_git_service(git_config)
        raw_content, _sha = git_svc.get_file(
            path=file_path, ref=version.git_commit_sha,
        )
        if not raw_content:
            return {}
        return deserialize_project_yaml(raw_content)
    except Exception:
        logger.warning(
            "GitHub fallback failed for sha %s",
            version.git_commit_sha,
            exc_info=True,
        )
        return {}


# ------------------------------------------------------------------
# Serialization
# ------------------------------------------------------------------

def _serialize_version(version: ModelVersion, include_data: bool = False) -> dict[str, Any]:
    model_data = version.model_data or {}
    model_names = sorted(model_data.keys()) if isinstance(model_data, dict) else []
    data = {
        "version_id": str(version.version_id),
        "version_number": version.version_number,
        "commit_message": version.commit_message,
        "committed_by": {
            "name": version.user_name_snapshot,
            "email": version.user_email_snapshot,
            "role": version.user_role_snapshot,
        },
        "is_current": version.is_current,
        "is_auto_commit": version.is_auto_commit,
        "rollback_metadata": version.rollback_metadata or {},
        "content_hash": version.content_hash,
        "created_at": version.created_at.isoformat(),
        "git_sync_status": version.git_sync_status,
        "git_commit_sha": version.git_commit_sha,
        "pr_number": version.pr_number,
        "pr_url": version.pr_url or "",
        "model_count": len(model_names),
        "model_names": model_names,
    }
    if include_data:
        # Fetch from GitHub if model_data is empty (new architecture stores None)
        if not model_data and version.git_commit_sha:
            model_data = _fetch_models_data_from_github(
                version.project_instance, version,
            )
            model_names = sorted(model_data.keys()) if isinstance(model_data, dict) else []
            data["model_count"] = len(model_names)
            data["model_names"] = model_names

        data["model_data"] = model_data
        yaml_parts = []
        for name in model_names:
            yaml_parts.append(f"# --- {name} ---")
            yaml_parts.append(yaml.dump(model_data[name], indent=4, default_flow_style=False))
        data["yaml_content"] = "\n".join(yaml_parts)
        parent = (
            ModelVersion.objects.filter(
                project_instance=version.project_instance,
                config_model__isnull=True,
                version_number=version.version_number - 1,
            ).values_list("version_id", flat=True).first()
        )
        data["parent_version_id"] = str(parent) if parent else None
    return data


def serialize_version_detail(version: ModelVersion) -> dict[str, Any]:
    return _serialize_version(version, include_data=True)


def update_git_sync_status(version_id: str, sync_status: str, commit_sha: str = "") -> None:
    try:
        version = ModelVersion.objects.get(version_id=version_id)
        version.git_sync_status = sync_status
        if commit_sha:
            version.git_commit_sha = commit_sha
        version.save(update_fields=["git_sync_status", "git_commit_sha"])
    except ModelVersion.DoesNotExist:
        logger.error("ModelVersion %s not found for git sync update", version_id)
