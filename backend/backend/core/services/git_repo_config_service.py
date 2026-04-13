"""Git repo config service — merged from git_repo_config_context + git_repo_config_session.

Handles CRUD, test connection, and serialization for GitRepoConfig.
"""

import logging
from typing import Any, Optional

from django.conf import settings

from backend.core.models.git_repo_config import GitRepoConfig
from backend.core.models.project_details import ProjectDetails
from backend.errors.exceptions import (
    GitConfigAlreadyExistsException,
    GitConfigurationNotFoundException,
    GitConnectionFailedException,
    ResourcePermissionDeniedException,
)
from backend.core.services.git_service import (
    get_git_service,
    GitHubService,
    GitLabService,
    _is_gitlab_host,
)
from backend.utils.tenant_context import get_current_user

logger = logging.getLogger(__name__)

DEFAULT_REPO_URL = getattr(settings, "GIT_DEFAULT_REPO_URL", "")
DEFAULT_REPO_TOKEN_SETTING = "GIT_DEFAULT_REPO_TOKEN"


def _check_project_access(project_id: str) -> None:
    """Verify the current user is the project creator.

    Raises ResourcePermissionDeniedException if not.
    Skipped when no user context is available (e.g. Celery tasks).
    """
    try:
        user_info = get_current_user()
    except Exception:
        return  # No user context (Celery worker) — skip check
    username = user_info.get("username", "")
    if not username:
        return  # No user identity — skip (OSS single-user fallback)
    project = ProjectDetails.objects.filter(project_uuid=project_id).first()
    if not project:
        return  # Project not found — let downstream handle 404
    project_owner = (project.created_by or {}).get("username", "")
    if project_owner and project_owner != username:
        raise ResourcePermissionDeniedException()


# ------------------------------------------------------------------
# Read
# ------------------------------------------------------------------

def get_config(project_id: str) -> Optional[dict[str, Any]]:
    _check_project_access(project_id)
    config = _get_config_by_project(project_id)
    if not config:
        return None
    return _serialize_config(config)


# ------------------------------------------------------------------
# Create / Update
# ------------------------------------------------------------------

def save_config(project_id: str, config_data: dict[str, Any]) -> dict[str, Any]:
    logger.warning("save_config called with data: %s",
                   {k: v for k, v in config_data.items() if k != 'token'})
    _check_project_access(project_id)
    repo_type = config_data.get("repo_type", "custom")
    if repo_type == "default":
        config_data = _prepare_default_config(config_data)

    # Server-side token validation — test before storing
    credentials = config_data.get("credentials", {})
    token = credentials.get("token", "")
    repo_url = config_data.get("repo_url", "")
    if token and repo_url:
        try:
            service = _build_service_from_data(config_data)
            service.test_connection()
        except Exception as exc:
            logger.warning("save_config failed: %s", str(exc))
            raise GitConnectionFailedException(
                error_message=f"Token validation failed: {exc}"
            )

    existing = _get_config_by_project(project_id)
    if existing:
        config = _update_config(project_id, config_data)
    else:
        config = _create_config(project_id, config_data)
    return _serialize_config(config)


# ------------------------------------------------------------------
# Delete
# ------------------------------------------------------------------

def delete_config(project_id: str) -> None:
    _check_project_access(project_id)
    config = GitRepoConfig.objects.filter(project_id=project_id, is_deleted=False).first()
    if not config:
        raise GitConfigurationNotFoundException(project_id=project_id)
    config.is_deleted = True
    config.is_active = False
    config.save()


# ------------------------------------------------------------------
# Test Connection
# ------------------------------------------------------------------

def _build_service_from_data(config_data: dict[str, Any]):
    """Instantiate a GitHubService or GitLabService from raw config_data (no saved model)."""
    repo_url = config_data.get("repo_url", "")
    credentials = config_data.get("credentials", {})
    token = credentials.get("token", "")
    branch = config_data.get("branch_name", "main")

    if not repo_url:
        raise GitConnectionFailedException(error_message="Repository URL is required.")

    if "github.com" in repo_url:
        return GitHubService(repo_url, token, branch)
    if "gitlab.com" in repo_url or _is_gitlab_host(repo_url):
        return GitLabService(repo_url, token, branch)
    raise GitConnectionFailedException(
        error_message=f"Unsupported git provider. Only GitHub and GitLab are supported: {repo_url}"
    )


def test_connection(project_id: str, config_data: dict[str, Any]) -> dict[str, Any]:
    _check_project_access(project_id)
    repo_type = config_data.get("repo_type", "custom")
    if repo_type == "default":
        config_data = _prepare_default_config(config_data)

    service = _build_service_from_data(config_data)
    result = service.test_connection()

    # Include branch list so frontend can populate branch dropdown
    try:
        result["branches"] = service.list_branches()
    except Exception:
        result["branches"] = []

    return result


def create_branch_from_data(
    project_id: str, config_data: dict[str, Any],
) -> dict[str, Any]:
    """Create a branch using inline credentials (before config is saved)."""
    _check_project_access(project_id)
    repo_type = config_data.get("repo_type", "custom")
    if repo_type == "default":
        config_data = _prepare_default_config(config_data)

    branch_name = config_data.get("branch_name", "")
    from_branch = config_data.get("from_branch", "main")
    if not branch_name:
        raise GitConnectionFailedException(error_message="Branch name is required.")

    service = _build_service_from_data({**config_data, "branch_name": from_branch})
    result = service.create_branch(branch_name, from_branch=from_branch)
    return {"branch_name": result.get("branch_name", branch_name), "sha": result.get("sha", "")}


def list_project_folders(
    project_id: str, config_data: dict[str, Any],
) -> list[dict[str, Any]]:
    """List project folders in repo that contain models.yaml."""
    _check_project_access(project_id)
    repo_type = config_data.get("repo_type", "custom")
    if repo_type == "default":
        config_data = _prepare_default_config(config_data)

    source_branch = config_data.get("source_branch", config_data.get("branch_name", "main"))
    service = _build_service_from_data({**config_data, "branch_name": source_branch})

    dirs = service.list_directory(path="", ref=source_branch)
    folders = []
    for d in dirs:
        content, _ = service.get_file(f"{d['name']}/models.yaml", ref=source_branch)
        if content:
            from backend.core.services.yaml_serializer import deserialize_project_yaml
            models_data = deserialize_project_yaml(content)
            folders.append({
                "name": d["name"],
                "model_count": len(models_data),
                "model_names": sorted(models_data.keys())[:10],
            })
    return folders


# ------------------------------------------------------------------
# Available repos
# ------------------------------------------------------------------

def get_available_repos() -> list[dict[str, Any]]:
    configs = GitRepoConfig.objects.filter(
        is_deleted=False, is_active=True,
    ).values("repo_type", "repo_url", "branch_name", "auth_type").distinct()
    return [
        {"repo_type": c["repo_type"], "repo_url": c["repo_url"],
         "branch_name": c["branch_name"], "auth_type": c["auth_type"]}
        for c in configs
    ]


# ------------------------------------------------------------------
# Sync status
# ------------------------------------------------------------------

def update_sync_status(
    project_id: str,
    connection_status: str,
    error_message: str = "",
    last_synced_at=None,
) -> None:
    config = GitRepoConfig.objects.filter(project_id=project_id, is_deleted=False).first()
    if config:
        config.connection_status = connection_status
        config.error_message = error_message
        if last_synced_at:
            config.last_synced_at = last_synced_at
        config.save()


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

def _get_config_by_project(project_id: str) -> Optional[GitRepoConfig]:
    return GitRepoConfig.objects.filter(
        project_id=project_id, is_deleted=False, is_active=True,
    ).first()


def _create_config(project_id: str, config_data: dict[str, Any]) -> GitRepoConfig:
    existing = GitRepoConfig.objects.filter(project_id=project_id, is_deleted=False).first()
    if existing:
        raise GitConfigAlreadyExistsException(project_id=project_id)
    config = GitRepoConfig(
        project_id=project_id,
        repo_type=config_data["repo_type"],
        repo_url=config_data["repo_url"],
        auth_type=config_data.get("auth_type", "pat"),
        encrypted_credentials=config_data.get("credentials", {}),
        branch_name=config_data.get("branch_name", "main"),
        base_path=config_data.get("base_path", ""),
        git_project_folder=config_data.get("git_project_folder", ""),
        connection_status=config_data.get("connection_status", "pending"),
    )
    config.save()
    logger.warning("GitRepoConfig created: %s", config.git_repo_config_id)
    return config


def _update_config(project_id: str, config_data: dict[str, Any]) -> GitRepoConfig:
    config = GitRepoConfig.objects.filter(project_id=project_id, is_deleted=False).first()
    if not config:
        raise GitConfigurationNotFoundException(project_id=project_id)
    _READONLY_PROPS = {"pr_workflow_enabled"}
    for field in ("repo_type", "repo_url", "auth_type", "branch_name", "base_path", "connection_status", "error_message", "pr_mode", "pr_base_branch", "pr_branch_prefix", "git_project_folder"):
        if field in config_data and field not in _READONLY_PROPS:
            setattr(config, field, config_data[field])
    if "credentials" in config_data:
        config.encrypted_credentials = config_data["credentials"]
    config.save()
    logger.warning("GitRepoConfig updated: %s", config.git_repo_config_id)
    return config


def _prepare_default_config(config_data: dict[str, Any]) -> dict[str, Any]:
    config_data = config_data.copy()
    repo_url = DEFAULT_REPO_URL
    default_token = getattr(settings, DEFAULT_REPO_TOKEN_SETTING, "")
    if not repo_url:
        raise GitConnectionFailedException(
            error_message="Default git repository is not configured. "
            "Set GIT_DEFAULT_REPO_URL in your environment or use a custom repository."
        )
    config_data["repo_url"] = repo_url
    config_data["repo_type"] = "default"
    config_data["auth_type"] = "pat"
    config_data["branch_name"] = config_data.get("branch_name", "main")
    config_data["credentials"] = {"token": default_token}
    return config_data


def _serialize_config(config: GitRepoConfig) -> dict[str, Any]:
    return {
        "id": str(config.git_repo_config_id),
        "project_id": str(config.project_id),
        "repo_type": config.repo_type,
        "repo_url": config.repo_url,
        "auth_type": config.auth_type,
        "credentials": config.masked_credentials,
        "branch_name": config.branch_name,
        "base_path": config.base_path,
        "is_active": config.is_active,
        "connection_status": config.connection_status,
        "error_message": config.error_message,
        "pr_mode": config.pr_mode,
        "pr_workflow_enabled": config.pr_workflow_enabled,
        "pr_base_branch": config.pr_base_branch,
        "pr_branch_prefix": config.pr_branch_prefix,
        "git_project_folder": config.git_project_folder,
        "last_synced_at": config.last_synced_at.isoformat() if config.last_synced_at else None,
        "created_by": config.created_by,
        "last_modified_by": config.last_modified_by,
        "created_at": config.created_at.isoformat() if config.created_at else None,
        "modified_at": config.modified_at.isoformat() if config.modified_at else None,
    }
