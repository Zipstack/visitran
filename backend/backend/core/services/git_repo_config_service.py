"""Git repo config service — merged from git_repo_config_context + git_repo_config_session.

Handles CRUD, test connection, and serialization for GitRepoConfig.
"""

import logging
from typing import Any, Optional

from django.conf import settings

from backend.core.models.git_repo_config import GitRepoConfig
from backend.errors.exceptions import (
    GitConfigAlreadyExistsException,
    GitConfigurationNotFoundException,
    GitConnectionFailedException,
)
from backend.core.services.git_service import get_git_service, GitHubService

logger = logging.getLogger(__name__)

DEFAULT_REPO_URL = getattr(settings, "GIT_DEFAULT_REPO_URL", "")
DEFAULT_REPO_TOKEN_SETTING = "GIT_DEFAULT_REPO_TOKEN"


# ------------------------------------------------------------------
# Read
# ------------------------------------------------------------------

def get_config(project_id: str) -> Optional[dict[str, Any]]:
    config = _get_config_by_project(project_id)
    if not config:
        return None
    return _serialize_config(config)


# ------------------------------------------------------------------
# Create / Update
# ------------------------------------------------------------------

def save_config(project_id: str, config_data: dict[str, Any]) -> dict[str, Any]:
    repo_type = config_data.get("repo_type", "custom")
    if repo_type == "default":
        config_data = _prepare_default_config(config_data)

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
    config = GitRepoConfig.objects.filter(project_id=project_id, is_deleted=False).first()
    if not config:
        raise GitConfigurationNotFoundException(project_id=project_id)
    config.is_deleted = True
    config.is_active = False
    config.save()


# ------------------------------------------------------------------
# Test Connection
# ------------------------------------------------------------------

def test_connection(project_id: str, config_data: dict[str, Any]) -> dict[str, Any]:
    repo_type = config_data.get("repo_type", "custom")
    if repo_type == "default":
        config_data = _prepare_default_config(config_data)

    repo_url = config_data.get("repo_url", "")
    credentials = config_data.get("credentials", {})
    token = credentials.get("token", "")

    if not repo_url or "github.com" not in repo_url:
        raise GitConnectionFailedException(error_message=f"Invalid or unsupported repository URL: {repo_url}")

    service = GitHubService(repo_url, token, config_data.get("branch_name", "main"))
    return service.test_connection()


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
        connection_status=config_data.get("connection_status", "pending"),
    )
    config.save()
    return config


def _update_config(project_id: str, config_data: dict[str, Any]) -> GitRepoConfig:
    config = GitRepoConfig.objects.filter(project_id=project_id, is_deleted=False).first()
    if not config:
        raise GitConfigurationNotFoundException(project_id=project_id)
    for field in ("repo_type", "repo_url", "auth_type", "branch_name", "base_path", "connection_status", "error_message"):
        if field in config_data:
            setattr(config, field, config_data[field])
    if "credentials" in config_data:
        config.encrypted_credentials = config_data["credentials"]
    config.save()
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
        "last_synced_at": config.last_synced_at.isoformat() if config.last_synced_at else None,
        "created_by": config.created_by,
        "last_modified_by": config.last_modified_by,
        "created_at": config.created_at.isoformat() if config.created_at else None,
        "modified_at": config.modified_at.isoformat() if config.modified_at else None,
    }
