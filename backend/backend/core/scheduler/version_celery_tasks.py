"""Celery tasks for version history: combined YAML auto-commit and manual commit.

Registered via CELERY_IMPORTS in settings.
"""

import logging

from celery import shared_task
from django.db import transaction

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Combined YAML auto-commit
# ------------------------------------------------------------------

_ACTION_MAP = {
    "model_created": "add {name} model",
    "model_deleted": "remove {name} model",
    "transform_updated": "update {name} transformations",
    "config_updated": "update {name} configuration",
    "presentation_updated": "update {name} presentation",
}


def _build_commit_message(trigger_action: str) -> str:
    if ":" in trigger_action:
        action, name = trigger_action.split(":", 1)
        template = _ACTION_MAP.get(action)
        if template:
            return template.format(name=name)
    if trigger_action.startswith("rollback_to_v"):
        return f"revert to version {trigger_action.split('v', 1)[-1]}"
    if trigger_action.startswith("execute_version_v"):
        return f"execute version {trigger_action.split('v', 1)[-1]}"
    return trigger_action


def _load_project_and_config(project_id: str):
    from backend.core.models.git_repo_config import GitRepoConfig
    from backend.core.models.project_details import ProjectDetails

    project = ProjectDetails.objects.filter(project_uuid=project_id).first()
    if not project:
        return None, None
    config = GitRepoConfig.objects.filter(
        project_id=project_id, is_deleted=False, is_active=True,
    ).first()
    return project, config


def _load_models_data(project):
    from backend.core.models.config_models import ConfigModels

    return {
        cm.model_name: cm.model_data
        for cm in ConfigModels.objects.filter(project_instance=project)
        if cm.model_data
    }


def _push_and_create_version(project, config, models_data, commit_message,
                              author_info, is_auto_commit):
    from datetime import datetime

    from django.db.models import Max

    from backend.core.models.model_version import ModelVersion
    from backend.core.services.git_service import get_git_service
    from backend.core.services.yaml_serializer import (
        get_project_yaml_path,
        serialize_project_to_yaml,
    )

    author_info = author_info or {}
    name = author_info.get("name") or author_info.get("username", "")
    email = author_info.get("email", "")

    yaml_content = serialize_project_to_yaml(
        project_name=project.project_name,
        models_data=models_data,
        author=name or None,
        timestamp=datetime.utcnow().isoformat() + "Z",
    )

    file_path = get_project_yaml_path(
        project_name=config.git_project_folder or project.project_name,
    )

    git_svc = get_git_service(config)
    result = git_svc.push_combined_file(
        file_path=file_path,
        content=yaml_content,
        commit_message=commit_message,
        author_name=name or None,
        author_email=email or None,
    )

    with transaction.atomic():
        last = ModelVersion.objects.filter(
            project_instance=project,
        ).aggregate(max_v=Max("version_number"))["max_v"] or 0
        next_version = last + 1

        ModelVersion.objects.filter(
            project_instance=project,
        ).update(is_current=False)

        ModelVersion.objects.create(
            project_instance=project,
            version_number=next_version,
            commit_message=commit_message,
            committed_by=author_info,
            is_auto_commit=is_auto_commit,
            is_current=True,
            git_commit_sha=result.get("commit_sha", "")[:40],
            git_branch_name=config.branch_name,
            git_sync_status="synced",
            model_data=None,
            organization=project.organization,
        )

    return {
        "version_number": next_version,
        "commit_sha": result.get("commit_sha", ""),
        "file_path": file_path,
    }


def _run_auto_commit(project_id: str, trigger_action: str,
                     author_info: dict | None = None) -> dict:
    """Plain function — core auto-commit logic, no Celery decorator."""
    project, config = _load_project_and_config(project_id)
    if not project or not config:
        logger.info("auto_commit skipped — no git config for project %s", project_id)
        return {"status": "skipped", "reason": "no project or git config"}

    models_data = _load_models_data(project)
    if not models_data:
        return {"status": "skipped", "reason": "no models"}

    commit_message = _build_commit_message(trigger_action)

    result = _push_and_create_version(
        project, config, models_data, commit_message,
        author_info, is_auto_commit=True,
    )
    logger.info(
        "auto_commit OK: project=%s v%d sha=%s",
        project_id, result["version_number"], result["commit_sha"][:8],
    )
    return {"status": "committed", **result}


@shared_task(
    bind=True, max_retries=3, default_retry_delay=10, acks_late=True,
)
def auto_commit_to_github(self, project_id: str, trigger_action: str,
                           author_info: dict | None = None) -> dict:
    """Push combined YAML to git on every model save (Celery wrapper)."""
    try:
        return _run_auto_commit(project_id, trigger_action, author_info)
    except Exception as exc:
        logger.exception("auto_commit_to_github failed: %s", project_id)
        raise self.retry(exc=exc)


def _run_manual_commit(project_id: str, title: str,
                       description: str = "",
                       author_info: dict | None = None) -> dict:
    """Plain function — core manual-commit logic, no Celery decorator."""
    project, config = _load_project_and_config(project_id)
    if not project or not config:
        logger.info("manual_commit skipped — no git config for project %s", project_id)
        return {"status": "skipped", "reason": "no project or git config"}

    models_data = _load_models_data(project)
    if not models_data:
        return {"status": "skipped", "reason": "no models"}

    commit_message = f"{title}\n\n{description}".strip() if description else title

    result = _push_and_create_version(
        project, config, models_data, commit_message,
        author_info, is_auto_commit=False,
    )
    logger.info(
        "manual_commit OK: project=%s v%d sha=%s",
        project_id, result["version_number"], result["commit_sha"][:8],
    )
    return {"status": "committed", **result}


@shared_task(
    bind=True, max_retries=3, default_retry_delay=10, acks_late=True,
)
def manual_commit_to_github(self, project_id: str, title: str,
                              description: str = "",
                              author_info: dict | None = None) -> dict:
    """Push combined YAML to git for an explicit user commit (Celery wrapper)."""
    try:
        return _run_manual_commit(project_id, title, description, author_info)
    except Exception as exc:
        logger.exception("manual_commit_to_github failed: %s", project_id)
        raise self.retry(exc=exc)
