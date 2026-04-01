"""Celery tasks for version history: git sync, draft auto-save, stale cleanup.

Registered via CELERY_IMPORTS in settings.
"""

import logging

from celery import shared_task
from django.db import transaction
from django.db.models import QuerySet
from django.utils import timezone

from backend.utils.tenant_context import _get_tenant_context, clear_tenant_context

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Git sync
# ------------------------------------------------------------------

@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def push_version_to_git(self, version_id: str, organization_id: str = "") -> dict:
    """Push a committed ModelVersion's YAML to the configured git repo."""
    logger.info("push_version_to_git started for version_id=%s (attempt %d/%d)",
                version_id, self.request.retries + 1, self.max_retries + 1)

    if organization_id:
        _get_tenant_context().set_tenant(organization_id)
    try:
        return _execute_git_push(self, version_id)
    finally:
        clear_tenant_context()


def _execute_git_push(task, version_id: str) -> dict:
    from backend.core.models.git_repo_config import GitRepoConfig
    from backend.core.models.model_version import ModelVersion
    from backend.core.services.git_service import get_git_service
    from backend.core.services.yaml_serializer import build_git_file_path, serialize_model_to_yaml
    from backend.errors.exceptions import (
        GitConnectionFailedException, GitPushFailedException,
        GitRateLimitException, GitTokenExpiredException,
        UnsupportedGitProviderException,
    )

    try:
        version = ModelVersion.objects.select_related("config_model", "project_instance").get(version_id=version_id)
    except ModelVersion.DoesNotExist:
        logger.error("ModelVersion %s not found — aborting git sync", version_id)
        return {"status": "error", "message": "Version not found"}

    project = version.project_instance
    config_model = version.config_model
    project_id = str(project.project_uuid)

    config = GitRepoConfig.objects.filter(
        project_id=project_id, is_deleted=False, is_active=True,
    ).first()
    if not config:
        version.git_sync_status = "not_applicable"
        version.save(update_fields=["git_sync_status"])
        return {"status": "not_applicable"}

    model_name = config_model.model_name if config_model else "project"
    project_name = project.project_name
    yaml_content = serialize_model_to_yaml(
        model_data=version.model_data or {}, model_name=model_name,
        project_name=project_name, version_number=version.version_number,
    )
    org_id = str(project.organization_id) if project.organization_id else ""
    file_path = build_git_file_path(config=config, model_name=model_name, org_id=org_id, project_id=project_id)

    try:
        service = get_git_service(config)
        _existing, existing_sha = service.get_file(file_path)
        commit_message = (
            f"v{version.version_number}: {version.commit_message}"
            if version.commit_message else f"v{version.version_number}: {model_name}"
        )
        result = service.put_file(path=file_path, content=yaml_content, message=commit_message, sha=existing_sha)

        commit_sha = result.get("commit_sha", "")
        version.git_sync_status = "synced"
        version.git_commit_sha = commit_sha[:40]
        version.save(update_fields=["git_sync_status", "git_commit_sha"])
        config.connection_status = "connected"
        config.last_synced_at = timezone.now()
        config.error_message = ""
        config.save(update_fields=["connection_status", "last_synced_at", "error_message"])
        logger.info("Git sync SUCCESS: %s v%d -> %s", model_name, version.version_number, file_path)
        return {"status": "synced", "commit_sha": commit_sha, "file_path": file_path}

    except GitRateLimitException:
        _retry_or_fail(task, version, config, "GitHub API rate limit exceeded", countdown=900)
    except GitTokenExpiredException:
        _mark_failed(version, config, "Git authentication token expired")
        return {"status": "failed", "message": "Token expired"}
    except UnsupportedGitProviderException as exc:
        _mark_failed(version, config, str(exc))
        return {"status": "failed", "message": "Unsupported provider"}
    except (GitConnectionFailedException, GitPushFailedException) as exc:
        _retry_or_fail(task, version, config, str(exc))
    except Exception as exc:
        logger.exception("Unexpected error in git sync for version %s", version_id)
        _retry_or_fail(task, version, config, f"Unexpected error: {exc}", countdown=120)

    return {"status": "failed"}


def _retry_or_fail(task, version, config, error_msg, countdown=None):
    is_final = task.request.retries >= task.max_retries
    if is_final:
        _mark_failed(version, config, error_msg)
        return
    config.error_message = error_msg[:500]
    config.save(update_fields=["error_message"])
    logger.warning("Git sync attempt %d/%d failed for version %s: %s",
                   task.request.retries + 1, task.max_retries + 1, version.version_id, error_msg[:200])
    raise task.retry(countdown=countdown)


def _mark_failed(version, config, error_msg):
    version.git_sync_status = "failed"
    version.save(update_fields=["git_sync_status"])
    config.connection_status = "error"
    config.error_message = error_msg[:500]
    config.save(update_fields=["connection_status", "error_message"])
    logger.warning("Git sync FAILED for version %s: %s", version.version_id, error_msg[:200])


@shared_task
def retry_failed_git_syncs() -> dict:
    """Periodic task: retry all versions with failed git sync status."""
    from backend.core.models.git_repo_config import GitRepoConfig
    from backend.core.models.model_version import ModelVersion

    failed_versions = (
        QuerySet(model=ModelVersion)
        .filter(git_sync_status="failed")
        .select_related("project_instance")
    )
    dispatched = 0
    for version in failed_versions:
        has_config = (
            QuerySet(model=GitRepoConfig)
            .filter(project_id=version.project_instance_id, is_deleted=False, is_active=True)
            .exists()
        )
        if has_config:
            org_id = str(version.project_instance.organization_id) if version.project_instance.organization_id else ""
            push_version_to_git.delay(str(version.version_id), organization_id=org_id)
            dispatched += 1
    if dispatched:
        logger.info("Dispatched %d retry tasks for failed git syncs", dispatched)
    return {"dispatched": dispatched}


# ------------------------------------------------------------------
# Draft auto-save and cleanup
# ------------------------------------------------------------------

@shared_task(
    name="core.auto_save_dirty_drafts",
    bind=True, max_retries=2, default_retry_delay=5, acks_late=True,
)
def auto_save_dirty_drafts(self):
    """Periodic task: flush all dirty drafts (runs every 30s via celery-beat)."""
    from backend.core.models.user_draft import UserDraft

    dirty_drafts = UserDraft.objects.filter(is_dirty=True).select_for_update(skip_locked=True)
    now = timezone.now()
    saved_count = 0
    with transaction.atomic():
        for draft in dirty_drafts:
            draft.is_dirty = False
            draft.last_auto_save = now
            draft.save(update_fields=["is_dirty", "last_auto_save", "modified_at"])
            saved_count += 1
    if saved_count:
        logger.info("Auto-saved %d dirty draft(s).", saved_count)
    return {"saved_count": saved_count}


@shared_task(
    name="core.auto_save_single_draft",
    bind=True, max_retries=3, default_retry_delay=2, acks_late=True,
)
def auto_save_single_draft(self, draft_id: str):
    """Event-triggered task: auto-save a specific draft immediately."""
    from backend.core.models.user_draft import UserDraft

    try:
        with transaction.atomic():
            draft = UserDraft.objects.select_for_update(skip_locked=True).get(draft_id=draft_id)
            if draft.is_dirty:
                draft.is_dirty = False
                draft.last_auto_save = timezone.now()
                draft.save(update_fields=["is_dirty", "last_auto_save", "modified_at"])
                logger.info("Event auto-save completed for draft %s.", draft_id)
    except Exception:
        logger.warning("Draft %s not found for auto-save.", draft_id)


@shared_task(
    name="core.cleanup_stale_drafts",
    bind=True, max_retries=1, acks_late=True,
)
def cleanup_stale_drafts(self):
    """Periodic task: remove drafts older than the retention period (runs daily)."""
    from backend.core.models.user_draft import DRAFT_STALE_DAYS, UserDraft

    cutoff = timezone.now() - timezone.timedelta(days=DRAFT_STALE_DAYS)
    stale_qs = UserDraft.objects.filter(modified_at__lt=cutoff, last_auto_save__lt=cutoff)
    count = stale_qs.count()
    if count:
        stale_qs.delete()
        logger.info("Cleaned up %d stale draft(s).", count)
    return {"deleted_count": count}
