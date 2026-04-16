"""
Celery tasks for the Job Scheduler.

Entry-point task: ``trigger_scheduled_run``
  – called by django-celery-beat (periodic) and by the manual "Run now" API.
"""

import logging
import signal
import threading
import uuid
from contextlib import contextmanager
from datetime import timedelta

from celery import shared_task
from celery.exceptions import SoftTimeLimitExceeded
from django.core.mail import send_mail
from django.conf import settings
from django.utils import timezone

from backend.core.scheduler.models import TaskRunHistory, UserTaskDetails
from backend.core.scheduler.task_constant import TaskStatus

# Optional Slack integration (still in pluggable_apps)
try:
    from pluggable_apps.slack_integration.controllers import SlackIntegrationController
except (ImportError, RuntimeError):
    # RuntimeError can occur if slack_integration app is not in INSTALLED_APPS
    SlackIntegrationController = None

logger = logging.getLogger(__name__)

# Default max duration before a job is considered stuck (1 hour)
DEFAULT_STUCK_JOB_THRESHOLD_SECONDS = 3600


# ---------------------------------------------------------------------------
# Timeout helper (works with both prefork and thread pools)
# ---------------------------------------------------------------------------

class _RunTimeout(Exception):
    """Raised when a job exceeds its configured timeout."""


@contextmanager
def _timeout_guard(seconds: int):
    """Context manager that raises ``_RunTimeout`` after *seconds*.

    Uses SIGALRM on the main thread (prefork pool) and falls back to a
    threading.Timer for worker threads (thread/gevent/eventlet pools).
    A value of 0 disables the timeout.
    """
    if seconds <= 0:
        yield
        return

    is_main_thread = threading.current_thread() is threading.main_thread()

    if is_main_thread:
        def _handler(signum, frame):
            raise _RunTimeout(f"Job exceeded timeout of {seconds}s")

        old = signal.signal(signal.SIGALRM, _handler)
        signal.alarm(seconds)
        try:
            yield
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old)
    else:
        # Thread pool: use a timer that sets a flag (best-effort).
        timed_out = threading.Event()

        def _timer_expired():
            timed_out.set()

        timer = threading.Timer(seconds, _timer_expired)
        timer.daemon = True
        timer.start()
        try:
            yield
            if timed_out.is_set():
                raise _RunTimeout(f"Job exceeded timeout of {seconds}s")
        finally:
            timer.cancel()


# ---------------------------------------------------------------------------
# Notification helpers
# ---------------------------------------------------------------------------

def _send_slack_notification(user_task: UserTaskDetails, run: TaskRunHistory, success: bool):
    """Send Slack notification via the org-level Slack integration (if configured)."""
    if SlackIntegrationController is None:
        return

    try:
        if success:
            duration = 0.0
            if run.start_time and run.end_time:
                duration = (run.end_time - run.start_time).total_seconds()
            SlackIntegrationController.notify_job_success(
                job_name=user_task.task_name,
                duration_seconds=duration,
            )
        else:
            SlackIntegrationController.notify_job_failed(
                job_name=user_task.task_name,
                error_message=run.error_message or "Unknown error",
            )
    except Exception:
        logger.exception("Slack notification failed for task %s", user_task.id)


def _send_notification(user_task: UserTaskDetails, run: TaskRunHistory, success: bool):
    """Send email + Slack notifications for a completed job run."""
    should_notify = (success and user_task.notify_on_success) or (
        not success and user_task.notify_on_failure
    )

    # ── Email ──────────────────────────────────────────────────────────
    if should_notify and user_task.notification_emails:
        status_label = "succeeded" if success else "FAILED"
        subject = f"[Visitran] Job '{user_task.task_name}' {status_label}"

        body_lines = [
            f"Job:     {user_task.task_name}",
            f"Status:  {run.status}",
            f"Started: {run.start_time}",
            f"Ended:   {run.end_time}",
        ]
        if run.error_message:
            body_lines.append(f"Error:   {run.error_message}")

        try:
            send_mail(
                subject=subject,
                message="\n".join(body_lines),
                from_email=getattr(settings, "DEFAULT_FROM_EMAIL", "noreply@visitran.io"),
                recipient_list=user_task.notification_emails,
                fail_silently=True,
            )
        except Exception:
            logger.exception("Failed to send email notification for task %s", user_task.id)

    # ── Slack (uses org-level webhook config) ──────────────────────────
    _send_slack_notification(user_task, run, success)


# ---------------------------------------------------------------------------
# BASE_RESULT cleanup helper
# ---------------------------------------------------------------------------

def _clear_base_result():
    """Clear the module-level BASE_RESULT global to prevent stale data across worker reuse."""
    try:
        from visitran.events.printer import BASE_RESULT
        BASE_RESULT.clear()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Job chaining helper
# ---------------------------------------------------------------------------

def _trigger_chained_job(user_task: UserTaskDetails, user_id: int, organization_id: str):
    """If a downstream job is configured, fire it."""
    next_task = user_task.trigger_on_complete
    if not next_task:
        return

    logger.info(
        "Job chain: %s completed -> triggering %s",
        user_task.task_name,
        next_task.task_name,
    )

    trigger_scheduled_run.delay(
        user_task_id=next_task.id,
        user_id=user_id,
        organization_id=organization_id,
    )


# ---------------------------------------------------------------------------
# Main Celery task
# ---------------------------------------------------------------------------

@shared_task(
    name="backend.core.scheduler.celery_tasks.trigger_scheduled_run",
    bind=True,
    acks_late=True,
    max_retries=0,  # we handle retries ourselves
)
def trigger_scheduled_run(
    self,
    *,
    user_task_id: int,
    user_id: int,
    organization_id: str = None,
    models_override: list = None,
    trigger: str = "scheduled",
):
    """Execute a scheduled Visitran run.

    This is the Celery task wired to ``Task.SCHEDULER_JOB``.

    Args:
        models_override: If provided, execute only these model names (plus
            their downstream dependents) instead of every model in
            ``user_task.model_configs``. Used by the Quick Deploy flow to
            run a single model against the job's environment.
        trigger: "scheduled" (default, used by Celery beat) or "manual"
            (used by ad-hoc dispatch from trigger_task_once*). Stored in
            TaskRunHistory.kwargs alongside ``scope`` so Run History can
            distinguish scheduled vs on-demand runs.
    """
    scope = "model" if models_override else "job"
    from backend.application.context.application import ApplicationContext
    from backend.utils.tenant_context import _get_tenant_context
    from backend.core.models.user_model import User
    from backend.core.models.organization_model import Organization

    # Set up tenant context for background task
    # NOTE: organization_id arg is the Organization table's PK (id), not the
    # organization_id field.  We must look up the actual organization_id string
    # that TenantContext / DefaultOrganizationManagerMixin expects.
    try:
        user = User.objects.get(id=user_id)
        tenant_slug = "default_org"
        if organization_id:
            try:
                org = Organization.objects.get(id=organization_id)
                tenant_slug = org.organization_id
            except Organization.DoesNotExist:
                logger.warning("Organization with pk=%s not found, falling back to default_org", organization_id)
        ctx = _get_tenant_context()
        ctx.set_user({"email": user.email, "username": user.username, "id": user.id})
        ctx.set_tenant(tenant_slug)
        logger.info("Set tenant context: user=%s, org_pk=%s, tenant=%s", user.username, organization_id, tenant_slug)
    except User.DoesNotExist:
        logger.warning(f"User {user_id} not found, using default context")

    # ── Load task record ──────────────────────────────────────────────
    try:
        user_task = UserTaskDetails.objects.select_related(
            "environment", "project", "trigger_on_complete",
        ).get(id=user_task_id)
    except UserTaskDetails.DoesNotExist:
        logger.error("UserTaskDetails %s not found – aborting.", user_task_id)
        return

    # ── Determine retry state ─────────────────────────────────────────
    retry_num = 0
    existing_runs = TaskRunHistory.objects.filter(
        user_task_detail=user_task, status="RETRY",
    ).count()
    retry_num = existing_runs

    # ── Create run-history entry ──────────────────────────────────────
    # Note: organization is automatically set by DefaultOrganizationMixin from tenant context
    run_kwargs = {
        "user_task_id": user_task_id,
        "user_id": user_id,
        "model_configs": user_task.model_configs,
        "trigger": trigger,
        "scope": scope,
    }
    if models_override:
        run_kwargs["models_override"] = list(models_override)

    run = TaskRunHistory.objects.create(
        task_id=self.request.id or f"manual-{user_task_id}-{uuid.uuid4().hex[:8]}",
        retry_num=retry_num,
        status="STARTED",
        start_time=timezone.now(),
        user_task_detail=user_task,
        kwargs=run_kwargs,
        trigger=trigger,
        scope=scope,
    )

    # ── Mark task as running ──────────────────────────────────────────
    user_task.status = TaskStatus.RUNNING
    user_task.task_run_time = run.start_time
    user_task.save(update_fields=["status", "task_run_time"])

    # Slack "started" notification
    if SlackIntegrationController is not None:
        try:
            env_name = getattr(user_task.environment, "name", str(user_task.environment))
            SlackIntegrationController.notify_job_started(
                job_name=user_task.task_name,
                environment_name=env_name,
            )
        except Exception:
            logger.debug("Slack start notification skipped (no integration configured)")

    success = False
    error_msg = ""

    try:
        # ── Build execution context ───────────────────────────────────
        project_id = str(user_task.project.project_uuid)
        environment_id = str(user_task.environment.environment_id)

        app_context = ApplicationContext(
            project_id=project_id,
            environment_id=environment_id,
        )

        # Wire model_configs into the Visitran context for per-model overrides
        ctx = app_context.visitran_context
        ctx.model_configs = user_task.model_configs or {}

        # Build include/exclude lists from model_configs for backward compatibility
        ctx._select_models = [
            name for name, cfg in ctx.model_configs.items()
            if cfg.get("enabled", True)
        ]
        ctx._exclude_models = [
            name for name, cfg in ctx.model_configs.items()
            if not cfg.get("enabled", True)
        ]

        # ── Execute with timeout guard ────────────────────────────────
        timeout = user_task.run_timeout_seconds or 0

        with _timeout_guard(timeout):
            if models_override:
                app_context.execute_visitran_run_command(
                    environment_id=environment_id,
                    current_models=list(models_override),
                )
            else:
                app_context.execute_visitran_run_command(environment_id=environment_id)

        # ── Capture execution metrics from BASE_RESULT ──────────────
        try:
            from visitran.events.printer import BASE_RESULT

            # Snapshot and immediately clear the global to prevent stale
            # data leaking into a subsequent run on the same worker process.
            results_snapshot = list(BASE_RESULT)
            BASE_RESULT.clear()

            def _clean_name(raw):
                if "'" in raw:
                    return raw.split("'")[1].split(".")[-1]
                return raw

            user_results = [
                r for r in results_snapshot
                if not _clean_name(r.node_name).startswith("Source")
            ]
            run.result = {
                "models": [
                    {
                        "name": _clean_name(r.node_name),
                        "status": r.status,
                        "end_status": r.end_status,
                        "sequence": r.sequence_num,
                    }
                    for r in user_results
                ],
                "total": len(user_results),
                "passed": sum(1 for r in user_results if r.end_status == "OK"),
                "failed": sum(1 for r in user_results if r.end_status == "FAIL"),
            }
        except Exception:
            _clear_base_result()
            logger.debug("Could not capture BASE_RESULT metrics", exc_info=True)

        # ── Mark success ──────────────────────────────────────────────
        success = True
        run.status = "SUCCESS"
        run.end_time = timezone.now()
        run.save(update_fields=["status", "end_time", "result"])

        user_task.status = TaskStatus.SUCCESS
        user_task.task_completion_time = run.end_time
        user_task.prev_run_status = TaskStatus.SUCCESS
        user_task.save(update_fields=["status", "task_completion_time", "prev_run_status"])

    except (_RunTimeout, SoftTimeLimitExceeded) as exc:
        error_msg = str(exc) if str(exc) else f"Job exceeded timeout of {timeout}s"
        logger.warning("Job %s timed out: %s", user_task.task_name, error_msg)
        _clear_base_result()
        _mark_failure(run, user_task, error_msg)

    except Exception as exc:
        error_msg = str(exc)
        logger.exception("Job %s failed: %s", user_task.task_name, error_msg)
        _clear_base_result()
        _mark_failure(run, user_task, error_msg)

    # ── Retry logic ───────────────────────────────────────────────────
    if not success and user_task.max_retries > 0 and retry_num < user_task.max_retries:
        run.status = "RETRY"
        run.save(update_fields=["status"])

        user_task.status = TaskStatus.RETRYING
        user_task.save(update_fields=["status"])

        logger.info(
            "Retrying job %s (attempt %d/%d)",
            user_task.task_name,
            retry_num + 1,
            user_task.max_retries,
        )
        retry_kwargs = {
            "user_task_id": user_task_id,
            "user_id": user_id,
            "organization_id": organization_id,
            "trigger": trigger,
        }
        if models_override:
            retry_kwargs["models_override"] = list(models_override)
        trigger_scheduled_run.apply_async(
            kwargs=retry_kwargs,
            countdown=30 * (retry_num + 1),  # progressive backoff
        )
        return

    # ── Notifications ─────────────────────────────────────────────────
    _send_notification(user_task, run, success)

    # ── Job chaining (only on success) ────────────────────────────────
    if success:
        _trigger_chained_job(user_task, user_id, organization_id)


def _mark_failure(run: TaskRunHistory, user_task: UserTaskDetails, error_msg: str):
    """Helper to mark a run and its parent task as failed."""
    try:
        from visitran.events.printer import BASE_RESULT

        def _clean(raw):
            return raw.split("'")[1].split(".")[-1] if "'" in raw else raw

        user_results = [
            r for r in BASE_RESULT if not _clean(r.node_name).startswith("Source")
        ]
        run.result = {
            "models": [
                {
                    "name": _clean(r.node_name),
                    "status": r.status,
                    "end_status": r.end_status,
                    "sequence": r.sequence_num,
                }
                for r in user_results
            ],
            "total": len(user_results),
            "passed": sum(1 for r in user_results if r.end_status == "OK"),
            "failed": sum(1 for r in user_results if r.end_status == "FAIL"),
        }
    except Exception:
        pass
    run.status = "FAILURE"
    run.end_time = timezone.now()
    run.error_message = error_msg
    run.save(update_fields=["status", "end_time", "error_message", "result"])

    user_task.status = TaskStatus.FAILED
    user_task.task_completion_time = run.end_time
    user_task.prev_run_status = TaskStatus.FAILED
    user_task.save(update_fields=["status", "task_completion_time", "prev_run_status"])


# ---------------------------------------------------------------------------
# Stuck job recovery
# ---------------------------------------------------------------------------

@shared_task(name="backend.core.scheduler.celery_tasks.recover_stuck_jobs")
def recover_stuck_jobs():
    """Periodic cleanup task: mark jobs stuck in RUNNING as FAILED.

    A job is considered stuck if:
    - status is RUNNING, AND
    - it has been running longer than its run_timeout_seconds (or the
      default threshold if no timeout is configured).

    Uses the unfiltered queryset to check across all organizations.
    """
    now = timezone.now()

    # Bypass the DefaultOrganizationManagerMixin which filters by tenant context.
    # Recovery must check ALL orgs — use the base Manager queryset directly.
    from django.db.models import Manager
    base_qs = Manager.get_queryset(UserTaskDetails.objects)
    stuck_tasks = base_qs.filter(
        status=TaskStatus.RUNNING,
    )

    recovered = 0
    for task in stuck_tasks:
        threshold = task.run_timeout_seconds or DEFAULT_STUCK_JOB_THRESHOLD_SECONDS
        # Add a grace period (2x the timeout, minimum 10 minutes)
        grace = max(threshold * 2, 600)
        cutoff = now - timedelta(seconds=grace)

        if task.task_run_time and task.task_run_time < cutoff:
            error_msg = (
                f"Job recovered by cleanup: stuck in RUNNING since "
                f"{task.task_run_time.isoformat()} (threshold: {grace}s)"
            )
            logger.warning("Recovering stuck job %s: %s", task.task_name, error_msg)

            task.status = TaskStatus.FAILED
            task.task_completion_time = now
            task.prev_run_status = TaskStatus.FAILED
            task.save(update_fields=["status", "task_completion_time", "prev_run_status"])

            # Also close the open TaskRunHistory record
            open_runs = Manager.get_queryset(TaskRunHistory.objects).filter(
                user_task_detail=task,
                status="STARTED",
                end_time__isnull=True,
            )
            open_runs.update(
                status="FAILURE",
                end_time=now,
                error_message=error_msg,
            )
            recovered += 1

    if recovered:
        logger.info("Recovered %d stuck job(s)", recovered)
    return recovered
