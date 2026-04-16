import json
import logging
import uuid
from datetime import timedelta

from django.utils import timezone
from django_celery_beat.models import CrontabSchedule, IntervalSchedule, PeriodicTask
from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from backend.core.models.environment_models import EnvironmentModels
from backend.core.models.project_details import ProjectDetails
from backend.core.scheduler.models import TaskRunHistory, UserTaskDetails
from backend.core.scheduler.serializer import TaskRunHistorySerializer
from backend.core.scheduler.task_constant import Task, TaskStatus, TaskType
from backend.utils.tenant_context import get_organization

logger = logging.getLogger(__name__)



def _compute_next_run_time(periodic, last_run_at):
    """Derive the next run time from a PeriodicTask's schedule."""
    if not periodic or not periodic.enabled:
        return None
    try:
        schedule = periodic.schedule
        reference = last_run_at or periodic.last_run_at or timezone.now()
        remaining = schedule.remaining_estimate(reference)
        return timezone.now() + remaining
    except Exception:
        logger.debug("Failed to compute next_run_time for %s", periodic, exc_info=True)
        return None


def _is_valid_project_id(project_id):
    """Check if project_id is a real UUID (not a placeholder like '_all' or 'all')."""
    try:
        uuid.UUID(str(project_id))
        return True
    except ValueError:
        return False


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def get_model_columns(request, project_id, model_name):
    """Get columns for a model (for incremental config UI).

    Returns:
        - destination_columns: Final output columns of the model (for unique_key selection)
        - source_columns: Columns from the source table (for filter column selection)
    """
    try:
        from backend.application.context.application import ApplicationContext
        from backend.application.context.no_code_model import NoCodeModel

        # Normalize model name
        model_name = model_name.replace(" ", "_")

        no_code_model = NoCodeModel(project_id=project_id)

        # Get destination columns (model output columns)
        result = no_code_model.get_transformation_columns(
            model_name=model_name,
            transformation_id=None,
            transformation_type="current",
        )

        destination_columns = []
        column_names_data = result.get("column_names", {})
        if isinstance(column_names_data, dict):
            raw_columns = column_names_data.get("current") or column_names_data.get("visible", [])
        else:
            raw_columns = column_names_data if isinstance(column_names_data, list) else []

        column_descriptions = result.get("column_description", {}) or result.get("all_column_description", {})

        if isinstance(raw_columns, list):
            for col_name in raw_columns:
                if isinstance(col_name, str):
                    col_desc = column_descriptions.get(col_name, {})
                    destination_columns.append({
                        "column_name": col_name,
                        "data_type": col_desc.get("data_type") or col_desc.get("type", "unknown"),
                    })
                elif isinstance(col_name, dict):
                    destination_columns.append({
                        "column_name": col_name.get("column_name") or col_name.get("name"),
                        "data_type": col_name.get("data_type") or col_name.get("type", "unknown"),
                    })

        # Get source columns (from source table in database)
        source_columns = []
        try:
            # Get model config to find source table
            model_config = no_code_model.get_model_config(model_name)
            source_info = model_config.get("source", {})
            source_schema = source_info.get("schema_name", "")
            source_table = source_info.get("table_name", "")

            if source_schema and source_table:
                # Use ApplicationContext to get source table columns from database
                app_context = ApplicationContext(project_id=project_id)
                source_table_columns = app_context.get_table_columns(source_schema, source_table)

                for col in source_table_columns:
                    source_columns.append({
                        "column_name": col.get("column_name"),
                        "data_type": col.get("data_type") or col.get("column_dbtype", "unknown"),
                    })
        except Exception as e:
            logger.warning(f"Could not fetch source columns for {model_name}: {e}")
            # Fall back to destination columns if source fetch fails
            source_columns = destination_columns.copy()

        return Response(
            {
                "destination_columns": destination_columns,
                "source_columns": source_columns,
                # Keep 'columns' for backward compatibility
                "columns": destination_columns,
            },
            status=status.HTTP_200_OK,
        )

    except ProjectDetails.DoesNotExist:
        return Response(
            {"error": "Project not found"},
            status=status.HTTP_404_NOT_FOUND,
        )
    except FileNotFoundError:
        return Response(
            {"error": f"Model '{model_name}' not found"},
            status=status.HTTP_404_NOT_FOUND,
        )
    except Exception as e:
        logger.error(f"Error getting model columns for {model_name}: {e}")
        return Response(
            {"error": str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


def _serialize_task(task):
    """Serialize a UserTaskDetails instance for API response."""
    periodic = task.periodic_task
    task_type = TaskType.CRON

    interval_data = None
    cron_data = None

    if periodic:
        if periodic.interval:
            task_type = TaskType.INTERVAL
            interval_data = {
                "every": periodic.interval.every,
                "period": periodic.interval.period.capitalize(),
            }
        elif periodic.crontab:
            task_type = TaskType.CRON
            parts = [
                periodic.crontab.minute,
                periodic.crontab.hour,
                periodic.crontab.day_of_month,
                periodic.crontab.month_of_year,
                periodic.crontab.day_of_week,
            ]
            cron_data = {
                "cron_expression": " ".join(str(p) for p in parts),
                "timezone": str(periodic.crontab.timezone),
            }

    return {
        "user_task_id": task.id,
        "task_name": task.task_name,
        "task_status": task.status,
        "task_run_time": task.task_run_time,
        "task_completion_time": task.task_completion_time,
        "next_run_time": task.next_run_time or _compute_next_run_time(
            periodic, task.task_run_time
        ),
        "task_type": task_type,
        "description": task.description,
        "environment": {
            "id": str(task.environment.environment_id),
            "name": task.environment.environment_name,
            "type": task.environment.deployment_type,
        }
        if task.environment
        else None,
        "project": {
            "id": str(task.project.project_uuid),
            "name": task.project.project_name,
        }
        if task.project
        else None,
        "periodic_task_details": {
            "id": periodic.id if periodic else None,
            "name": periodic.name if periodic else None,
            "enabled": periodic.enabled if periodic else False,
            "interval": interval_data,
            "cron": cron_data,
        },
        # Per-model deployment configuration
        "model_configs": task.model_configs,
        # Execution controls
        "run_timeout_seconds": task.run_timeout_seconds,
        "max_retries": task.max_retries,
        # Notifications
        "notify_on_failure": task.notify_on_failure,
        "notify_on_success": task.notify_on_success,
        "notification_emails": task.notification_emails,
        # Job chaining
        "trigger_on_complete": task.trigger_on_complete_id,
    }


def _build_task_kwargs(user_task, user, organization):
    """Build the kwargs dict for the Celery task."""
    return json.dumps(
        {
            "user_task_id": user_task.id,
            "user_id": user.id,
            "organization_id": str(organization.id) if organization else None,
        }
    )


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def create_periodic_task(request, project_id):
    """Create a new scheduled job."""
    try:
        data = request.data
        task_type = data.get("task_type", TaskType.CRON)
        task_name = data.get("task_name")
        environment_id = data.get("environment")
        description = data.get("description", "")
        enabled = data.get("enabled", True)

        if not task_name:
            return Response(
                {"error": "task_name is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not environment_id:
            return Response(
                {"error": "environment is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        project = ProjectDetails.objects.get(project_uuid=project_id)
        environment = EnvironmentModels.objects.get(environment_id=environment_id)

        # Create schedule
        schedule = None
        if task_type == TaskType.CRON:
            cron_expr = data.get("cron_expression", "30 * * * *")
            parts = cron_expr.strip().split()
            if len(parts) < 5:
                return Response(
                    {"error": "Invalid cron expression"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            schedule = CrontabSchedule.objects.filter(
                minute=parts[0],
                hour=parts[1],
                day_of_month=parts[2],
                month_of_year=parts[3],
                day_of_week=parts[4],
            ).first() or CrontabSchedule.objects.create(
                minute=parts[0],
                hour=parts[1],
                day_of_month=parts[2],
                month_of_year=parts[3],
                day_of_week=parts[4],
            )
        elif task_type == TaskType.INTERVAL:
            every = int(data.get("every", 30))
            period = data.get("period", "minutes").lower()
            schedule = IntervalSchedule.objects.filter(
                every=every, period=period
            ).first() or IntervalSchedule.objects.create(
                every=every, period=period
            )

        # Create PeriodicTask
        periodic_task_name = f"{task_name}_{uuid.uuid4().hex[:8]}"
        periodic_kwargs = {}

        if task_type == TaskType.CRON:
            periodic_kwargs["crontab"] = schedule
        else:
            periodic_kwargs["interval"] = schedule

        # Set last_run_at far enough in the past so the task triggers
        # immediately on first schedule. This also avoids the celery beat
        # crash when last_run_at is None for interval schedules.
        if task_type == TaskType.INTERVAL:
            every = int(data.get("every", 30))
            period = data.get("period", "minutes").lower()
            period_seconds = {"seconds": 1, "minutes": 60, "hours": 3600, "days": 86400}
            initial_last_run = timezone.now() - timedelta(seconds=every * period_seconds.get(period, 60))
        else:
            initial_last_run = timezone.now()

        periodic_task = PeriodicTask.objects.create(
            name=periodic_task_name,
            task=Task.SCHEDULER_JOB,
            enabled=enabled,
            last_run_at=initial_last_run,
            **periodic_kwargs,
        )

        # Create UserTaskDetails
        user_task = UserTaskDetails.objects.create(
            task_id=periodic_task_name,
            task_name=task_name,
            description=description,
            project=project,
            environment=environment,
            periodic_task=periodic_task,
            created_by=request.user,
            organization=get_organization(),
            # Per-model deployment configuration
            model_configs=data.get("model_configs", {}),
            # Execution controls
            run_timeout_seconds=int(data.get("run_timeout_seconds", 0)),
            max_retries=int(data.get("max_retries", 0)),
            # Notifications
            notify_on_failure=data.get("notify_on_failure", False),
            notify_on_success=data.get("notify_on_success", False),
            notification_emails=data.get("notification_emails", []),
        )

        # Update periodic task kwargs with user_task_id
        periodic_task.kwargs = _build_task_kwargs(
            user_task, request.user, get_organization()
        )
        periodic_task.save()

        return Response(
            {"status": "Task created successfully"},
            status=status.HTTP_201_CREATED,
        )

    except ProjectDetails.DoesNotExist:
        return Response(
            {"error": "Project not found"}, status=status.HTTP_404_NOT_FOUND
        )
    except EnvironmentModels.DoesNotExist:
        return Response(
            {"error": "Environment not found"}, status=status.HTTP_404_NOT_FOUND
        )
    except Exception as e:
        logger.error(f"Error creating periodic task: {e}")
        return Response(
            {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def list_periodic_tasks(request, project_id):
    """List all periodic tasks for the organization."""
    try:
        page = int(request.GET.get("page", 1))
        limit = int(request.GET.get("limit", 10))

        tasks = UserTaskDetails.objects.all()
        if _is_valid_project_id(project_id):
            tasks = tasks.filter(project__project_uuid=project_id)
        tasks = tasks.select_related(
            "environment", "project", "periodic_task"
        ).order_by("-created_at")
        total = tasks.count()

        offset = (page - 1) * limit
        page_items = [_serialize_task(t) for t in tasks[offset : offset + limit]]

        return Response(
            {
                "data": {
                    "page_items": page_items,
                    "total_items": total,
                    "current_page": page,
                }
            },
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        logger.error(f"Error listing periodic tasks: {e}")
        return Response(
            {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def get_periodic_task(request, project_id, user_task_id):
    """Get a single periodic task by ID."""
    try:
        query = {"id": user_task_id}
        if _is_valid_project_id(project_id):
            query["project__project_uuid"] = project_id
        task = UserTaskDetails.objects.select_related(
            "environment", "project", "periodic_task"
        ).get(**query)
        return Response(
            {"data": [_serialize_task(task)]},
            status=status.HTTP_200_OK,
        )
    except UserTaskDetails.DoesNotExist:
        return Response(
            {"error": "Task not found"}, status=status.HTTP_404_NOT_FOUND
        )
    except Exception as e:
        logger.error(f"Error getting periodic task: {e}")
        return Response(
            {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def update_periodic_task(request, project_id, user_task_id):
    """Update an existing periodic task."""
    try:
        data = request.data
        user_task = UserTaskDetails.objects.get(
            id=user_task_id, project__project_uuid=project_id
        )
        periodic_task = user_task.periodic_task

        task_type = data.get("task_type", TaskType.CRON)

        if "enabled" in data:
            periodic_task.enabled = data["enabled"]

        if task_type == TaskType.CRON:
            cron_expr = data.get("cron_expression")
            if cron_expr:
                parts = cron_expr.strip().split()
                if len(parts) >= 5:
                    schedule = CrontabSchedule.objects.filter(
                        minute=parts[0],
                        hour=parts[1],
                        day_of_month=parts[2],
                        month_of_year=parts[3],
                        day_of_week=parts[4],
                    ).first() or CrontabSchedule.objects.create(
                        minute=parts[0],
                        hour=parts[1],
                        day_of_month=parts[2],
                        month_of_year=parts[3],
                        day_of_week=parts[4],
                    )
                    periodic_task.crontab = schedule
                    periodic_task.interval = None
        elif task_type == TaskType.INTERVAL:
            every = data.get("every")
            period = data.get("period", "minutes")
            if every:
                schedule = IntervalSchedule.objects.filter(
                    every=int(every), period=period.lower()
                ).first() or IntervalSchedule.objects.create(
                    every=int(every), period=period.lower()
                )
                periodic_task.interval = schedule
                periodic_task.crontab = None
                if not periodic_task.last_run_at:
                    periodic_task.last_run_at = timezone.now()

        periodic_task.save()

        # Update UserTaskDetails fields
        if "task_name" in data:
            user_task.task_name = data["task_name"]
        if "description" in data:
            user_task.description = data["description"]
        if "environment" in data:
            try:
                env = EnvironmentModels.objects.get(
                    environment_id=data["environment"]
                )
                user_task.environment = env
            except EnvironmentModels.DoesNotExist:
                pass

        # Per-model deployment configuration
        if "model_configs" in data:
            user_task.model_configs = data["model_configs"]

        # Execution controls
        if "run_timeout_seconds" in data:
            user_task.run_timeout_seconds = int(data["run_timeout_seconds"])
        if "max_retries" in data:
            user_task.max_retries = int(data["max_retries"])

        # Notifications
        if "notify_on_failure" in data:
            user_task.notify_on_failure = data["notify_on_failure"]
        if "notify_on_success" in data:
            user_task.notify_on_success = data["notify_on_success"]
        if "notification_emails" in data:
            user_task.notification_emails = data["notification_emails"]

        # Job chaining
        if "trigger_on_complete" in data:
            chain_id = data["trigger_on_complete"]
            if chain_id:
                try:
                    user_task.trigger_on_complete = UserTaskDetails.objects.get(
                        id=chain_id
                    )
                except UserTaskDetails.DoesNotExist:
                    pass
            else:
                user_task.trigger_on_complete = None

        user_task.save()

        # Update periodic task kwargs with current organization
        periodic_task.kwargs = _build_task_kwargs(
            user_task, request.user, get_organization()
        )
        periodic_task.save(update_fields=["kwargs"])

        return Response(
            {"status": "Task updated successfully"},
            status=status.HTTP_200_OK,
        )

    except UserTaskDetails.DoesNotExist:
        return Response(
            {"error": "Task not found"}, status=status.HTTP_404_NOT_FOUND
        )
    except Exception as e:
        logger.error(f"Error updating periodic task: {e}")
        return Response(
            {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(["DELETE"])
@permission_classes([IsAuthenticated])
def delete_periodic_task(request, project_id, task_id):
    """Delete a periodic task."""
    try:
        user_task = UserTaskDetails.objects.select_related(
            "periodic_task"
        ).get(periodic_task_id=task_id, project__project_uuid=project_id)
        periodic_task = user_task.periodic_task
        user_task.delete()
        if periodic_task:
            periodic_task.delete()

        return Response(
            {"status": "Task deleted successfully"},
            status=status.HTTP_200_OK,
        )
    except UserTaskDetails.DoesNotExist:
        return Response(
            {"error": "Task not found"}, status=status.HTTP_404_NOT_FOUND
        )
    except Exception as e:
        logger.error(f"Error deleting periodic task: {e}")
        return Response(
            {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def task_run_history(request, project_id, user_task_id):
    """Get run history for a task."""
    try:
        page = int(request.GET.get("page", 1))
        limit = int(request.GET.get("limit", 10))

        query = {"id": user_task_id}
        if _is_valid_project_id(project_id):
            query["project__project_uuid"] = project_id
        task = UserTaskDetails.objects.get(**query)
        runs = TaskRunHistory.objects.filter(user_task_detail=task)

        trigger_filter = request.GET.get("trigger")
        scope_filter = request.GET.get("scope")
        status_filter = request.GET.get("status")
        if trigger_filter:
            runs = runs.filter(trigger=trigger_filter)
        if scope_filter:
            runs = runs.filter(scope=scope_filter)
        if status_filter:
            runs = runs.filter(status=status_filter)

        runs = runs.order_by("-start_time")
        total = runs.count()

        offset = (page - 1) * limit
        serializer = TaskRunHistorySerializer(runs[offset : offset + limit], many=True)

        return Response(
            {
                "success": True,
                "data": {
                    "page_items": {
                        "id": task.id,
                        "job_name": task.task_name,
                        "env_type": task.environment.deployment_type
                        if task.environment
                        else None,
                        "next_run_time": task.next_run_time,
                        "run_history": serializer.data,
                    },
                    "total_items": total,
                    "current_page": page,
                    "limit": limit,
                },
            },
            status=status.HTTP_200_OK,
        )
    except UserTaskDetails.DoesNotExist:
        return Response(
            {"error": "Task not found"}, status=status.HTTP_404_NOT_FOUND
        )
    except Exception as e:
        logger.error(f"Error getting run history: {e}")
        return Response(
            {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


def _dispatch_task_run(task, user_id, models_override=None):
    """Shared dispatch: try Celery broker, fall back to synchronous execution.

    Always marks the run as ``trigger="manual"`` — only the Celery beat
    scheduler path hits ``trigger_scheduled_run`` without this dispatch
    wrapper, and it keeps the default ``trigger="scheduled"``.
    """
    run_kwargs = {
        "user_task_id": task.id,
        "user_id": user_id,
        "organization_id": str(task.organization_id) if task.organization_id else None,
        "trigger": "manual",
    }
    if models_override:
        run_kwargs["models_override"] = list(models_override)

    try:
        from backend.core.scheduler.task_constant import Task as TaskConst
        from celery import current_app

        current_app.send_task(TaskConst.SCHEDULER_JOB, kwargs=run_kwargs)

        task.status = TaskStatus.RUNNING
        task.task_run_time = timezone.now()
        task.save(update_fields=["status", "task_run_time"])

        return Response(
            {"success": True, "data": "Job submitted to Celery broker."},
            status=status.HTTP_200_OK,
        )
    except Exception as broker_err:
        logger.warning("Celery broker unavailable (%s), running synchronously.", broker_err)

    try:
        from backend.core.scheduler.celery_tasks import trigger_scheduled_run

        trigger_scheduled_run(**run_kwargs)

        return Response(
            {"success": True, "data": "Job executed synchronously (no broker)."},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        logger.error("Sync execution failed: %s", e)
        return Response(
            {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def trigger_task_once(request, project_id, user_task_id):
    """Trigger a task to run immediately.

    Tries Celery first; if the broker is unreachable, falls back to
    synchronous (in-process) execution so local dev works without Redis.
    """
    try:
        task = UserTaskDetails.objects.get(
            id=user_task_id, project__project_uuid=project_id
        )
    except UserTaskDetails.DoesNotExist:
        return Response(
            {"error": "Task not found"}, status=status.HTTP_404_NOT_FOUND
        )

    return _dispatch_task_run(task, request.user.id)


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def trigger_task_once_for_model(request, project_id, user_task_id, model_name):
    """Quick Deploy: trigger a job to run a single model against its configured environment.

    Execution reuses the scheduler pipeline (TaskRunHistory, retries, Slack
    notifications) but scopes the DAG run to ``model_name`` only. The model
    must be present and enabled in the task's ``model_configs``.
    """
    try:
        task = UserTaskDetails.objects.select_related("project").get(
            id=user_task_id, project__project_uuid=project_id
        )
    except UserTaskDetails.DoesNotExist:
        return Response(
            {"error": "Task not found"}, status=status.HTTP_404_NOT_FOUND
        )

    model_cfg = (task.model_configs or {}).get(model_name)
    if not model_cfg or not model_cfg.get("enabled", True):
        return Response(
            {"error": f"Model '{model_name}' is not enabled on this job."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    return _dispatch_task_run(task, request.user.id, models_override=[model_name])


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def list_recent_runs_for_model(request, project_id, model_name):
    """Return recent TaskRunHistory entries for any job in this project that
    includes ``model_name`` in its ``model_configs``. Mixes scheduled and
    quick-deploy runs; caller distinguishes via each row's
    ``kwargs.source``.
    """
    try:
        limit = int(request.GET.get("limit", 5))
    except (TypeError, ValueError):
        limit = 5
    limit = max(1, min(limit, 50))

    runs_qs = TaskRunHistory.objects.select_related(
        "user_task_detail", "user_task_detail__environment",
    ).filter(
        user_task_detail__project__project_uuid=project_id,
        user_task_detail__model_configs__has_key=model_name,
    ).order_by("-start_time")[:limit]

    data = []
    for run in runs_qs:
        task = run.user_task_detail
        env = task.environment
        kwargs = run.kwargs or {}
        models_override = kwargs.get("models_override") or []
        # Back-compat: rows written before the trigger/scope split only
        # carried kwargs.source=="quick_deploy" as their manual-model marker.
        legacy_source = kwargs.get("source")
        trigger = kwargs.get("trigger") or (
            "manual" if legacy_source == "quick_deploy" else "scheduled"
        )
        scope = kwargs.get("scope") or (
            "model" if models_override or legacy_source == "quick_deploy" else "job"
        )
        data.append({
            "run_id": run.id,
            "user_task_id": task.id,
            "task_name": task.task_name,
            "status": run.status,
            "start_time": run.start_time.isoformat() if run.start_time else None,
            "end_time": run.end_time.isoformat() if run.end_time else None,
            "error_message": run.error_message,
            "environment_name": getattr(env, "environment_name", "")
            or getattr(env, "name", ""),
            "trigger": trigger,
            "scope": scope,
            "models_override": models_override,
        })

    return Response({"data": data}, status=status.HTTP_200_OK)


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def list_deploy_candidates(request, project_id, model_name):
    """Return jobs in ``project_id`` that can deploy ``model_name``.

    A job qualifies when ``model_name`` is a key in ``model_configs`` and its
    ``enabled`` flag is truthy (defaults to True if the flag is absent).
    """
    tasks = UserTaskDetails.objects.select_related("environment", "project").filter(
        project__project_uuid=project_id,
        model_configs__has_key=model_name,
    )

    candidates = []
    for task in tasks:
        model_configs = task.model_configs or {}
        cfg = model_configs.get(model_name)
        if not cfg or not cfg.get("enabled", True):
            continue
        enabled_model_count = sum(
            1
            for m_cfg in model_configs.values()
            if isinstance(m_cfg, dict) and m_cfg.get("enabled", True)
        )
        env = task.environment
        candidates.append({
            "user_task_id": task.id,
            "task_name": task.task_name,
            "environment_id": str(env.environment_id) if env else "",
            "environment_name": (
                getattr(env, "environment_name", "")
                or getattr(env, "name", "")
            ) if env else "",
            "status": task.status,
            "prev_run_status": task.prev_run_status,
            "task_run_time": task.task_run_time.isoformat() if task.task_run_time else None,
            "next_run_time": task.next_run_time.isoformat() if task.next_run_time else None,
            "enabled_model_count": enabled_model_count,
        })

    return Response({"data": candidates}, status=status.HTTP_200_OK)
