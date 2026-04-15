# urls.py
from django.urls import path

from backend.core.scheduler.views import (
    create_periodic_task,
    list_periodic_tasks,
    delete_periodic_task,
    update_periodic_task,
    task_run_history,
    trigger_task_once,
    trigger_task_once_for_model,
    list_deploy_candidates,
    list_recent_runs_for_model,
    get_periodic_task,
    get_model_columns,
)

urlpatterns = [
    path("/update/<int:user_task_id>", update_periodic_task, name="update_periodic_task"),
    path("/create-periodic-task", create_periodic_task, name="create_periodic_task"),
    path("/list-periodic-tasks", list_periodic_tasks, name="list_periodic_tasks"),
    path(
        "/delete-periodic-task/<int:task_id>",
        delete_periodic_task,
        name="delete_periodic_task",
    ),
    path(
        "/list-periodic-task/<int:user_task_id>",
        get_periodic_task,
        name="get_periodic_task",
    ),
    path("/run-history/<int:user_task_id>", task_run_history, name="task_run_history"),
    path(
        "/trigger-periodic-task/<int:user_task_id>",
        trigger_task_once,
        name="trigger_task_once",
    ),
    path(
        "/trigger-periodic-task/<int:user_task_id>/model/<str:model_name>",
        trigger_task_once_for_model,
        name="trigger_task_once_for_model",
    ),
    path(
        "/quick-deploy/candidates/<str:model_name>",
        list_deploy_candidates,
        name="list_deploy_candidates",
    ),
    path(
        "/quick-deploy/recent-runs/<str:model_name>",
        list_recent_runs_for_model,
        name="list_recent_runs_for_model",
    ),
    # Model columns endpoint for incremental job configuration
    path(
        "/model/<str:model_name>/columns",
        get_model_columns,
        name="get_model_columns",
    ),
]

# Watermark endpoint for detecting watermark columns
try:
    from backend.core.scheduler.watermark_views import detect_watermark_columns

    urlpatterns += [
        path("/watermark/detect", detect_watermark_columns, name="detect_watermark_columns"),
    ]
except ImportError:
    pass
