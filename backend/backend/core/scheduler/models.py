from django.conf import settings
from django.db import models

from backend.core.models.environment_models import EnvironmentModels
from backend.core.models.project_details import ProjectDetails
from backend.core.scheduler.task_constant import TaskStatus
from utils.models.base_model import BaseModel
from utils.models.organization_mixin import (
    DefaultOrganizationManagerMixin,
    DefaultOrganizationMixin,
)


class UserTaskDetailsManager(DefaultOrganizationManagerMixin, models.Manager):
    pass


class UserTaskDetails(DefaultOrganizationMixin, BaseModel):
    task_id = models.CharField(max_length=255)
    task_name = models.CharField(max_length=255)
    status = models.CharField(
        max_length=50,
        choices=TaskStatus.choices,
        default=TaskStatus.PENDING,
    )
    updated_at = models.DateTimeField(auto_now=True)
    task_run_time = models.DateTimeField(blank=True, null=True)
    task_completion_time = models.DateTimeField(blank=True, null=True)
    next_run_time = models.DateTimeField(blank=True, null=True)
    prev_run_status = models.CharField(
        max_length=50,
        choices=TaskStatus.choices,
        default=TaskStatus.PENDING,
    )
    description = models.TextField(blank=True, null=True)

    # Per-model deployment configuration
    # Structure: {
    #   "model_name": {
    #     "enabled": true,
    #     "materialization": "INCREMENTAL",  # TABLE | VIEW | INCREMENTAL
    #     "incremental_config": {
    #       "primary_key": ["customer_id"],
    #       "delta_strategy": {"type": "timestamp", "column": "updated_at"}
    #     },
    #     "watermark": {
    #       "last_value": "2024-01-15T10:30:00Z",
    #       "last_run_at": "2024-01-15T09:00:00Z",
    #       "records_processed": 1523
    #     }
    #   }
    # }
    model_configs = models.JSONField(
        default=dict,
        blank=True,
        help_text="Per-model deployment configuration including materialization and incremental settings",
    )

    # Execution controls
    run_timeout_seconds = models.PositiveIntegerField(
        default=0, help_text="Max run duration in seconds. 0 = no limit.",
    )
    max_retries = models.PositiveSmallIntegerField(default=0)

    # Notifications
    notify_on_failure = models.BooleanField(default=False)
    notify_on_success = models.BooleanField(default=False)
    notification_emails = models.JSONField(default=list, blank=True)

    # Job chaining
    trigger_on_complete = models.ForeignKey(
        "self", on_delete=models.SET_NULL, blank=True, null=True,
        related_name="triggered_by",
        help_text="Job to trigger when this job completes successfully.",
    )

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
    )
    environment = models.ForeignKey(
        EnvironmentModels,
        on_delete=models.PROTECT,
    )
    periodic_task = models.ForeignKey(
        "django_celery_beat.PeriodicTask",
        on_delete=models.CASCADE,
        blank=True,
        null=True,
    )
    project = models.ForeignKey(
        ProjectDetails,
        on_delete=models.CASCADE,
        related_name="user_tasks",
    )

    objects = UserTaskDetailsManager()

    class Meta:
        app_label = "job_scheduler"
        db_table = "job_scheduler_usertaskdetails"

    def __str__(self):
        return f"{self.task_name} ({self.status})"


class TaskRunHistoryManager(DefaultOrganizationManagerMixin, models.Manager):
    pass


class TaskRunHistory(DefaultOrganizationMixin, BaseModel):
    task_id = models.CharField(
        max_length=255,
        help_text="Celery task ID for this specific run.",
    )
    retry_num = models.PositiveIntegerField(default=0)
    status = models.CharField(
        max_length=50,
        choices=[
            ("PENDING", "Pending"),
            ("STARTED", "Started"),
            ("SUCCESS", "Success"),
            ("FAILURE", "Failure"),
            ("REVOKED", "Revoked"),
            ("RETRY", "Retry"),
        ],
        default="PENDING",
        help_text="Current status of the task run.",
    )
    start_time = models.DateTimeField(blank=True, null=True)
    end_time = models.DateTimeField(blank=True, null=True)
    kwargs = models.JSONField(blank=True, null=True)
    result = models.JSONField(blank=True, null=True)
    error_message = models.TextField(blank=True, null=True)
    trigger = models.CharField(
        max_length=20,
        choices=[("scheduled", "Scheduled"), ("manual", "Manual")],
        default="scheduled",
        help_text="How the run was initiated: cron/interval schedule or manual dispatch.",
    )
    scope = models.CharField(
        max_length=20,
        choices=[("job", "Full job"), ("model", "Single model")],
        default="job",
        help_text="Whether the run executed all job models or a single model.",
    )

    user_task_detail = models.ForeignKey(
        UserTaskDetails,
        on_delete=models.CASCADE,
        related_name="task_runs",
    )

    objects = TaskRunHistoryManager()

    class Meta:
        app_label = "job_scheduler"
        db_table = "job_scheduler_taskrunhistory"
        verbose_name = "Task Run History"
        verbose_name_plural = "Task Run Histories"
        ordering = ["-start_time"]
        unique_together = [("task_id", "retry_num")]
        indexes = [
            models.Index(fields=["task_id"], name="job_schedul_task_id_4dc8ac_idx"),
            models.Index(fields=["status"], name="job_schedul_status_86a75c_idx"),
            models.Index(
                fields=["user_task_detail"], name="job_schedul_user_ta_5cd43a_idx"
            ),
            models.Index(fields=["trigger"], name="job_schedul_trigger_idx"),
            models.Index(fields=["scope"], name="job_schedul_scope_idx"),
        ]

    def __str__(self):
        return f"Run {self.task_id} ({self.status})"
