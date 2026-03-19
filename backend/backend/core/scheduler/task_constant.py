from django.db import models


class TaskStatus(models.TextChoices):
    PENDING = "PENDING", "Pending"
    RUNNING = "RUNNING", "Running"
    COMPLETED = "COMPLETED", "Completed"
    FAILED = "FAILED", "Failed"
    SUCCESS = "SUCCESS", "Success"
    FAILED_PERMANENTLY = "FAILED PERMANENTLY", "Failed Permanently"
    RETRYING = "Retrying", "Retrying"


class TaskType:
    CRON = "cron"
    INTERVAL = "interval"


class Task:
    SCHEDULER_JOB = "backend.core.scheduler.celery_tasks.trigger_scheduled_run"
