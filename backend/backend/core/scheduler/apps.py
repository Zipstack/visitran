from django.apps import AppConfig


class SchedulerConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "backend.core.scheduler"
    label = "job_scheduler"  # Keep same label for database compatibility
