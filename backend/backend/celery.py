import os
from importlib.util import find_spec

from celery import Celery

from backend.log_consumer_celery_tasks import TaskRegistry

os.environ.setdefault(
    "DJANGO_SETTINGS_MODULE",
    os.environ.get("DJANGO_SETTINGS_MODULE", "backend.server.settings.dev"),
)
# Initialize Celery application
app = Celery("backend")

TaskRegistry()
# Load settings from Django's settings module using the 'CELERY' namespace.
app.config_from_object("django.conf:settings", namespace="CELERY")

# Built-in periodic tasks (runs alongside django-celery-beat's DB schedules)
app.conf.beat_schedule = {
    "recover-stuck-jobs": {
        "task": "backend.core.scheduler.celery_tasks.recover_stuck_jobs",
        "schedule": 600.0,  # every 10 minutes
    },
}


optional_modules = [
    "backend.core.scheduler",
    "backend.core.scheduler.celery_tasks",
]


def safe_autodiscover(modules):
    existing_modules = []
    for module in modules:
        try:
            if find_spec(module):  # Check if the module exists
                existing_modules.append(module)
        except ModuleNotFoundError:
            # Log the missing module or silently skip
            print(f"Optional module {module} not found. Skipping.")
    return existing_modules


# Safely discover tasks for optional modules
existing_modules = safe_autodiscover(optional_modules)
if existing_modules:
    app.autodiscover_tasks(existing_modules, related_name="celery_tasks")

# Standard autodiscover for installed apps.
app.autodiscover_tasks()


@app.task(bind=True)
def debug_task(self):
    print(f"Request: {self.request!r}")
