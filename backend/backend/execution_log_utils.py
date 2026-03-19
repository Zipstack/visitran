import logging
import sys

from django.db import IntegrityError
from django.db.utils import ProgrammingError
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from backend.utils.constants import ExecutionLogConstants

logger = logging.getLogger(__name__)


def create_log_consumer_scheduler_if_not_exists() -> None:
    try:
        interval, _ = IntervalSchedule.objects.get_or_create(
            every=ExecutionLogConstants.CONSUMER_INTERVAL,
            period=IntervalSchedule.SECONDS,
        )
    except ProgrammingError as error:
        logger.warning(
            "ProgrammingError occurred while creating "
            "log consumer scheduler. If you are currently running "
            "migrations for new environment, you can ignore this warning"
        )
        if "migrate" not in sys.argv:
            logger.warning(f"ProgrammingError details: {error}")
        return
    except IntervalSchedule.MultipleObjectsReturned as error:
        logger.error(f"Error occurred while getting interval schedule: {error}")
        interval = IntervalSchedule.objects.filter(
            every=ExecutionLogConstants.CONSUMER_INTERVAL,
            period=IntervalSchedule.SECONDS,
        ).first()
    try:
        # Create the scheduler
        task, created = PeriodicTask.objects.get_or_create(
            name=ExecutionLogConstants.PERIODIC_TASK_NAME,
            task=ExecutionLogConstants.TASK,
            defaults={
                "interval": interval,
                "queue": ExecutionLogConstants.CELERY_QUEUE_NAME,
                "enabled": ExecutionLogConstants.IS_ENABLED,
            },
        )
        if not created:
            task.enabled = ExecutionLogConstants.IS_ENABLED
            task.interval = interval
            task.queue = ExecutionLogConstants.CELERY_QUEUE_NAME
            task.save()
            logger.info("Log consumer scheduler updated successfully.")
        else:
            logger.info("Log consumer scheduler created successfully.")
    except IntegrityError as error:
        logger.error(f"Error occurred while creating log consumer scheduler: {error}")
