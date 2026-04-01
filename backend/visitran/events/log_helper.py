import json
import logging
from queue import Queue
from typing import Optional

from typing import Any

from django.conf import settings
from kombu import Connection

from visitran.constants import LogEventArgument, LogProcessingTask

LOG_QUEUE: Optional[Queue[str]] = None
log_queue = None

logger = logging.getLogger("log_helper")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s - %(message)s")
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)


def get_queue() -> Queue[str]:
    global LOG_QUEUE
    if not LOG_QUEUE:
        LOG_QUEUE = Queue()
    return LOG_QUEUE


if not log_queue:
    logging.info("log_queue is None")
    log_queue = get_queue()
logger.debug(f"M_main_Q: {id(log_queue)}")


class LogHelper:
    kombu_conn = Connection(settings.CELERY_BROKER_URL)

    @staticmethod
    def log(
        message: str,
        level: str = "INFO",
    ) -> dict[str, str]:
        return {
            "level": level,
            "message": message,
        }

    @staticmethod
    def info(
            message: str,
    ) -> dict[str, str]:
        return {
            "level": "INFO",
            "message": message,
        }

    @staticmethod
    def warn(
            message: str,
    ) -> dict[str, str]:
        return {
            "level": "WARNING",
            "message": message,
        }

    @staticmethod
    def error(
            message: str,
    ) -> dict[str, str]:
        return {
            "level": "ERROR",
            "message": message,
        }
    @staticmethod
    def publish(message: dict[str, str]) -> bool:
        try:
            logger.debug(f"log_helper_Q: {id(log_queue)}")
            payload = json.dumps(message)
            log_queue.put(payload)  # type: ignore[union-attr]
        except Exception as err:
            logger.error(f"Could not queue message: {err}")
            return False
        return True

    @classmethod
    def _get_task_message(
            cls, user_session_id: str, event: str, message: Any
    ) -> dict[str, Any]:

        task_kwargs = {
            LogEventArgument.EVENT: event,
            LogEventArgument.MESSAGE: message,
            LogEventArgument.USER_SESSION_ID: user_session_id,
        }
        task_message = {
            "args": [],
            "kwargs": task_kwargs,
            "retries": 0,
            "utc": True,
        }
        return task_message

    @classmethod
    def _get_task_header(cls, task_name: str) -> dict[str, Any]:
        return {
            "task": task_name,
        }

    @classmethod
    def publish_log(cls, channel_id: str, payload: dict[str, Any]) -> bool:
        """Publish a message to the queue."""
        try:
            with cls.kombu_conn.Producer(serializer="json") as producer:
                event = f"logs:{channel_id}"
                task_message = cls._get_task_message(
                    user_session_id=channel_id,
                    event=event,
                    message=payload,
                )
                headers = cls._get_task_header(LogProcessingTask.TASK_NAME)
                # Publish the message to the queue
                producer.publish(
                    body=task_message,
                    exchange="",
                    headers=headers,
                    routing_key=LogProcessingTask.QUEUE_NAME,
                    compression=None,
                    retry=True,
                )
                logging.debug(f"Published '{channel_id}' <= {payload}")
        except Exception as e:
            logging.error(f"Failed to publish '{channel_id}' <= {payload}: {e}")
            return False
        return True
