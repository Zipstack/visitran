import logging
from typing import Any

from django.conf import settings
from kombu import Connection

from backend.log_constant import LogEventArgument, LogProcessingTask


class LogPublisher:

    kombu_conn = Connection(settings.CELERY_BROKER_URL)

    @classmethod
    def _get_task_message(cls, user_session_id: str, event: str, message: Any) -> dict[str, Any]:

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
    def publish(cls, channel_id: str, payload: dict[str, Any]) -> bool:
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
