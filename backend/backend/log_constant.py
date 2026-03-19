class LogEventArgument:
    EVENT = "event"
    MESSAGE = "message"
    USER_SESSION_ID = "user_session_id"


class LogProcessingTask:
    TASK_NAME = "logs_consumer"
    QUEUE_NAME = "celery_log_task_queue"
