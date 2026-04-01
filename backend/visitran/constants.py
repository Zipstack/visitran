from enum import Enum

from django.conf import settings


class BaseConstant(str, Enum):
    """Base method for constants and string representations."""

    def __str__(self) -> str:
        return self.value  # type: ignore[no-any-return]

    def __repr__(self) -> str:
        return self.value  # type: ignore[no-any-return]


class VisitranCLIConstant(BaseConstant):
    PROFILE_LOCATED = "Path where profile is located."
    PROJECT_WILL_BE_CREATED = "Path where project will be [green]created[/green]."


class VisitranProfileConstant(BaseConstant):
    MODEL = "model"
    SEED = "seed"
    TEST = "test"
    SNAPSHOT = "snapshot"

    MODEL_PATHS = "model-paths"
    SEED_PATHS = "seed-paths"
    TEST_PATHS = "test-paths"
    SNAPSHOT_PATHS = "snapshot-paths"


class SnapshotConstants(BaseConstant):
    # Configurations
    TIMESTAMP = "timestamp"
    UPDATED_AT = "updated_at"
    CHECK = "check"
    CHECK_COLS = "check_cols"
    STRATEGY = "strategy"

    # This is for validating cols in check stratergy
    RESTRICTED_COLS = ["all", "*"]


class EventsConstants(BaseConstant):
    IMPORT_FAILED = "Import of Dependency Module "

    # status
    FAILED = "Failed"


class ColumnEventStatus(BaseConstant):
    JOINS = "joins"
    GROUPS = "groups"
    SYNTHESIS = "synthesis"
    SYNTHESIS_VALIDATE = "synthesis_validator"
    UNION = "union"

    @classmethod
    def validate(cls, event_type: str) -> bool:
        if event_type in (cls.JOINS, cls.GROUPS, cls.SYNTHESIS, cls.UNION, cls.SYNTHESIS_VALIDATE):
            return True
        return False


class CloudConstants(BaseConstant):
    BUCKET_NAME = settings.GS_BUCKET_NAME


class LogEventArgument:
    EVENT = "event"
    MESSAGE = "message"
    USER_SESSION_ID = "user_session_id"
    LOG_EVENTS_ID = "log_events_id"


class LogProcessingTask:
    TASK_NAME = "logs_consumer"
    QUEUE_NAME = "celery_log_task_queue"
