from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Protocol

import visitran.events.proto_types as proto_type


class Cache:
    pass


class EventLevel(str, Enum):
    """Enum Class Containing Log Level."""

    DEBUG = "debug"
    TEST = "test"
    INFO = "info"
    WARN = "warn"
    ERROR = "error"


@dataclass
class BaseEvent:
    """BaseEvent for proto message generated python events."""

    def level_tag(self) -> EventLevel:
        return EventLevel.DEBUG

    def audience(self) -> str:
        return "developer"

    def message(self) -> str:
        raise NotImplementedError("message() not implemented for event")

    def code(self) -> str:
        raise NotImplementedError("code() not implemented for event")


def msg_from_base_event(event: BaseEvent, level: Optional[EventLevel] = None) -> EventMsg:
    """Function to Parse and fetch Message data from BaseEvent."""
    msg_class_name = f"{type(event).__name__}Msg"

    msg_cls = getattr(proto_type, msg_class_name)

    msg_level: str = level.value if level else event.level_tag().value
    if msg_level is None:
        raise AttributeError
    event_info = proto_type.EventInfo(
        level=msg_level,
        msg=event.message(),
        name=type(event).__name__,
    )
    new_event: EventMsg = msg_cls(data=event, info=event_info)
    return new_event


@dataclass
class TestLevel(BaseEvent):
    """Class for holding Test Log Level."""

    __test__ = False

    def level_tag(self) -> EventLevel:
        return EventLevel.TEST


@dataclass
class DebugLevel(BaseEvent):
    def level_tag(self) -> EventLevel:
        return EventLevel.DEBUG


@dataclass
class InfoLevel(BaseEvent):
    def level_tag(self) -> EventLevel:
        return EventLevel.INFO


@dataclass
class WarnLevel(BaseEvent):
    def level_tag(self) -> EventLevel:
        return EventLevel.WARN


@dataclass
class ErrorLevel(BaseEvent):
    def level_tag(self) -> EventLevel:
        return EventLevel.ERROR


@dataclass
class UserLevel(BaseEvent):
    """User-facing events shown in the activity log (not developer noise)."""

    def level_tag(self) -> EventLevel:
        return EventLevel.INFO

    def audience(self) -> str:
        return "user"

    def title(self) -> str:
        """Clean, short title for the activity feed."""
        return self.message()

    def subtitle(self) -> str:
        """Contextual metadata shown below the title."""
        return ""

    def status(self) -> str:
        """One of: running, success, error, warning, info."""
        return "success"


class NoFile:
    """Prevents an event from going to the file."""

    pass


class NoStdOut:
    """Prevents an event from going to stdout."""

    pass


class EventMsg(Protocol):
    """Base Class for Holding Event Message."""

    info: proto_type.EventInfo
    data: BaseEvent
