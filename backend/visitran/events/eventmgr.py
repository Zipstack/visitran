from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto
from logging.handlers import RotatingFileHandler
from typing import TYPE_CHECKING, Any, Callable, Optional, TextIO
from uuid import uuid4

from colorama import Fore, Style

from visitran.events.log_helper import LogHelper

from visitran.events.local_context import StateStore

if TYPE_CHECKING:  # pragma: no cover
    from logging import Logger

import visitran.flags as flags
from visitran.events.base_types import BaseEvent, EventLevel, EventMsg, msg_from_base_event

LOG_LINE = ""
Filter = Callable[[EventMsg], bool]


def no_filter(_: EventMsg) -> bool:
    return True


# TO DO : Scrubber - Visitran OR 14
# A Scrubber removes secrets from an input string, returning a sanitized string.
# Scrubber = Callable[[str], str]


def no_scrubber(s: str) -> str:
    return s


class LineFormat(Enum):
    """Class to Hold Code for Log Format."""

    PlainText = auto()
    DebugText = auto()
    Json = auto()


# Map from visitran event levels to python log levels
_log_level_map = {
    EventLevel.DEBUG: 10,
    EventLevel.TEST: 10,
    EventLevel.INFO: 20,
    EventLevel.WARN: 30,
    EventLevel.ERROR: 40,
}

# poll the log file for logs


def send_to_logger(logger_line: Logger, level: str, log_line: str) -> None:
    """Method to write the logs to files."""
    if level == "test":
        logger_line.debug(log_line)
    elif level == "debug":
        logger_line.debug(log_line)
    elif level == "info":
        logger_line.info(log_line)
    elif level == "warn":
        logger_line.warning(log_line)
    elif level == "error":
        logger_line.error(log_line)
    else:
        raise AssertionError(f"While attempting to log {log_line}, encountered the unhandled level: {level}")


@dataclass
class LoggerConfig:
    """Base class for Logger Config."""

    name: str
    filter: Filter = no_filter
    line_format: LineFormat = LineFormat.PlainText
    level: EventLevel = EventLevel.WARN
    use_colors: bool = flags.USE_COLORS
    output_stream: Optional[TextIO] = None
    output_file_name: Optional[str] = None
    logger: Optional[Any] = None


class _Logger:
    """Parent Logger Config class, where all the logger configs are set."""

    def __init__(self, event_manager: EventManager, config: LoggerConfig) -> None:
        self.name: str = config.name
        self.filter: Filter = config.filter
        self.level: EventLevel = config.level
        self.event_manager: EventManager = event_manager
        self._python_logger: Optional[logging.Logger] = logging.getLogger(config.name)
        self._stream: Optional[TextIO] = config.output_stream

        if config.output_file_name:
            log = logging.getLogger(config.name)
            log.setLevel(_log_level_map[config.level])
            handler = RotatingFileHandler(
                filename=str(config.output_file_name),
                encoding="utf8",
                maxBytes=10 * 1024 * 1024,  # 10 mb
                backupCount=5,
            )
            logging.Formatter("%(asctime)s [%(levelname)s] [%(thread)d] %(message)s")
            log.handlers.clear()
            log.addHandler(handler)
            self._python_logger = log
        if config.output_stream:
            handler_stream = logging.StreamHandler(config.output_stream)
            log = logging.getLogger(config.name)
            log.handlers.clear()
            log.addHandler(handler_stream)
            self._python_logger = log

    def create_line(self, msg: EventMsg) -> str:
        raise NotImplementedError()

    def write_line(self, msg: EventMsg) -> None:
        line = self.create_line(msg)
        if self._python_logger is not None and self.name == "file_log":
            audience = getattr(msg.data, "audience", lambda: "developer")()
            if audience == "user":
                ts = datetime.utcnow().strftime("%H:%M:%S")
                title = getattr(msg.data, "title", lambda: msg.info.msg)()
                subtitle = getattr(msg.data, "subtitle", lambda: "")()
                event_status = getattr(msg.data, "status", lambda: "info")()
                event_code = getattr(msg.data, "code", lambda: "")()
                payload = {
                    "level": msg.info.level,
                    "audience": audience,
                    "title": title,
                    "subtitle": subtitle,
                    "status": event_status,
                    "code": event_code,
                    "timestamp": ts,
                    "message": msg.info.msg,
                }
                LogHelper.publish_log(StateStore.get("log_events_id"), payload)
            else:
                LogHelper.publish_log(StateStore.get("log_events_id"), LogHelper.log(line, msg.info.level, audience))

        if self._stream is not None and self.name == "stdout_log":
            self._stream.write(line + "\n")


class TextLogger(_Logger):
    """Child class of Logger specific to Text Logger."""

    def __init__(self, event_manager: EventManager, config: LoggerConfig) -> None:
        super().__init__(event_manager, config)
        self.use_colors = config.use_colors
        self.use_debug_format = config.line_format == LineFormat.DebugText

    def create_line(self, msg: EventMsg) -> str:
        return self.create_debug_line(msg) if self.use_debug_format else self.create_info_line(msg)

    def create_info_line(self, msg: EventMsg) -> str:
        ts: str = datetime.utcnow().strftime("%H:%M:%S")
        # TO DO OR 14 scrubbed_msg: str = self.scrubber(msg.info.msg)  # type: ignore
        color_ts = self._get_color_tag(msg=msg, invoker="ts")
        color_msg = self._get_color_tag(msg=msg, invoker="msg")
        return f"{color_ts}{ts}  {color_msg}{msg.info.msg}"

    def create_debug_line(self, msg: EventMsg) -> str:
        log_line: str = ""
        # Create a separator if this is the beginning of an invocation
        if msg.info.name == "MainReportVersion":
            separator = 30 * "="
            log_line = f"\n\n{separator} {msg.info.ts} | {self.event_manager.invocation_id} {separator}\n"
        ts: str = datetime.utcnow().strftime("%H:%M:%S")
        level = msg.info.level
        color_ts = self._get_color_tag(msg=msg, invoker="ts")
        color_msg = self._get_color_tag(msg=msg, invoker="msg")
        color_level = self._get_color_tag(msg=msg, invoker="level")
        log_line += f"{color_ts}{ts} {color_level}[{level:<5}]{color_msg} {msg.info.msg}"
        return log_line

    def _get_color_tag(self, msg: EventMsg, invoker: str) -> Any:
        """Function to Set colors when color tag is enabled."""
        if self.use_colors and invoker == "msg":
            return Style.NORMAL
        if self.use_colors and invoker == "ts":
            return Fore.GREEN
        if self.use_colors and invoker == "level":
            if msg.info.level == "info":
                return Fore.CYAN
            if msg.info.level == "debug":
                return Fore.GREEN
            if msg.info.level == "error":
                return Fore.RED
        if not self.use_colors:
            return Style.NORMAL

    def _get_thread_name(self) -> str:
        """Function to fetch thread name for logs."""
        thread_name = ""
        if threading.current_thread().name:
            thread_name = threading.current_thread().name
            thread_name = thread_name[:10]
            thread_name = thread_name.ljust(10, " ")
            thread_name = f" [{thread_name}]:"
        return thread_name


def msg_to_dict(msg: EventMsg) -> dict[str, dict[str, str]]:
    """Function to serialize text logs to JSON."""
    msg_dict = dict()
    try:
        msg_info = msg.info.to_dict()
        msg_dict["info"] = msg_info
        # To Do : Addind Data to JSON if "to_dict" in dir(msg.data):
        # msg_data = msg.data.to_dict()
        # msg_dict["data"] = msg_data
        return msg_dict

    except AttributeError as exc:
        event_type = type(msg).__name__
        raise AttributeError(f"type {event_type} is not serializable. {repr(exc)}")


class JsonLogger(_Logger):
    """Child class specific to JSON Logging."""

    def create_line(self, msg: EventMsg) -> str:
        msg_dict = msg_to_dict(msg)
        raw_log_line = json.dumps(msg_dict, indent=4)
        # TO DO line = self.scrubber(raw_log_line)

        return raw_log_line


class EventManager:
    """Manager Class handling Logging Events."""

    def __init__(self) -> None:
        self.loggers: list[_Logger] = []
        self.invocation_id: str = str(uuid4())

    def fire_event(self, e: BaseEvent, level: Optional[EventLevel] = None) -> None:
        """Entry Point Function for all Logging Events."""
        msg = msg_from_base_event(e, level=level)

        for logger in self.loggers:
            if logger.filter(msg):
                logger.write_line(msg)

    def add_logger(self, config: LoggerConfig) -> None:
        """Function Invoked when Logger Config is added."""
        logger = JsonLogger(self, config) if config.line_format == LineFormat.Json else TextLogger(self, config)
        logger.event_manager = self
        self.loggers.append(logger)
