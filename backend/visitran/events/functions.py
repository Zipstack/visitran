import os
import sys
from functools import partial
from typing import Optional

import colorama

import visitran.flags as flags
from visitran.events.base_types import BaseEvent, Cache, EventLevel, EventMsg, NoFile, NoStdOut
from visitran.events.eventmgr import EventManager, LineFormat, LoggerConfig
from visitran.events.types import Formatting

LOG_VERSION = 3
LOG_PATH = ""
colorama.init(wrap=True)


def setup_event_logger(log_path: str, level_override: Optional[EventLevel] = None) -> None:
    """Function is invoked in the begining to setup folders and loggers."""
    cleanup_event_logger()
    LOG_PATH = os.path.join(log_path, "visitran.log")
    os.makedirs(log_path, exist_ok=True)
    event_manager.add_logger(_get_stdout_config(level_override))
    # create and add the file logger to the event manager
    event_manager.add_logger(_get_logfile_config(LOG_PATH))


def _get_stdout_config(level: Optional[EventLevel] = None) -> LoggerConfig:
    """Function to fetch config for console logs."""
    fmt = LineFormat.PlainText
    if flags.LOG_FORMAT == "json":
        fmt = LineFormat.Json
    elif flags.DEBUG:
        fmt = LineFormat.DebugText

    return LoggerConfig(
        name="stdout_log",
        level=level or (EventLevel.DEBUG if flags.DEBUG else EventLevel.INFO),
        use_colors=bool(flags.USE_COLORS),
        line_format=fmt,
        filter=partial(
            _stdout_filter,
            bool(flags.LOG_CACHE_EVENTS),
            bool(flags.DEBUG),
            bool(flags.QUIET),
        ),
        output_stream=sys.stdout,
    )


def _stdout_filter(log_cache_events: bool, debug_mode: bool, quiet_mode: bool, msg: EventMsg) -> bool:
    """Function to Fetch filters for Console Logs."""
    event_level_debug = (EventLevel(msg.info.level) != EventLevel.DEBUG) or debug_mode
    event_level_quiet = (EventLevel(msg.info.level) == EventLevel.ERROR) or not quiet_mode
    log_format_bool = flags.LOG_FORMAT == "json" and isinstance(msg.data, Formatting)
    is_no_std_out = isinstance(msg.data, NoStdOut)
    is_cached = isinstance(msg.data, Cache) or log_cache_events
    return not is_no_std_out and (not is_cached) and event_level_debug and event_level_quiet and log_format_bool


def _get_logfile_config(log_path: str) -> LoggerConfig:
    """Function to fetch config for File Logs."""
    return LoggerConfig(
        name="file_log",
        line_format=LineFormat.Json if flags.LOG_FORMAT == "json" else LineFormat.DebugText,
        use_colors=bool(flags.USE_COLORS),
        level=EventLevel.DEBUG if flags.DEBUG else EventLevel.INFO,
        filter=partial(_logfile_filter, bool(flags.LOG_CACHE_EVENTS)),
        output_file_name=log_path,
    )


def _logfile_filter(log_cache_events: bool, msg: EventMsg) -> bool:
    """Function to fetch filters for file logs."""
    event_level_debug = (EventLevel(msg.info.level) != EventLevel.DEBUG) or flags.DEBUG
    return (
        not isinstance(msg.data, NoFile)
        and not (isinstance(msg.data, Cache) and not log_cache_events)
        and not (flags.LOG_FORMAT == "json" and isinstance(msg.data, Formatting))
        and event_level_debug
    )


def cleanup_event_logger() -> None:
    """Function to cleanup loggers."""
    event_manager.loggers.clear()


event_manager: EventManager = EventManager()


def fire_event(e: BaseEvent, level: Optional[EventLevel] = None) -> None:
    """Top-level method for accessing the eventing system."""
    event_manager.fire_event(e, level=level)
