import datetime
import json
import logging

import google.cloud.logging
import pytz
from django.conf import settings

from backend.core.middlewares.log_aggregator import (
    get_log_level,
    get_log_severity,
    get_request_logs,
    is_log_aggregation_enabled,
    set_log_level,
    write_request_logs,
)


def _get_google_cloud_client():
    """Get a Google Cloud client."""
    service_account_info = json.loads(settings.LOGGING_SERVICE_ACCOUNT_INFO)
    return google.cloud.logging.Client.from_service_account_info(service_account_info)


class RequestLogHandler(logging.Handler):
    def __init__(self, log_name: str = "Visitran-backend"):
        super().__init__()
        self.log_name = log_name
        self.client = _get_google_cloud_client()
        self.logger = self.client.logger(log_name)

    def format_log(self, record):
        now_utc = datetime.datetime.now(pytz.utc)
        request_start_time = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if record.levelno > get_log_level():
            set_log_level(level_no=record.levelno, level_name=record.levelname)
        return {
            "severity": record.levelname,
            "time": request_start_time,
            "logMessage": self.format(record),
            "sourceLocation": {"file": record.pathname, "line": record.lineno, "functionName": record.funcName},
        }

    def emit(self, record):
        try:
            if is_log_aggregation_enabled():
                if record.msg == "send logs to google cloud":
                    request_logs = get_request_logs()
                    request_logs["logName"] = self.logger.full_name
                    request_logs["resource"] = {
                        "type": "global",
                        "labels": {"project_id": self.logger.project},
                    }
                    request_logs["severity"] = get_log_severity()
                    try:
                        entries = [request_logs]
                        self.client.logging_api.write_entries(entries, partial_success=True)
                    except Exception as e:
                        print(f"Error sending logs to GCP: {e}")
                        print(request_logs)
                        print("_______________ end of logs ________________")
                else:
                    write_request_logs(self.format_log(record))
            else:
                message = self.format(record)
                self.logger.log_text(message)
        except Exception as e:
            print(f"Error logging to Google Cloud: {e}")
            print(f"{record.levelname} {record.msg}")
            self.handleError(record)


class LogsCustomFormatter(logging.Formatter):
    """Formatter that adds color to log levels."""

    grey = "\x1b[38;20m"
    green = "\x1b[32;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format_str = "%(asctime)s - %(levelname)s - %(module)s - %(message)s"

    FORMATS = {
        logging.DEBUG: green + format_str + reset,
        logging.INFO: grey + format_str + reset,
        logging.WARNING: yellow + format_str + reset,
        logging.ERROR: red + format_str + reset,
        logging.CRITICAL: bold_red + format_str + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)
