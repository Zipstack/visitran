import logging
import time
import uuid
from threading import local
from typing import Any

from django.core.handlers.wsgi import WSGIRequest

from backend.utils.tenant_context import get_current_tenant

_thread_locals = local()


def is_log_aggregation_enabled():
    """Returns True if log aggregation is enabled for the current request."""
    return getattr(_thread_locals, "aggregate_logs", False)


def get_request_id():
    """Returns the current request ID or None if not set."""
    return getattr(_thread_locals, "request_id", None)


def get_request_logs():
    """Returns the list of logs for the current request or None if not set."""
    return getattr(_thread_locals, "request_logs", {})


def get_log_severity():
    """Returns the list of logs for the current request or None if not set."""
    return getattr(_thread_locals, "log_severity", "INFO")


def get_log_level():
    """Returns the list of logs for the current request or None if not set."""
    return getattr(_thread_locals, "log_severity_no", logging.INFO)


def set_log_level(level_no: int, level_name: str):
    _thread_locals.log_severity = level_name
    _thread_locals.log_severity_no = level_no


def initiate_request_logs(request: WSGIRequest):
    _thread_locals.request_logs = {
        "insertId": str(uuid.uuid4()),
        "protoPayload": {"@type": "type.googleapis.com/google.appengine.logging.v1.RequestLog"},
        "labels": {
            "request_id": get_request_id(),
            "request_type": "http",
            "log_type": "request",
            "url": request.path,
        },
    }


def write_request_logs(log_entity: Any):
    """Writes the given log entry to the current request's log list."""
    if hasattr(_thread_locals, "request_logs") is False:
        print(f"{log_entity.get('severity')} - {log_entity.get('time')} - {log_entity.get('logMessage')}")
    request_logs = _thread_locals.request_logs
    payload = request_logs.get("protoPayload", {})
    if "line" in payload:
        payload["line"].append(log_entity)
    else:
        payload["line"] = [log_entity]
    _thread_locals.request_logs["protoPayload"] = payload


def write_request_and_response(request, response, latency):
    payload = _thread_locals.request_logs
    request_meta = {}
    if request.META:
        request_meta = request.META
    user_agent = request_meta.get("HTTP_USER_AGENT", "Unknown")
    remote_ip = request_meta.get("HTTP_X_FORWARDED_FOR", "Unknown")
    size = 0
    if hasattr(response, "content"):
        size = len(response.content)
    elif hasattr(response, "block_size"):
        size = response.block_size
    payload["http_request"] = {
        "requestMethod": request.method,
        "requestUrl": request.build_absolute_uri(),
        "userAgent": user_agent,
        "remoteIp": remote_ip,
        "latency": latency,
        "status": response.status_code,
        "responseSize": size,
    }
    if request.resolver_match:
        payload["labels"]["endpoint"] = request.resolver_match.url_name
        path_parameters = request.resolver_match.kwargs
        payload["labels"]["project_id"] = str(path_parameters.get("project_id", "Unknown"))
    _thread_locals.request_logs = payload


class LogAggregatorMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request: WSGIRequest):
        _failure_flag = False
        try:
            # Setting the request logs in prequest
            starting_time = time.perf_counter()
            _thread_locals.aggregate_logs = True
            _thread_locals.request_id = str(uuid.uuid4())
            _thread_locals.log_severity_no = logging.INFO
            _thread_locals.log_severity = "INFO"
            initiate_request_logs(request=request)
            response = self.get_response(request)
        except Exception as e:
            _failure_flag = True
            _thread_locals.aggregate_logs = False
            logging.error(f"Error occurred while initiating logs: {e}")
            response = self.get_response(request)
            return response
        try:
            if not _failure_flag:
                timestamp = time.perf_counter()
                latency = str(timestamp - starting_time) + "s"
                _thread_locals.request_logs["labels"]["tenant_id"] = get_current_tenant()
                write_request_and_response(request, response, latency=latency)
                logging.info("send logs to google cloud")
                del _thread_locals.request_logs
                _thread_locals.aggregate_logs = False
                return response
        except Exception as e:
            logging.error(f"Error occurred while processing logs: {e}")
            return response
