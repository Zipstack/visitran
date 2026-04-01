import datetime
import decimal
import logging
import math
import traceback
from functools import wraps
from typing import Any

from django.core.cache import cache
from rest_framework import status
from rest_framework.response import Response

from backend.core.redis_client import RedisClient
from backend.errors.exceptions import VisitranBackendBaseException
from visitran.errors import VisitranBaseExceptions


def handle_http_request(func) -> Any:
    """This decorator is used to handle the router exceptions and some pre-
    request validations if needed."""

    lock_endpoints = ["execute-seed-command", "execute-run-command"]

    roll_back_endpoints = ["execute-run-command"]

    def handle_exceptions(*args, **kwargs) -> Response:
        view_name = args[0].resolver_match.view_name
        project_id = kwargs.get("project_id")
        lock_key = f"{view_name}_{project_id}"
        try:
            logging.info(f"Creating a lock - {lock_key}")
            if view_name in lock_endpoints:
                _has_lock = cache.get(lock_key)
                if _has_lock:
                    return Response(
                        data={"status": "failed", "error_message": "Previous action is still in process"},
                        status=status.HTTP_429_TOO_MANY_REQUESTS,
                    )
                cache.set(lock_key, True, timeout=120)
            response: Response = func(*args, **kwargs)
            response.status_code = status.HTTP_200_OK
            logging.info(f"Deleting lock - {lock_key}")
            cache.delete(lock_key)
            return response
        except (VisitranBackendBaseException, VisitranBaseExceptions) as visitran_err:
            exception_type = "backend" if isinstance(visitran_err, VisitranBackendBaseException) else "core"
            status_code = (
                visitran_err.status_code if hasattr(visitran_err, "status_code") else status.HTTP_400_BAD_REQUEST
            )

            logging.error(f" -- Visitran {exception_type} exceptions {visitran_err.__class__.__name__} --")
            logging.error(
                f"Handle Visitran{exception_type.title()}Exception: {repr(visitran_err)}, "
                f"error_string: {visitran_err.error_args()}, "
                f"args: {args}, kwargs: {kwargs}, function_name: {func.__name__}"
            )
            logging.error(traceback.format_exc())

            response_data = visitran_err.error_response()
            response_data["is_rollback"] = visitran_err.error_args().get("is_rollback", False)

            logging.error(f" response data - {response_data}")
            logging.info(f" deleting the lock - {lock_key}")
            cache.delete(lock_key)

            return Response(data=response_data, status=status_code)
        except Exception as visitran_err:
            # Some unhandled error where occurred
            logging.critical(
                f"Handle Exception: {repr(visitran_err)}, args: {args}, \
                    kwargs: {kwargs}, function_name: {func.__name__}"
            )
            logging.critical(f"-- {visitran_err.__str__()} --")
            logging.critical(traceback.format_exc())
            cache.delete(lock_key)
            return Response(
                data={"status": "failed", "error_message": visitran_err.__str__()},
                status=status.HTTP_400_BAD_REQUEST,
            )
        finally:
            logging.info("--- Request processed ---")

    return handle_exceptions


def sanitize_data(data):
    """Recursively convert any Python structure into a JSON‑serialisable form.

    ── Rules applied ──────────────────────────────────────────────────────
    • decimal.Decimal → int (if integral) or float
    • float NaN / ±Inf → None
    • tuple            → list
    • dict / list      → processed recursively
    """
    # ── Decimal handling ──
    if isinstance(data, decimal.Decimal):
        data = int(data) if data == data.to_integral_value() else float(data)

    # ── Float sanity check ──
    if isinstance(data, float):
        if math.isnan(data) or math.isinf(data):
            return None
        return data  # already safe

    # ── Tuples → lists ──
    if isinstance(data, tuple):
        return [sanitize_data(item) for item in data]

    # ── Lists ──
    if isinstance(data, list):
        return [sanitize_data(item) for item in data]

    # ── Dicts ──
    if isinstance(data, dict):
        return {k: sanitize_data(v) for k, v in data.items()}

    if isinstance(data, datetime.datetime):
        return str(data)

    # ── Primitives (str, int, bool, None, etc.) ──
    return data


def redis_singleton_lock(ttl: int = 600):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            data = kwargs.get("data") or (args[1] if len(args) > 1 else None)

            chat_message_id = None
            if isinstance(data, dict):
                chat_message_id = data.get("chatMessageId") or data.get("chat_message_id")

            if not chat_message_id:
                return func(*args, **kwargs)

            redis = RedisClient().redis_client

            key = f"transformation:{chat_message_id}:lock"

            acquired = redis.setnx(key, "1")
            if not acquired:
                logging.warning(f"[RedisLock] Duplicate ignored for chat_message_id={chat_message_id}")
                return None

            redis.expire(key, ttl)

            try:
                return func(*args, **kwargs)
            except Exception:
                redis.delete(key)
                raise

        return wrapper

    return decorator
