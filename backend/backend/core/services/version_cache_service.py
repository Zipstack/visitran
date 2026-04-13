"""Version cache service — Redis-backed caching for version control queries.

Uses the existing RedisClient singleton from core/redis_client.py.
"""

import json
import logging
import time
from typing import Any

from django.db import transaction

from backend.core.redis_client import RedisClient

logger = logging.getLogger(__name__)

# Cache TTLs (seconds)
VERSION_HISTORY_TTL = 300
LATEST_VERSION_TTL = 300
VERSION_DETAIL_TTL = 600

_PREFIX = "vc"

_stats = {"hits": 0, "misses": 0, "sets": 0, "invalidations": 0}


def _redis() -> RedisClient:
    return RedisClient()


# ------------------------------------------------------------------
# Key generation
# ------------------------------------------------------------------

def _key_version_history(model_id: str, page: int, limit: int) -> str:
    return f"{_PREFIX}:hist:{model_id}:{page}:{limit}"

def _key_latest_version(model_id: str) -> str:
    return f"{_PREFIX}:latest:{model_id}"

def _key_version_detail(model_id: str, version_number: int) -> str:
    return f"{_PREFIX}:ver:{model_id}:{version_number}"


# ------------------------------------------------------------------
# Core get/set via RedisClient with JSON serialization
# ------------------------------------------------------------------

def get_cached(key: str) -> Any | None:
    start = time.monotonic()
    raw = _redis().get(key)
    elapsed_ms = round((time.monotonic() - start) * 1000, 2)
    if raw is not None:
        _stats["hits"] += 1
        logger.debug("Cache HIT: %s (%.2fms)", key, elapsed_ms)
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return raw
    _stats["misses"] += 1
    logger.debug("Cache MISS: %s (%.2fms)", key, elapsed_ms)
    return None


def set_cached(key: str, value: Any, ttl: int) -> None:
    try:
        raw = json.dumps(value, default=str)
        client = _redis()
        client.set(key, raw)
        client.expire(key, ttl)
        _stats["sets"] += 1
    except Exception:
        logger.warning("Cache SET failed for key: %s", key, exc_info=True)


# ------------------------------------------------------------------
# Cached query methods
# ------------------------------------------------------------------

def get_version_history(model_id: str, page: int, limit: int) -> dict[str, Any] | None:
    return get_cached(_key_version_history(model_id, page, limit))

def set_version_history(model_id: str, page: int, limit: int, data: dict[str, Any]) -> None:
    set_cached(_key_version_history(model_id, page, limit), data, VERSION_HISTORY_TTL)

def get_latest_version(model_id: str) -> dict[str, Any] | None:
    return get_cached(_key_latest_version(model_id))

def set_latest_version(model_id: str, data: dict[str, Any]) -> None:
    set_cached(_key_latest_version(model_id), data, LATEST_VERSION_TTL)

def get_version_detail(model_id: str, version_number: int) -> dict[str, Any] | None:
    return get_cached(_key_version_detail(model_id, version_number))

def set_version_detail(model_id: str, version_number: int, data: dict[str, Any]) -> None:
    set_cached(_key_version_detail(model_id, version_number), data, VERSION_DETAIL_TTL)


# ------------------------------------------------------------------
# Invalidation
# ------------------------------------------------------------------

def invalidate_model_versions(model_id: str) -> None:
    keys_to_delete = [_key_latest_version(model_id)]
    for page in range(1, 6):
        for limit in (10, 20, 50):
            keys_to_delete.append(_key_version_history(model_id, page, limit))
    _bulk_delete(keys_to_delete)
    logger.info("Invalidated version cache for model %s (%d keys)", model_id, len(keys_to_delete))


def invalidate_on_commit(model_id: str) -> None:
    def _do_invalidate():
        invalidate_model_versions(model_id)
    transaction.on_commit(_do_invalidate)


def _bulk_delete(keys: list[str]) -> None:
    client = _redis()
    for key in keys:
        try:
            client.delete(key)
        except Exception:
            logger.warning("Cache DELETE failed for key: %s", key, exc_info=True)
    _stats["invalidations"] += len(keys)


# ------------------------------------------------------------------
# Cache warming
# ------------------------------------------------------------------

def warm_after_commit(model_id: str, version_detail: dict[str, Any], version_number: int) -> None:
    set_latest_version(model_id, version_detail)
    set_version_detail(model_id, version_number, version_detail)
    logger.info("Warmed cache for model %s version %d", model_id, version_number)


# ------------------------------------------------------------------
# Stats
# ------------------------------------------------------------------

def get_cache_stats() -> dict[str, Any]:
    total = _stats["hits"] + _stats["misses"]
    hit_rate = (_stats["hits"] / total * 100) if total > 0 else 0.0
    return {
        "hits": _stats["hits"], "misses": _stats["misses"],
        "sets": _stats["sets"], "invalidations": _stats["invalidations"],
        "total_requests": total, "hit_rate_percent": round(hit_rate, 2),
    }

def reset_cache_stats() -> None:
    _stats["hits"] = 0
    _stats["misses"] = 0
    _stats["sets"] = 0
    _stats["invalidations"] = 0
