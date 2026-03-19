import logging
import re
from typing import Optional, Any

from django.conf import settings
from django.core.cache import cache

logger = logging.getLogger(__name__)

# Redis key used to store the shared key registry across all worker processes
_REGISTRY_CACHE_KEY = "oss_cache_key_registry"


class OssCacheService:
    _local_key_registry = set()

    @classmethod
    def _get_registry(cls) -> set:
        """Get key registry from shared cache (Redis) if available,
        fall back to local in-memory set for single-process dev."""
        try:
            registry = cache.get(_REGISTRY_CACHE_KEY)
            if registry is not None:
                return set(registry)
        except Exception as e:
            logger.warning(f"Failed to read cache registry: {e}")
        return cls._local_key_registry

    @classmethod
    def _save_registry(cls, registry: set) -> None:
        """Persist key registry to shared cache."""
        try:
            cache.set(_REGISTRY_CACHE_KEY, list(registry), timeout=None)
        except Exception as e:
            logger.warning(f"Failed to save cache registry: {e}")
        cls._local_key_registry = registry

    @staticmethod
    def get_key(key: str) -> Optional[Any]:
        data = cache.get(str(key))
        return data.decode("utf-8") if isinstance(data, bytes) else data

    @classmethod
    def set_key(cls,
        key: str, value: Any, expire: int = int(settings.CACHE_TTL_SEC)
    ) -> None:
        key = str(key)
        registry = cls._get_registry()
        registry.add(key)
        cls._save_registry(registry)
        cache.set(key, value, expire)

    @classmethod
    def clear_cache(cls, key_pattern: str) -> Any:
        pattern = key_pattern.replace("*", ".*")
        regex = re.compile(f"^{pattern}$")

        registry = cls._get_registry()
        keys_to_delete = [key for key in registry if regex.match(key)]
        for key in keys_to_delete:
            cache.delete(key)
            registry.discard(key)
        cls._save_registry(registry)

    @staticmethod
    def delete_a_key(key: str, version: Any = None) -> None:
        cache.delete(key, version)

