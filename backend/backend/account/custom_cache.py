"""Custom cache utilities for account module."""

from django_redis import get_redis_connection


class CustomCache:
    """Custom cache implementation using Redis."""

    def __init__(self) -> None:
        self.cache = get_redis_connection("default")

    def rpush(self, key: str, value: str) -> None:
        """Push a value to the right end of a list."""
        self.cache.rpush(key, value)

    def lrem(self, key: str, value: str) -> None:
        """Remove a value from a list."""
        self.cache.lrem(key, value)

    def lrange(self, key: str, start: int, end: int) -> list:
        """Get a range of values from a list."""
        return self.cache.lrange(key, start, end)
