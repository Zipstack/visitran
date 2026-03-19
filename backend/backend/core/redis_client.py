import logging
from typing import Any

import redis
from django.conf import settings


class RedisClient:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        self._redis_client: redis.Redis | None | bool = None
        try:
            self._redis_client = redis.Redis.from_url(url=settings.REDIS_URL_STREAMER, decode_responses=True)
        except Exception as e:
            self._redis_client = False
            logging.error(f"Error connecting to redis {e}")

    @property
    def redis_client(self):
        if not self._redis_client:
            logging.error("Redis connection is not active")
            return None
        return self._redis_client

    def pubsub(self):
        if self.redis_client:
            return self.redis_client.pubsub()
        raise ConnectionError("Redis connection is not active")

    def get(self, key: str, default: Any = None) -> Any:
        if self.redis_client:
            value = self.redis_client.get(key) or default
            logging.info(f"Getting redis value for {key}")
            logging.debug(f"redis value for {key} is {value}")
            return value
        return default

    def set(self, key: str, value: Any, *args, **kwargs) -> None:
        if self.redis_client:
            self.redis_client.set(key, value, *args, **kwargs)

    def delete(self, key: str, *args) -> None:
        if self.redis_client:
            self.redis_client.delete(key, *args)

    def expire(self, key: str, ttl: int) -> None:
        if self.redis_client:
            self.redis_client.expire(key, ttl)

    def publish(self, channel: str, message: str) -> None:
        if self.redis_client:
            self.redis_client.publish(channel, message)
        else:
            logging.error("Redis connection is not active. Cannot publish message.")
            raise ConnectionError("Redis connection is not active")
