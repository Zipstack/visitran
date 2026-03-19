from typing import Any

from visitran.adapters.seed import BaseSeed
from visitran.adapters.snowflake.connection import SnowflakeConnection


class SnowflakeSeed(BaseSeed):
    def __init__(self, db_connection: SnowflakeConnection, schema: str, abs_path: str):
        self._db_connection: SnowflakeConnection = db_connection
        super().__init__(db_connection, schema, abs_path)

    @property
    def db_connection(self) -> SnowflakeConnection:
        return self._db_connection
