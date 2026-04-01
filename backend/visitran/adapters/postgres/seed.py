import ibis
import pandas as pd
from typing import Any

from visitran.adapters.postgres.connection import PostgresConnection
from visitran.adapters.seed import BaseSeed


class PostgresSeed(BaseSeed):
    def __init__(self, db_connection: PostgresConnection, schema: str, abs_path: str):
        self._db_connection: PostgresConnection = db_connection
        self._statements: list[Any] = []
        super().__init__(db_connection, schema, abs_path)

    @property
    def db_connection(self) -> PostgresConnection:
        return self._db_connection

