from typing import Any

from visitran.adapters.seed import BaseSeed
from visitran.adapters.trino.connection import TrinoQEConnection


class TrinoSeed(BaseSeed):
    def __init__(self, db_connection: TrinoQEConnection, schema: str, abs_path: str):
        self._db_connection: TrinoQEConnection = db_connection
        self._statements: list[Any] = []
        super().__init__(db_connection, schema, abs_path)

    @property
    def db_connection(self) -> TrinoQEConnection:
        return self._db_connection
