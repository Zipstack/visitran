from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any

from visitran.adapters.seed import BaseSeed

warnings.filterwarnings("ignore", message=".*?pkg_resources.*?")

if TYPE_CHECKING:  # pragma: no cover

    from visitran.adapters.bigquery.connection import BigQueryConnection


class BigQuerySeed(BaseSeed):
    def __init__(self, db_connection: BigQueryConnection, schema: str, abs_path: str):
        self._db_connection: BigQueryConnection = db_connection
        self._statements: list[Any] = []
        self.dataset_id = schema
        self.abs_path = abs_path
        super().__init__(db_connection, schema, abs_path)

    @property
    def db_connection(self) -> BigQueryConnection:
        """Returns the BigQueryConnection object used to connect to the
        database."""
        return self._db_connection
