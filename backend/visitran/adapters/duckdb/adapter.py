from typing import Union

from visitran.adapters.adapter import BaseAdapter
from visitran.adapters.duckdb.connection import DuckDbConnection
from visitran.adapters.duckdb.db_reader import DuckDBReader
from visitran.adapters.duckdb.model import DuckDbModel
from visitran.adapters.duckdb.seed import DuckDbSeed
from visitran.adapters.scd import BaseSCD
from visitran.templates.model import VisitranModel
from visitran.templates.snapshot import VisitranSnapshot


class DuckDbAdapter(BaseAdapter):
    def __init__(self, conn_details: dict[str, Union[str, int]]) -> None:
        super().__init__(conn_details=conn_details)

    def load_connection_obj(self, conn_details: dict[str, Union[str, int]]) -> DuckDbConnection:
        self._connection: DuckDbConnection = DuckDbConnection(conn_details.get("file_path", ""), schema="")
        return self._connection

    @property
    def db_connection(self) -> DuckDbConnection:
        return self._connection

    def load_model(self, model: VisitranModel) -> DuckDbModel:
        self._model = DuckDbModel(db_connection=self.db_connection, model=model)
        return self._model

    def load_seed(self, schema: str, abs_path: str) -> DuckDbSeed:
        self._seed = DuckDbSeed(db_connection=self.db_connection, schema=schema, abs_path=abs_path)
        return self._seed

    def load_scd(self, visitran_snapshot: VisitranSnapshot) -> BaseSCD:
        raise NotImplementedError

    def load_db_reader(self) -> None:
        self._db_reader = DuckDBReader(db_connection=self.db_connection)
        return self._db_reader
