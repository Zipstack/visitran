from typing import Union

from visitran.adapters.adapter import BaseAdapter
from visitran.adapters.scd import BaseSCD
from visitran.adapters.trino.connection import TrinoQEConnection
from visitran.adapters.trino.model import TrinoModel
from visitran.adapters.trino.seed import TrinoSeed
from visitran.adapters.trino.db_reader import TrinoDBReader
from visitran.templates.model import VisitranModel
from visitran.templates.snapshot import VisitranSnapshot


class TrinoQEAdapter(BaseAdapter):
    def __init__(self, conn_details: dict[str, Union[str, int]]) -> None:
        super().__init__(conn_details=conn_details)

    def load_connection_obj(self, conn_details: dict[str, Union[str, int]]) -> TrinoQEConnection:
        self._connection: TrinoQEConnection = TrinoQEConnection(
            host=str(conn_details.get("host", "")),
            port=int(conn_details.get("port", "0")),
            user=str(conn_details.get("user", "")),
            passw=str(conn_details.get("passw", "")),
            catalog=str(conn_details.get("catalog", "")),
            schema=str(conn_details.get("schema", "")),
            connection_url=str(conn_details.get("connection_url", "")),
        )
        return self._connection

    @property
    def db_connection(self) -> TrinoQEConnection:
        return self._connection

    def load_model(self, model: VisitranModel) -> TrinoModel:
        self._model = TrinoModel(db_connection=self.db_connection, model=model)
        return self._model

    def load_seed(self, schema: str, abs_path: str) -> TrinoSeed:
        self._seed = TrinoSeed(db_connection=self.db_connection, schema=schema, abs_path=abs_path)
        return self._seed

    def load_scd(self, visitran_snapshot: VisitranSnapshot) -> BaseSCD:
        raise NotImplementedError

    def load_db_reader(self) -> None:
        self._db_reader = TrinoDBReader(db_connection=self.db_connection)
        return self._db_reader
