from typing import Union

from visitran.adapters.adapter import BaseAdapter
from visitran.adapters.postgres.connection import PostgresConnection
from visitran.adapters.postgres.db_reader import PostgresDBReader
from visitran.adapters.postgres.model import PostgresModel
from visitran.adapters.postgres.scd import PostgresSCD
from visitran.adapters.postgres.seed import PostgresSeed
from visitran.templates.model import VisitranModel
from visitran.templates.snapshot import VisitranSnapshot


class PostgresAdapter(BaseAdapter):
    def __init__(self, conn_details: dict[str, Union[str, int]]) -> None:
        super().__init__(conn_details=conn_details)

    def load_connection_obj(self, conn_details: dict[str, Union[str, int]]) -> PostgresConnection:
        self._connection: PostgresConnection = PostgresConnection(
            host=str(conn_details.get("host", "")),
            port=int(conn_details.get("port", "0")),
            user=str(conn_details.get("user", "")),
            passw=str(conn_details.get("passw", "")),
            dbname=str(conn_details.get("dbname", "")),
            schema=str(conn_details.get("schema", "")),
            connection_url=str(conn_details.get("connection_url", "")),
        )
        return self._connection

    @property
    def db_connection(self) -> PostgresConnection:
        return self._connection

    def load_model(self, model: VisitranModel) -> PostgresModel:
        self._model = PostgresModel(db_connection=self.db_connection, model=model)
        return self._model

    def load_seed(self, schema: str, abs_path: str) -> PostgresSeed:
        self._seed = PostgresSeed(db_connection=self.db_connection, schema=schema, abs_path=abs_path)
        return self._seed

    def load_scd(self, visitran_snapshot: VisitranSnapshot) -> PostgresSCD:
        self._scd = PostgresSCD(db_connection=self.db_connection, visitran_scd=visitran_snapshot)
        return self._scd

    def load_db_reader(self) -> None:
        self._db_reader = PostgresDBReader(db_connection=self.db_connection)
        return self._db_reader
