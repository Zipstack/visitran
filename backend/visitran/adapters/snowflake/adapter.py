from typing import Union

from visitran.adapters.adapter import BaseAdapter
from visitran.adapters.scd import BaseSCD
from visitran.adapters.snowflake.connection import SnowflakeConnection
from visitran.adapters.snowflake.db_reader import SnowflakeDBReader
from visitran.adapters.snowflake.model import SnowflakeModel
from visitran.adapters.snowflake.seed import SnowflakeSeed
from visitran.templates.model import VisitranModel
from visitran.templates.snapshot import VisitranSnapshot


class SnowflakeAdapter(BaseAdapter):
    def load_connection_obj(self, conn_details: dict[str, Union[str, int]]) -> SnowflakeConnection:
        self._connection: SnowflakeConnection = SnowflakeConnection(
            username=str(conn_details.get("username", "")),
            password=str(conn_details.get("password", "")),
            account=str(conn_details.get("account", "")),
            warehouse=str(conn_details.get("warehouse", "")),
            database=str(conn_details.get("database", "")),
            schema=str(conn_details.get("schema", "")),
            role=str(conn_details.get("role", "")),
            connection_url=str(conn_details.get("connection_url", "")),
        )
        return self._connection

    @property
    def db_connection(self) -> SnowflakeConnection:
        return self._connection

    def load_model(self, model: VisitranModel) -> SnowflakeModel:
        self._model = SnowflakeModel(db_connection=self.db_connection, model=model)
        return self._model

    def load_seed(self, schema: str, abs_path: str) -> SnowflakeSeed:
        self._seed = SnowflakeSeed(db_connection=self.db_connection, schema=schema, abs_path=abs_path)
        return self._seed

    def load_db_reader(self) -> None:
        self._db_reader = SnowflakeDBReader(db_connection=self.db_connection)
        return self._db_reader

    def load_scd(self, visitran_snapshot: VisitranSnapshot) -> BaseSCD:
        raise NotImplementedError
