from typing import Union

from visitran.adapters.adapter import BaseAdapter
from visitran.adapters.databricks.connection import DatabricksConnection
from visitran.adapters.databricks.db_reader import DatabricksDBReader
from visitran.adapters.databricks.model import DatabricksModel
from visitran.adapters.databricks.scd import DatabricksSCD
from visitran.adapters.databricks.seed import DatabricksSeed
from visitran.templates.model import VisitranModel
from visitran.templates.snapshot import VisitranSnapshot


class DatabricksAdapter(BaseAdapter):
    """Databricks adapter for Visitran with Unity Catalog and Delta Lake
    support."""

    def __init__(self, conn_details: dict[str, Union[str, int]]) -> None:
        super().__init__(conn_details=conn_details)

    def load_connection_obj(self, conn_details: dict[str, Union[str, int]]) -> DatabricksConnection:
        """Load Databricks connection object."""
        self._connection: DatabricksConnection = DatabricksConnection(
            server_hostname=str(conn_details.get("server_hostname", "")),
            http_path=str(conn_details.get("http_path", "")),
            access_token=str(conn_details.get("access_token", "")),
            catalog=str(conn_details.get("catalog", "")),
            schema=str(conn_details.get("schema", "default")),
        )
        return self._connection

    @property
    def db_connection(self) -> DatabricksConnection:
        """Get the Databricks connection."""
        return self._connection

    def load_model(self, model: VisitranModel) -> DatabricksModel:
        """Load Databricks model for materialization."""
        self._model = DatabricksModel(db_connection=self.db_connection, model=model)
        return self._model

    def load_seed(self, schema: str, abs_path: str) -> DatabricksSeed:
        """Load Databricks seed for CSV data loading."""
        self._seed = DatabricksSeed(db_connection=self.db_connection, schema=schema, abs_path=abs_path)
        return self._seed

    def load_scd(self, visitran_snapshot: VisitranSnapshot) -> DatabricksSCD:
        """Load Databricks SCD for slowly changing dimensions."""
        self._scd = DatabricksSCD(db_connection=self.db_connection, visitran_scd=visitran_snapshot)
        return self._scd

    def load_db_reader(self) -> DatabricksDBReader:
        """Load Databricks database reader for schema discovery."""
        self._db_reader = DatabricksDBReader(db_connection=self.db_connection)
        return self._db_reader
