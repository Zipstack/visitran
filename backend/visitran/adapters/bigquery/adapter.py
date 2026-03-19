from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Union

from visitran.adapters.adapter import BaseAdapter
from visitran.adapters.bigquery.connection import BigQueryConnection
from visitran.adapters.bigquery.db_reader import BigQueryDBReader
from visitran.adapters.bigquery.model import BigQueryModel
from visitran.adapters.bigquery.seed import BigQuerySeed

if TYPE_CHECKING:  # pragma: no cover
    from visitran.adapters.scd import BaseSCD
    from visitran.templates.model import VisitranModel
    from visitran.templates.snapshot import VisitranSnapshot


class BigQueryAdapter(BaseAdapter):
    def __init__(self, conn_details: dict[str, Union[str, int]]) -> None:
        super().__init__(conn_details=conn_details)

    def load_connection_obj(self, conn_details: dict[str, Union[str, int]]) -> BigQueryConnection:
        """Loads a BigQueryConnection object with the given connection
        details."""
        self._connection: BigQueryConnection = BigQueryConnection(
            project_id=conn_details["project_id"],
            dataset_id=conn_details["dataset_id"],
            credentials=conn_details["credentials"],
            connection_url=str(conn_details.get("connection_url", "")),
        )
        return self._connection

    @property
    def db_connection(self) -> BigQueryConnection:
        """Returns the BigQueryConnection object."""
        return self._connection

    def load_model(self, model: VisitranModel) -> BigQueryModel:
        """Loads a BigQueryModel object with the given VisitranModel."""
        self._model = BigQueryModel(db_connection=self.db_connection, model=model)
        return self._model

    def load_seed(self, schema: str, abs_path: str) -> BigQuerySeed:
        """Loads a BigQuerySeed object with the given schema and absolute
        path."""
        self._seed = BigQuerySeed(db_connection=self.db_connection, schema=schema, abs_path=abs_path)
        return self._seed

    def load_scd(self, visitran_snapshot: VisitranSnapshot) -> Optional[BaseSCD]:
        """Loads a BaseSCD object with the given VisitranSnapshot."""
        raise NotImplementedError

    def load_db_reader(self) -> None:
        self._db_reader = BigQueryDBReader(db_connection=self.db_connection)
        return self._db_reader
