import os

import pytest

from adapters.postgres.adapter import PostgresAdapter
from adapters.postgres.connection import PostgresConnection
from adapters.postgres.model import PostgresModel
from adapters.postgres.scd import PostgresSCD
from visitran.templates.model import VisitranModel
from visitran.templates.snapshot import VisitranSnapshot

postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")


@pytest.mark.postgres
@pytest.mark.xdist_group(name="postgres")
class TestPostgtesAdapter:
    CONN_DETAILS = {
        "host": postgres_host,
        "port": 5432,
        "user": "postgres",
        "passw": os.getenv("dbpassword", "pgpass"),
        "dbname": "airbnb",
        "schema": "dev",
    }

    def test_postgres_adapter(self) -> None:
        visitran_obj = VisitranModel()  # type: ignore[abstract]
        visitran_scd = VisitranSnapshot()

        postgres_adapter = PostgresAdapter(conn_details=self.CONN_DETAILS)  # type: ignore
        assert isinstance(postgres_adapter.db_connection, PostgresConnection)
        assert isinstance(postgres_adapter.load_model(model=visitran_obj), PostgresModel)
        assert isinstance(postgres_adapter.load_scd(visitran_snapshot=visitran_scd), PostgresSCD)
