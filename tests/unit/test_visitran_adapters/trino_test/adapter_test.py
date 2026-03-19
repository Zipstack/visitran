import pytest

from adapters.trino.adapter import TrinoQEAdapter
from adapters.trino.connection import TrinoQEConnection
from adapters.trino.model import TrinoModel
from visitran.templates.model import VisitranModel


@pytest.mark.trino
@pytest.mark.trino_postgres
@pytest.mark.xdist_group(name="trino")
class TestTrinoQEAdapter:
    CONN_DETAILS = {
        "host": "localhost",
        "port": 8080,
        "user": "pytest",
        "passw": "",
        "catalog": "postgres",
        "schema": "raw",
    }

    def test_trino_adapter(self) -> None:
        visitran_obj = VisitranModel()  # type: ignore[abstract]

        trino_adapter = TrinoQEAdapter(conn_details=self.CONN_DETAILS)  # type: ignore
        assert isinstance(trino_adapter.db_connection, TrinoQEConnection)
        assert isinstance(trino_adapter.load_model(model=visitran_obj), TrinoModel)
