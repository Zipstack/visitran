import os

import pytest

from adapters.duckdb.adapter import DuckDbAdapter
from adapters.duckdb.connection import DuckDbConnection
from adapters.duckdb.model import DuckDbModel
from visitran.templates.model import VisitranModel


@pytest.mark.duckdb
@pytest.mark.minimal_core
@pytest.mark.xdist_group(name="duckdb")
class TestDuckDBAdapter:
    SCRIPT_DIR = os.path.dirname(__file__)  # <-- absolute dir the script is in
    REL_PATH = "../../../integration/data/airbnb/init_duckdb"
    ABS_DB_PATH = os.path.join(SCRIPT_DIR, REL_PATH)

    CONN_DETAILS = {"file_path": ABS_DB_PATH}

    def test_duckdb_adapter(self) -> None:
        visitran_obj = VisitranModel()  # type: ignore[abstract]

        duckdb_adapter = DuckDbAdapter(conn_details=self.CONN_DETAILS)  # type: ignore
        assert duckdb_adapter.db_connection.file_path == self.CONN_DETAILS["file_path"]
        assert isinstance(duckdb_adapter.db_connection, DuckDbConnection)
        assert isinstance(duckdb_adapter.load_model(model=visitran_obj), DuckDbModel)
