import os

import pytest

from adapters.snowflake.adapter import SnowflakeAdapter
from adapters.snowflake.connection import SnowflakeConnection
from adapters.snowflake.model import SnowflakeModel
from visitran.templates.model import VisitranModel


@pytest.mark.snowflake
@pytest.mark.xdist_group(name="snowflake")
class TestSnowflakeAdapter:
    SCRIPT_DIR = os.path.dirname(__file__)  # <-- absolute dir the script is in
    REL_PATH = "../../integration/data/airbnb/init_snowflake"
    ABS_DB_PATH = os.path.join(SCRIPT_DIR, REL_PATH)
    snowflake_user: str = os.getenv("SNOWFLAKE_USERNAME", "snowflake_user")
    snowflake_pass: str = os.getenv("SNOWFLAKE_PASSWORD", "snowflake_password")
    snowflake_account: str = os.getenv("SNOWFLAKE_ACCOUNT", "snowflake_account")

    CONN_DETAILS = {
        "username": snowflake_user,
        "password": snowflake_user,
        "account": snowflake_user,
        "warehouse": "COMPUTE_WH",
        "database": "TEST_AIRBNB",
        "schema": "RAW",
        "role": "ACCOUNTADMIN",
    }

    def test_snowflake_adapter(self) -> None:
        visitran_obj = VisitranModel()  # type: ignore[abstract]

        snowflake_adapter = SnowflakeAdapter(conn_details=self.CONN_DETAILS)  # type: ignore
        assert isinstance(snowflake_adapter.db_connection, SnowflakeConnection)
        assert isinstance(snowflake_adapter.load_model(model=visitran_obj), SnowflakeModel)
