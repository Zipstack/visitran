from __future__ import annotations

import os

import ibis
import pytest
from ibis.backends.trino import Backend
from pytest_mock import MockerFixture

from adapters.postgres.connection import PostgresConnection
from adapters.trino.connection import TrinoQEConnection

dberrstr = "database name cannot be empty"
dbpassword: str = os.getenv("DB_PASSWORD", "password")


@pytest.mark.adapter
@pytest.mark.minimal_core
class TestAdapter:
    def test_postgres_adapter(self, mocker: MockerFixture) -> None:
        assert PostgresConnection.dbtype == "postgres"
        assert isinstance(PostgresConnection.connection_details, dict)

        mocker.patch("ibis.postgres.connect", return_value=Backend())

        adapter = PostgresConnection(
            host=" localhost ",
            port=5432,
            user=" user",
            passw=dbpassword + " ",
            dbname=" dbname",
            schema=" schema",
        )

        conn = adapter.connection

        assert isinstance(conn, Backend)

        ibis.postgres.connect.assert_called_once_with(
            host="localhost",
            port=5432,
            user="user",
            password=dbpassword,
            database="dbname",
        )

        assert adapter.host == "localhost"
        assert adapter.port == 5432
        assert adapter.user == "user"
        assert adapter.passw == dbpassword
        assert adapter.dbname == "dbname"
        assert adapter.schema == "schema"

        with pytest.raises(ValueError) as err:
            adapter = PostgresConnection(
                host="",
                port=5432,
                user="user",
                passw=dbpassword,
                dbname="dbname",
                schema="schema",
            )
        assert str(err.value) == "host cannot be empty"

        with pytest.raises(ValueError) as err:
            adapter.port = -1
        assert str(err.value) == "port should be between 0 and 65535"

        with pytest.raises(ValueError) as err:
            adapter.user = ""
        assert str(err.value) == "user cannot be empty"

        with pytest.raises(ValueError) as err:
            adapter.passw = ""
        assert str(err.value) == "password cannot be empty"

        with pytest.raises(ValueError) as err:
            adapter.dbname = ""
        assert str(err.value) == dberrstr

        with pytest.raises(ValueError) as err:
            adapter.schema = ""
        assert str(err.value) == "database schema name cannot be empty"

        with pytest.raises(AttributeError):
            adapter.connection = conn  # type: ignore

    def test_trino_adapter(self, mocker: MockerFixture) -> None:
        assert TrinoQEConnection.dbtype == "trino"
        assert isinstance(TrinoQEConnection.connection_details, dict)

        mocker.patch("ibis.trino.connect", return_value=Backend())

        adapter = TrinoQEConnection(
            host=" localhost ",
            port=8080,
            user=" user",
            passw=" " + dbpassword,
            catalog=" dbname",
            schema=" schema",
        )

        conn = adapter.connection

        assert isinstance(conn, Backend)

        ibis.trino.connect.assert_called_once_with(
            host="localhost",
            port=8080,
            user="user",
            password=dbpassword,
            database="dbname",
            schema="schema",
        )

        assert adapter.host == "localhost"
        assert adapter.port == 8080
        assert adapter.user == "user"
        assert adapter.passw == dbpassword
        assert adapter.dbname == "dbname"
        assert adapter.schema == "schema"

        with pytest.raises(ValueError) as err:
            adapter = TrinoQEConnection(
                host=" localhost",
                port=8080,
                user="user",
                passw=dbpassword,
                catalog="",
                schema="schema",
            )
        assert str(err.value) == dberrstr

        with pytest.raises(ValueError) as err:
            adapter.port = 78897
        assert str(err.value) == "port should be between 0 and 65535"

        with pytest.raises(ValueError) as err:
            adapter.user = ""
        assert str(err.value) == "user cannot be empty"

        adapter.passw = ""
        assert adapter.passw == ""

        with pytest.raises(ValueError) as err:
            adapter.dbname = ""
        assert str(err.value) == dberrstr

        with pytest.raises(ValueError) as err:
            adapter.schema = ""
        assert str(err.value) == "database schema name cannot be empty"
