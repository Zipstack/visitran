import os
from typing import TYPE_CHECKING, NamedTuple

import pytest
import sqlalchemy as sa
from sqlalchemy.engine.base import Engine

if TYPE_CHECKING:  # pragma: no cover
    pass

mysql_password: str = os.getenv("dbpassword", "mysqlpass")


class ConnectionData(NamedTuple):
    ip: str
    port: int
    db_connection_url: str
    password: str
    dbname: str
    catalog: str
    user: str
    schema: str
    engine: Engine


@pytest.fixture(scope="session")
def mysql_sakila_db():
    engine = sa.create_engine(f"mysql+pymysql://visitran:{mysql_password}@localhost:3307/sakila?charset=utf8mb4")

    mysqldata = ConnectionData(
        "localhost",
        3307,
        f"mysql+pymysql://visitran:{mysql_password}@localhost:3307/sakila?charset=utf8mb4",
        mysql_password,
        "sakila",
        "",
        "visitran",
        "sakila",
        engine,
    )

    yield mysqldata

    engine.dispose()
