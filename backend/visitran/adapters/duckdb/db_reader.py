import sqlalchemy

from visitran.adapters.db_reader import BaseDBReader
from visitran.adapters.duckdb.connection import DuckDbConnection


class DuckDBReader(BaseDBReader):
    def __init__(self, db_connection: DuckDbConnection) -> None:
        super().__init__(db_connection)
        self.sqlalchemy_engine = sqlalchemy.create_engine(
            self.connection.connection_string,
            isolation_level="REPEATABLE READ",
        )
        self.inspector = sqlalchemy.inspect(self.sqlalchemy_engine)
