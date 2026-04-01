import sqlalchemy

from visitran.adapters.db_reader import BaseDBReader
from visitran.adapters.postgres.connection import PostgresConnection


class PostgresDBReader(BaseDBReader):
    def __init__(self, db_connection: PostgresConnection) -> None:
        super().__init__(db_connection)
        self.sqlalchemy_engine = sqlalchemy.create_engine(
            self.connection.connection_string,
            isolation_level="REPEATABLE READ",
        )
        self.inspector = sqlalchemy.inspect(self.sqlalchemy_engine)
