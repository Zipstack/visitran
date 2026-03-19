import sqlalchemy

from visitran.adapters.db_reader import BaseDBReader
from visitran.adapters.trino.connection import TrinoQEConnection


class TrinoDBReader(BaseDBReader):
    def __init__(self, db_connection: TrinoQEConnection) -> None:
        super().__init__(db_connection)
        self.sqlalchemy_engine = sqlalchemy.create_engine(
            self.connection.connection_string,
        )
        self.inspector = sqlalchemy.inspect(self.sqlalchemy_engine)
