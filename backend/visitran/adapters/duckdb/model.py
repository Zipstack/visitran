from typing import Any

from visitran.adapters.duckdb.connection import DuckDbConnection
from visitran.adapters.model import BaseModel
from visitran.templates.model import VisitranModel


class DuckDbModel(BaseModel):
    def __init__(self, db_connection: DuckDbConnection, model: VisitranModel) -> None:
        super().__init__(db_connection, model)
        self._statements: list[Any] = []
        self._db_connection: DuckDbConnection = db_connection

    @property
    def db_connection(self) -> DuckDbConnection:
        return self._db_connection

    def execute_ephemeral(self) -> None:
        return

    def execute_table(self) -> None:
        self.db_connection.drop_table_if_exist(
            table_name=self.model.destination_table_name,
        )
        self.db_connection.create_table(
            self.model.destination_table_name,
            self.model.select_statement,
        )
        table_obj = self.db_connection.get_table_obj(
            schema_name=self.model.destination_schema_name,
            table_name=self.model.destination_table_name,
        )
        self.model.destination_table_obj = table_obj

    def execute_view(self) -> None:
        self.db_connection.drop_view(view_name=self.model.destination_table_name)
        self.db_connection.create_view(self.model.destination_table_name, self.model.select_statement)
        # Executing all the constructed SQL statements in adapters
        self.db_connection.bulk_execute_statements(statements=self._statements)

        table_obj = self.db_connection.get_table_obj(
            schema_name=self.model.destination_schema_name,
            table_name=self.model.destination_table_name,
        )
        self.model.destination_table_obj = table_obj

    def execute_incremental(self) -> None:
        if self.model.destination_table_exists:
            # then call select_if_incremental
            # insert the results into table
            self.model.select_statement = self.model.select_if_incremental()
            self.db_connection.insert_into_table(self.model.destination_table_name, self.model.select_statement)
        else:
            self.db_connection.drop_table_if_exist(self.model.destination_table_name)
            self.db_connection.create_table(self.model.destination_table_name, self.model.select_statement)
        table_obj = self.db_connection.get_table_obj(
            schema_name=self.model.destination_schema_name,
            table_name=self.model.destination_table_name,
        )
        self.model.destination_table_obj = table_obj
