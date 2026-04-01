from typing import Any

from visitran.adapters.duckdb.connection import DuckDbConnection
from visitran.adapters.seed import BaseSeed
from visitran.errors import SeedFailureException


class DuckDbSeed(BaseSeed):
    def __init__(self, db_connection: DuckDbConnection, schema: str, abs_path: str):
        self._db_connection: DuckDbConnection = db_connection
        super().__init__(db_connection, schema, abs_path)

    @property
    def db_connection(self) -> DuckDbConnection:
        return self._db_connection

    def execute(self) -> None:
        """
        This checks the CSV file is exist in DB, and creates schema in the
        target database from project configuration and inserts the CSV
        records.
        Overiding the base execute method to use the duckdb inbuild method
        """

        if self.db_connection is None:
            raise SeedFailureException(
                seed_file_name=self.csv_file_name,
                error_message="Database connection is not available. Please check your connection settings.",
            )

        # The drop SQL query will drop the table if it is only exists !
        # Constructing SQL statement for CSV schema in target adapters
        try:
            self.db_connection.insert_csv_records(abs_path=self.abs_path, table_name=self.destination_table_name)
        except SeedFailureException:
            raise
        except Exception as err:
            raise SeedFailureException(
                seed_file_name=self.csv_file_name,
                error_message=f'Failed to seed table "{self.destination_table_name}": {err}',
            )
