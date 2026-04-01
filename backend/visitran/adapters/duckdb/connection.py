from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any, Union

import duckdb
import ibis
from ibis.common.exceptions import IbisError
from ibis.expr.types.relations import Table

from visitran.adapters.connection import BaseConnection
from visitran.errors import (
    ConnectionFailedError,
    DatabasePermissionDeniedError,
    SchemaAlreadyExist,
    SchemaCreationFailed,
    TableNotFound,
)

if TYPE_CHECKING:  # pragma: no cover
    from ibis.backends.duckdb import Backend

SCHEMA_FILE_PATH = Path(__file__).with_name("schema.json")


class DuckDbConnection(BaseConnection):
    """Holds all DuckDb adapters methods."""

    connection_details: dict[str, Union[str, int]] = {}
    dbtype: str = "duckdb"
    file_path: str = ":file_path"

    def __init__(
        self,
        file_path: str,
        schema: str = "",
    ) -> None:
        self.file_path = file_path or ""
        self.dbname: str = self._construct_db_name()
        self._connection_string: str = f"duckdb:///{self.dbname}"
        self.local = threading.local()

    @property
    def connection_string(self) -> str:
        """Returns connection string."""
        return self._connection_string

    def _construct_db_name(self) -> str:
        file_path = Path(self.file_path)
        if file_path.is_file():
            return self.file_path
        else:
            return f"{self.file_path or '.'}{os.sep}local.db"

    @classmethod
    def connection_fields(cls) -> dict[str, Any]:
        """Load the connection fields JSON schema from the file."""
        with open(SCHEMA_FILE_PATH, encoding="utf-8") as file:
            connection_fields = json.load(file)
        return connection_fields

    @property
    def connection(self) -> Backend:
        """Get connection object."""
        if not hasattr(self.local, "connection"):
            try:
                self.local.connection = ibis.duckdb.connect(database=self.database_name, read_only=False)
            except IbisError as err:
                error_message = str(err).split("failed:")
                failed_message = error_message[len(error_message) - 1].strip()
                raise ConnectionFailedError(db_type="duckdb", error_message=failed_message) from err
            except Exception as err:
                raise ConnectionFailedError(db_type="duckdb", error_message=str(err)) from err
        return self.local.connection

    def get_connection_details(self):
        return {
            "file_path": self.file_path,
        }

    def validate(self) -> None:
        """Validate required connection parameters."""
        required = {
            "file_path": self.file_path,
        }
        for field, value in required.items():
            if not value:
                raise ConnectionFieldMissingException(missing_fields=field)

    def create_table(self, table_name: str, table_statement: Table) -> None:
        """Create table in DuckDB."""
        self.connection.create_table(table_name, table_statement)

    def create_view(self, view_name: str, table_statement: Table) -> None:
        """Create View in DuckDB."""
        self.connection.create_view(view_name, table_statement)

    def drop_table_if_exist(self, table_name: str) -> None:
        """Drop Table in DuckDB."""
        self.connection.drop_table(table_name, force=True)

    def drop_view_if_exist(self, view_name: str) -> None:
        """Drop a view in DuckDB."""
        self.connection.drop_view(view_name, force=True)

    def export_database(self, export_path: str) -> Any:
        duckdb_connection = duckdb.connect(database=self.database_name, read_only=False)
        result = duckdb_connection.execute(f"EXPORT DATABASE '{export_path}'")
        duckdb_connection.close()
        return result

    def import_database(self, db_path: str, import_path: str) -> Any:
        duckdb_connection = duckdb.connect(database=db_path, read_only=False)
        result = duckdb_connection.execute(f"IMPORT DATABASE '{import_path}'")
        duckdb_connection.close()
        return result

    def get_table_obj(self, schema_name: str, table_name: str) -> Table:
        """Return table object."""
        try:
            return self.connection.table(table_name)
        except Exception as err:
            raise TableNotFound(table_name=table_name, schema_name=schema_name, failure_reason=str(err)) from err

    def insert_csv_records(self, abs_path: str, table_name: str) -> str:
        """Keeping the header and auto_detect true will make the first column
        of the CSV as header."""
        qi = self.quote_identifier
        self.connection.raw_sql(
            f"CREATE OR REPLACE TABLE {qi(table_name)} AS "
            f"SELECT * FROM read_csv('{abs_path}', header=true, auto_detect=true);"
        )

    def list_all_schemas(self) -> list[Any]:
        """Duckdb doesn't has schema name."""
        return ["default"]

    def list_all_tables(self, schema_name: str, database_name: Union[str, None] = None) -> list[str]:
        tables = self.connection.list_tables()
        return tables

    def get_table_row_count(self, schema_name: str, table_name: str) -> int:
        table = self.connection.table(table_name)
        row_count = table.count().execute()
        return row_count

    def create_schema(self, schema_name: str) -> None:
        try:
            qi = self.quote_identifier
            self.connection.raw_sql(f"CREATE SCHEMA IF NOT EXISTS {qi(schema_name)};")
        except Exception as e:
            if any(keyword in str(e).lower() for keyword in ["permission", "not authorized", "access denied"]):
                raise DatabasePermissionDeniedError(self.schema_name, str(e))
            elif "already exists" in str(e).lower():
                raise SchemaAlreadyExist(self.schema_name, str(e))
            else:
                raise SchemaCreationFailed(schema_name, str(e))
