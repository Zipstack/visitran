from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

import ibis
from ibis.common.exceptions import IbisError
from visitran.adapters.connection import BaseConnection
from visitran.errors import (
    ConnectionFailedError,
    DatabasePermissionDeniedError,
    SchemaAlreadyExist,
    SchemaCreationFailed,
    ConnectionFieldMissingException,
)

if TYPE_CHECKING:  # pragma: no cover
    from ibis.backends import BaseBackend
    from ibis.expr.types.relations import Table

SCHEMA_FILE_PATH = Path(__file__).with_name("schema.json")


class DatabricksConnection(BaseConnection):
    """Holds all Databricks adapter methods."""

    connection_details: dict[str, Union[str, int]] = {}
    dbtype: str = "databricks"

    @staticmethod
    def quote_identifier(identifier: str) -> str:
        """Databricks uses backtick quoting for identifiers."""
        safe = identifier.replace("`", "\\`")
        return f"`{safe}`"

    def __init__(
        self,
        server_hostname: Optional[str] = None,
        http_path: Optional[str] = None,
        access_token: Optional[str] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> None:
        # Sanitize inputs before any usage (including super().__init__)
        from urllib.parse import urlparse

        if server_hostname:
            server_hostname = server_hostname.strip()
            if not server_hostname.startswith(("http://", "https://")):
                server_hostname = f"https://{server_hostname}"

            parsed = urlparse(server_hostname)
            server_hostname = parsed.hostname
        if http_path:
            http_path = http_path.strip()

        catalog = catalog if catalog else ""
        schema = schema if schema else ""

        super().__init__(
            host=server_hostname,
            passw=access_token,
            dbname=catalog,
            schema=schema,
        )
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.access_token = access_token
        self.catalog = catalog
        self.schema = schema
        self._connection_string: str = self._build_connection_string()
        self.local = threading.local()

    def _build_connection_string(self) -> str:
        """Build Databricks connection string."""
        http_path = self.http_path.lstrip("/") if self.http_path else ""
        conn_str = f"databricks://{self.server_hostname}/{http_path}"
        params = []
        if self.catalog:
            params.append(f"catalog={self.catalog}")
        if self.schema:
            params.append(f"schema={self.schema}")
        if params:
            conn_str += f"?{'&'.join(params)}"
        return conn_str

    @property
    def connection_string(self) -> str:
        """Returns connection string."""
        return self._connection_string

    @classmethod
    def connection_fields(cls) -> dict[str, Any]:
        """Load the connection fields JSON schema from the file."""
        with open(SCHEMA_FILE_PATH, "r", encoding="utf-8") as file:
            connection_fields = json.load(file)
        return connection_fields

    @property
    def connection(self) -> BaseBackend:
        """Get connection object."""
        if not hasattr(self.local, "connection"):
            try:
                # Build connection parameters
                conn_params = {
                    "server_hostname": self.server_hostname,
                    "http_path": self.http_path,
                    "access_token": self.access_token,
                }

                # Add optional parameters
                if self.catalog:
                    conn_params["catalog"] = self.catalog
                if self.schema:
                    conn_params["schema"] = self.schema

                self.local.connection = ibis.databricks.connect(**conn_params)
            except IbisError as err:
                error_message = str(err).split("failed:")
                failed_message = error_message[len(error_message) - 1].strip()
                raise ConnectionFailedError(db_type="databricks", error_message=failed_message) from err
            except Exception as err:
                raise ConnectionFailedError(db_type="databricks", error_message=str(err)) from err
        return self.local.connection

    def list_all_schemas(self) -> list[str]:
        """Lists all schemas in the current catalog.

        Exceptions propagate to callers — test_connection_data and
        the schema browser both have their own error handling.
        """
        schemas = self.connection.list_databases()
        return [s for s in schemas if s.lower() != "information_schema"]

    def list_all_tables(self, schema_name: str | list[str], database_name: str | None = None) -> list[str]:
        """Lists all tables in the specified schema."""
        return self.connection.list_tables(database=schema_name)

    def create_table(self, schema_name: str, table_name: str, table_statement: Table) -> None:
        """Create table in Databricks with catalog support."""
        # Databricks creates tables in Delta format by default
        # Catalog is already set in connection, just specify database (schema)
        self.connection.create_table(
            table_name,
            table_statement,
            database=schema_name,
        )

    def create_view(self, schema_name: str, view_name: str, table_statement: Table) -> None:
        """Create view in Databricks with catalog support."""
        # Catalog is already set in connection, just specify database (schema)
        self.connection.create_view(
            view_name,
            table_statement,
            database=schema_name,
        )

    def insert_into_table(self, schema_name: str, table_name: str, table_statement: Table) -> None:
        """Insert data into Databricks table."""
        # Catalog is already set in connection, just specify database (schema)
        self.connection.insert(
            table_name,
            table_statement,
            database=schema_name,
        )

    def create_schema(self, schema_name: str) -> None:
        """Create schema in Databricks.

        Uses backtick-quoted identifiers per Databricks SQL syntax.
        Qualifies with catalog when configured, otherwise uses session default.
        """
        qi = self.quote_identifier
        if self.catalog:
            sql = f"CREATE SCHEMA IF NOT EXISTS {qi(self.catalog)}.{qi(schema_name)}"
        else:
            sql = f"CREATE SCHEMA IF NOT EXISTS {qi(schema_name)}"
        try:
            cursor = self.connection.raw_sql(sql)
            cursor.close()
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ("permission", "not authorized", "access denied")):
                raise DatabasePermissionDeniedError(schema_name, str(e))
            elif "already exists" in error_msg:
                raise SchemaAlreadyExist(schema_name, str(e))
            else:
                raise SchemaCreationFailed(schema_name, str(e))

    def _build_connection_dict(self) -> dict[str, Any]:
        return {
            "server_hostname": self.server_hostname,
            "http_path": self.http_path,
            "access_token": self.access_token,
            "catalog": self.catalog,
            "schema": self.schema,
        }

    def get_connection_details(self) -> dict[str, Any]:
        """Return connection details."""
        return self._redact_connection_details(self._build_connection_dict())

    def get_raw_connection_details(self) -> dict[str, Any]:
        return self._build_connection_dict()

    def validate(self) -> None:
        """Validate required connection parameters."""
        required = {
            "server_hostname": self.server_hostname,
            "http_path": self.http_path,
            "access_token": self.access_token,
            "catalog": self.catalog,
        }
        for field, value in required.items():
            if not value:
                raise ConnectionFieldMissingException(missing_fields=field)

    def upsert_into_table(
        self,
        schema_name: str,
        table_name: str,
        select_statement: "Table",
        primary_key: Union[str, list[str]],
    ) -> dict:
        """Efficient upsert using Databricks Delta Lake's MERGE INTO statement.
        Returns dict with rows_affected from cursor.rowcount.
        """
        rowcount = None

        # Handle both single column and composite keys
        if isinstance(primary_key, str):
            key_columns = [primary_key]
        else:
            key_columns = primary_key

        # Get target table columns
        target_columns = self.get_table_columns(schema_name=schema_name, table_name=table_name)

        # Create temporary table name
        temp_table_name = f"{table_name}__temp"

        qi = self.quote_identifier

        # Build fully qualified table names (with catalog if specified)
        if self.catalog:
            target_table_fq = f"{qi(self.catalog)}.{qi(schema_name)}.{qi(table_name)}"
            temp_table_fq = f"{qi(self.catalog)}.{qi(schema_name)}.{qi(temp_table_name)}"
        else:
            target_table_fq = f"{qi(schema_name)}.{qi(table_name)}"
            temp_table_fq = f"{qi(schema_name)}.{qi(temp_table_name)}"

        try:
            # 1. Create temporary table with incremental data
            self.connection.create_table(temp_table_name, select_statement, database=schema_name)

            # 2. Build the MERGE statement
            on_conditions = [f"target.{qi(k)} = source.{qi(k)}" for k in key_columns]
            on_clause = " AND ".join(on_conditions)

            update_columns = [col for col in target_columns if col not in key_columns]
            update_set_clause = ", ".join([f"target.{qi(col)} = source.{qi(col)}" for col in update_columns])

            insert_columns = ", ".join([qi(col) for col in target_columns])
            insert_values = ", ".join([f"source.{qi(col)}" for col in target_columns])

            merge_query = f"""
                MERGE INTO {target_table_fq} AS target
                USING {temp_table_fq} AS source
                ON {on_clause}
                WHEN MATCHED THEN
                    UPDATE SET {update_set_clause}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_columns})
                    VALUES ({insert_values})
            """

            merge_cursor = self.connection.raw_sql(merge_query)
            _rc = merge_cursor.rowcount if hasattr(merge_cursor, "rowcount") else None
            rowcount = _rc if (_rc is not None and _rc >= 0) else None
            try:
                merge_cursor.close()
            except Exception:
                pass
            logging.info("Databricks MERGE completed for %s.%s", schema_name, table_name)

        except Exception as e:
            logging.error("Databricks upsert failed for %s.%s: %s", schema_name, table_name, e)
            raise ConnectionFailedError(
                db_type="databricks",
                error_message=f"Upsert failed for {schema_name}.{table_name}: {e}",
            ) from e
        finally:
            try:
                cleanup_cursor = self.connection.raw_sql(f"DROP TABLE IF EXISTS {temp_table_fq}")
                cleanup_cursor.close()
            except Exception:
                pass
        return {"rows_affected": rowcount}
