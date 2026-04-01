from __future__ import annotations

import json
import logging
import threading
import urllib.parse
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any, Union
from urllib.parse import parse_qs, unquote, urlparse

import ibis
from ibis.common.exceptions import IbisError
from visitran.adapters.connection import BaseConnection
from visitran.errors import (
    ConnectionFailedError,
    SchemaAlreadyExist,
    DatabasePermissionDeniedError,
    SchemaCreationFailed,
    InvalidConnectionUrlException,
    ConnectionFieldMissingException,
)

warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    module="snowflake.connector.vendored.requests",
)

SCHEMA_FILE_PATH = Path(__file__).with_name("schema.json")


if TYPE_CHECKING:  # pragma: no cover
    from ibis.backends.snowflake import Backend
    from ibis.expr.types.relations import Table


class SnowflakeConnection(BaseConnection):
    """Holds all Snowflake adapters methods."""

    connection_details: dict[str, Union[str, int]] = {}
    dbtype: str = "snowflake"

    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        account: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        role: Optional[str] = None,
        connection_url: Optional[str] = None,
        connection_type: Optional[str] = None,
    ) -> None:
        super().__init__(
            user=username,
            passw=password,
            dbname=database,
            schema=schema,
            host=account,
            port=443,
            connection_url=connection_url,
            connection_type=connection_type,
        )
        if connection_type and connection_type.lower() == "url":
            self._parse_url(connection_url)
        else:
            self.username = username
            self.password = password
            self.account = account
            self.warehouse = warehouse
            self.database = database
            self.schema = schema
            self.role = role
            self.connection_url = self.build_snowflake_url()
            self.connection_type = "host"
        # URL-encode username and password to handle special characters like @, :, etc.
        encoded_username = urllib.parse.quote(str(self.username)) if self.username else ""
        encoded_password = urllib.parse.quote(str(self.password)) if self.password else ""
        self._connection_string: str = f"snowflake://{encoded_username}:{encoded_password}@{self.account}/{self.database}/{self.schema}"
        # Append query parameters
        conn_params = []
        if self.warehouse:
            conn_params.append(f"warehouse={urllib.parse.quote(str(self.warehouse))}")
        if self.role:
            conn_params.append(f"role={urllib.parse.quote(str(self.role))}")
        if conn_params:
            self._connection_string += f"?{'&'.join(conn_params)}"
        self._engine = None
        self._connection_url = self.connection_url
        self.local = threading.local()

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
    def connection(self) -> Backend:
        """Get connection object."""
        if not hasattr(self.local, "connection"):
            try:
                self.local.connection = ibis.snowflake.connect(
                    user=self.username,
                    password=self.password,
                    account=self.account,
                    warehouse=self.warehouse,
                    database=self.database,
                    role=self.role,
                )
            except IbisError as err:
                error_message = str(err).split("failed:")
                failed_message = error_message[len(error_message) - 1].strip()
                raise ConnectionFailedError(db_type="snowflake", error_message=failed_message) from err
            except Exception as err:
                raise ConnectionFailedError(db_type="snowflake", error_message=str(err)) from err
        return self.local.connection

    def list_all_schemas(self) -> list[str]:
        sql_query = """
            SELECT
                schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('INFORMATION_SCHEMA')
            ORDER BY schema_name
        """
        rows = self.connection.raw_sql(sql_query).fetchall()
        schema_names = [row[0] for row in rows]
        return schema_names

    def _use_schema(self, schema_name: str) -> None:
        """Set the active schema for the Snowflake session."""
        qi = self.quote_identifier
        self.connection.raw_sql(f"USE SCHEMA {qi(schema_name)}")

    def list_all_tables(self, schema_name: Union[str, list[str]], database_name: Union[str, None] = None) -> list[str]:
        # Tell Snowflake to use that schema for the session
        self._use_schema(schema_name)
        return self.connection.list_tables()

    def create_table(self, schema_name: str, table_name: str, table_statement: Table) -> None:
        """Returns SQL statement with params to create table."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            # Ensure Snowflake session uses the correct schema
            self._use_schema(schema_name)

            self.connection.create_table(table_name, table_statement, database=schema_name)

    def create_view(self, schema_name: str, view_name: str, table_statement: Table) -> None:
        """Returns SQL statement with params to create View."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            # Ensure Snowflake session uses the correct schema
            self._use_schema(schema_name)

            self.connection.create_view(view_name, table_statement, database=schema_name)

    def insert_into_table(self, schema_name: str, table_name: str, table_statement: Table) -> str:
        """Insert into Table."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            # Ensure Snowflake session uses the correct schema
            self._use_schema(schema_name)

            self.connection.insert(table_name, table_statement, database=schema_name)

    def create_schema(self, schema_name: str) -> None:
        try:
            qi = self.quote_identifier
            conn = self.connection
            conn.raw_sql(f"USE DATABASE {qi(self.database)};")
            conn.raw_sql(f"CREATE SCHEMA IF NOT EXISTS {qi(self.database)}.{qi(schema_name)};")
        except Exception as e:
            if any(keyword in str(e).lower() for keyword in ["permission", "not authorized", "access denied"]):
                raise DatabasePermissionDeniedError(self.schema_name, str(e))
            elif "already exists" in str(e).lower():
                raise SchemaAlreadyExist(self.schema_name, str(e))
            else:
                raise SchemaCreationFailed(schema_name, str(e))

    def _parse_url(self, url: str) -> None:
        """Parse Snowflake connection URL"""
        try:
            # Pattern: snowflake://username:password@account/database/schema?params
            if not url.startswith("snowflake://"):
                raise InvalidConnectionUrlException(error_message="Invalid URI scheme. Expected 'snowflake://'")

            parsed_uri = urlparse(url)

            # Properly decode username and password that may contain special characters
            self.username = unquote(parsed_uri.username) if parsed_uri.username else None
            self.password = unquote(parsed_uri.password) if parsed_uri.password else None
            self.account = parsed_uri.hostname

            path_segments = [s for s in parsed_uri.path.split("/") if s]

            self.database = None
            self.schema = "PUBLIC"

            if len(path_segments) >= 1:
                self.database = path_segments[0]
            if len(path_segments) >= 2:
                self.schema = path_segments[1]

            query_params = parse_qs(parsed_uri.query)

            warehouse_values = query_params.get("warehouse", [None])
            role_values = query_params.get("role", [None])
            self.warehouse = unquote(warehouse_values[0]) if warehouse_values[0] else None
            self.role = unquote(role_values[0]) if role_values[0] else None

            self.host = f"{self.account}.snowflakecomputing.com"
            self.port = 443

        except Exception as e:
            raise InvalidConnectionUrlException(error_message=f"Failed to parse Snowflake URL: {str(e)}")

    def validate(self) -> None:
        """Validate required connection parameters."""
        required = {
            "username": self.username,
            "password": self.password,
            "account": self.account,
            "warehouse": self.warehouse,
            "database": self.database,
            "connection_type": self.connection_type,
        }
        for field, value in required.items():
            if not value:
                raise ConnectionFieldMissingException(missing_fields=field)

    def _build_connection_dict(self):
        return {
            "account": self.account,
            "username": self.username,
            "password": self.password,
            "role": self.role,
            "database": self.database,
            "schema": self.schema,
            "warehouse": self.warehouse,
            "connection_url": self.connection_url,
            "connection_type": self.connection_type,
        }

    def get_connection_details(self):
        return self._redact_connection_details(self._build_connection_dict())

    def get_raw_connection_details(self):
        return self._build_connection_dict()

    def build_snowflake_url(self):
        # URL-encode username and password to handle special characters like @, :, etc.
        encoded_username = urllib.parse.quote(str(self.username)) if self.username else ""
        encoded_password = urllib.parse.quote(str(self.password)) if self.password else ""
        url = f"snowflake://{encoded_username}:{encoded_password}@{self.account}/{self.database}/{self.schema}"

        # Add query parameters
        params = [f"warehouse={urllib.parse.quote(self.warehouse)}"]
        if self.role:
            params.append(f"role={urllib.parse.quote(self.role)}")

        return f"{url}?{'&'.join(params)}"

    def upsert_into_table(
        self,
        schema_name: str,
        table_name: str,
        select_statement: "Table",
        primary_key: Union[str, list[str]],
    ) -> None:
        """Efficient upsert using Snowflake's MERGE INTO statement.

        This approach is optimal for Snowflake because:
        1. MERGE INTO is natively supported and optimized
        2. Atomic operation with ACID properties
        3. Handles both single and composite keys efficiently
        4. Better performance than separate DELETE + INSERT
        """
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

        try:
            # Ensure Snowflake session uses the correct schema
            self._use_schema(schema_name)

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
                MERGE INTO {qi(schema_name)}.{qi(table_name)} AS target
                USING {qi(schema_name)}.{qi(temp_table_name)} AS source
                ON {on_clause}
                WHEN MATCHED THEN
                    UPDATE SET {update_set_clause}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_columns})
                    VALUES ({insert_values})
            """

            self.connection.raw_sql(merge_query)

        except Exception as e:
            logging.error(f"Snowflake upsert failed for {schema_name}.{table_name}: {str(e)}")
            raise Exception(f"Snowflake upsert failed for {schema_name}.{table_name}: {str(e)}") from e
        finally:
            # Clean up temporary table
            try:
                self.connection.raw_sql(f"DROP TABLE IF EXISTS {qi(schema_name)}.{qi(temp_table_name)}")
            except Exception:
                pass  # Ignore cleanup errors
