from __future__ import annotations

import json
import logging
import threading
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any, Union, Optional
from urllib.parse import parse_qs, urlparse

import ibis
from ibis.backends import BaseBackend
from ibis.common.exceptions import IbisError
from sqlalchemy.exc import OperationalError

from visitran.adapters.connection import BaseConnection
from visitran.errors import (
    ConnectionFailedError,
    ConnectionFieldMissingException,
    DatabasePermissionDeniedError,
    InvalidConnectionUrlException,
    SchemaAlreadyExist,
    SchemaCreationFailed,
)

if TYPE_CHECKING:  # pragma: no cover
    from ibis.expr.types.relations import Table

SCHEMA_FILE_PATH = Path(__file__).with_name("schema.json")


class PostgresConnection(BaseConnection):
    """Holds all postgres adapters methods."""

    connection_details: dict[str, Union[str, int]] = {}
    dbtype: str = "postgres"

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        passw: Optional[str] = None,
        dbname: Optional[str] = None,
        schema: Optional[str] = None,
        connection_url: Optional[str] = None,
        connection_type: Optional[str] = None,
    ) -> None:
        super().__init__(
            host=host,
            port=int(port) if port is not None else 5432,
            user=user,
            passw=passw,
            dbname=dbname,
            schema=schema,
            connection_url=connection_url,
            connection_type=connection_type,
        )
        self._connection: str = ""
        self.connection_url: str = connection_url
        self.local = threading.local()

        if connection_type and connection_type.lower() == "url":
            self._parse_url(connection_url)
        else:
            self.host = host
            self.port = port if port is not None else 5432
            self.user = user
            self.passw = passw
            self.dbname = dbname
            self.schema = schema if schema else ""
            self.connection_url = f"postgresql://{user}:{passw}@{host}:{port}/{dbname}"
            self.connection_type = "host"
            if self.schema:
                self.connection_url = self.connection_url + f"?currentSchema={self.schema}"
        self._connection_string: str = (
            f"postgresql+psycopg2://{self.user}:{self.passw}@{self.host}:{self.port}/{self.dbname}"
        )

    @property
    def connection_string(self) -> str:
        """Returns connection string."""
        return self._connection_string

    @classmethod
    def connection_fields(cls) -> dict[str, Any]:
        """Load the connection fields JSON schema from the file."""
        with open(SCHEMA_FILE_PATH, encoding="utf-8") as file:
            connection_fields = json.load(file)
        return connection_fields

    @property
    def connection(self) -> BaseBackend:
        """Get connection object."""
        if not hasattr(self.local, "connection"):
            try:
                self.local.connection = ibis.postgres.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.passw,
                    database=self.dbname,
                )
            except (IbisError, OperationalError) as err:
                error_message = str(err).split("failed:")
                failed_message = error_message[len(error_message) - 1].strip()
                raise ConnectionFailedError(db_type="postgres", error_message=failed_message) from err
            except Exception as err:
                raise ConnectionFailedError(db_type="postgres", error_message=str(err)) from err
        return self.local.connection

    def list_all_schemas(self) -> list[str]:
        """Lists all schemas except system schemas."""
        conn = self.connection
        query = """
            SELECT nspname
            FROM pg_catalog.pg_namespace
            WHERE nspname NOT IN ('information_schema', 'pg_catalog')
                  AND nspname NOT LIKE 'pg_toast%'
                  AND nspname NOT LIKE 'pg_temp_%'
            ORDER BY nspname;
        """
        result = conn.raw_sql(query)
        return [row[0] for row in result.fetchall()]

    def create_schema(self, schema_name: str) -> None:
        try:
            qi = self.quote_identifier
            self.connection.raw_sql(f"CREATE SCHEMA IF NOT EXISTS {qi(schema_name)};")
        except Exception as e:
            if any(keyword in str(e).lower() for keyword in ["permission", "not authorized", "access denied"]):
                raise DatabasePermissionDeniedError(schema_name, str(e))
            elif "already exists" in str(e).lower():
                raise SchemaAlreadyExist(schema_name, str(e))
            else:
                raise SchemaCreationFailed(schema_name, str(e))

    def _parse_url(self, url: str) -> None:
        """Parse PostgreSQL connection URL."""
        try:
            # Pattern: postgresql://user:password@host:port/database?params
            parsed = urlparse(url)
            if parsed.scheme not in ["postgresql", "postgres"]:
                raise InvalidConnectionUrlException(error_message="Invalid PostgreSQL URL scheme")

            self.host = parsed.hostname
            self.port = parsed.port if parsed.port else 5432
            self.user = parsed.username
            self.passw = parsed.password

            # Extract database from path
            if parsed.path:
                self.dbname = parsed.path.lstrip("/")

            # Parse query parameters
            if parsed.query:
                params = parse_qs(parsed.query)
                if "currentSchema" in params:
                    self.schema = params["currentSchema"][0]

        except Exception as e:
            raise InvalidConnectionUrlException(error_message=f"Failed to parse PostgreSQL URL: {str(e)}")

    def _build_connection_dict(self):
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "passw": self.passw,
            "dbname": self.dbname,
            "schema": self.schema,
            "connection_url": self.connection_url,
            "connection_type": self.connection_type,
        }

    def get_connection_details(self):
        return self._redact_connection_details(self._build_connection_dict())

    def get_raw_connection_details(self):
        return self._build_connection_dict()

    def validate(self) -> None:
        """Validate required connection parameters."""
        required = {
            "host": self.host,
            "user": self.user,
            "passw": self.passw,
            "dbname": self.dbname,
            "connection_type": self.connection_type,
        }
        for field, value in required.items():
            if not value:
                raise ConnectionFieldMissingException(missing_fields=field)

    def insert_into_table(self, schema_name: str, table_name: str, table_statement: Table) -> str:
        """Insert into Table."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.connection.insert(table_name, table_statement, database=schema_name)

    def upsert_into_table(
        self,
        schema_name: str,
        table_name: str,
        select_statement: Table,
        primary_key: Union[str, list[str]],
    ) -> None:
        """Efficient upsert using PostgreSQL's INSERT ... ON CONFLICT.

        This approach is optimal for PostgreSQL because:
        1. PostgreSQL's INSERT ... ON CONFLICT is highly efficient
        2. No temporary tables needed
        3. Atomic operation
        4. Better performance than MERGE for PostgreSQL
        """

        # Handle both single column and composite keys
        if isinstance(primary_key, str):
            key_columns = [primary_key]
        else:
            key_columns = primary_key

        # Get target table columns
        target_columns = self.get_table_columns(schema_name=schema_name, table_name=table_name)

        # Ensure unique constraint exists on primary key columns
        try:
            self._ensure_unique_constraint(schema_name, table_name, key_columns)
        except Exception as e:
            # If constraint creation fails, fall back to DELETE + INSERT strategy
            if "already exists" in str(e).lower():
                pass
            else:
                self._fallback_upsert(schema_name, table_name, select_statement, key_columns)
                return

        qi = self.quote_identifier

        # Build the ON CONFLICT clause
        conflict_columns = ", ".join([qi(col) for col in key_columns])

        # Build the UPDATE SET clause (update all columns except primary key)
        update_columns = [col for col in target_columns if col not in key_columns]
        update_set_clause = ", ".join([f"{qi(col)} = EXCLUDED.{qi(col)}" for col in update_columns])

        # Compile the select statement
        compiled_select = select_statement.compile()

        # Build the upsert query
        upsert_query = f"""
            INSERT INTO {qi(schema_name)}.{qi(table_name)}
            ({', '.join([qi(col) for col in target_columns])})
            {compiled_select}
            ON CONFLICT ({conflict_columns})
            DO UPDATE SET {update_set_clause}
        """

        # Execute the upsert
        self.connection.raw_sql(upsert_query)





    def _ensure_unique_constraint(self, schema_name: str, table_name: str, key_columns: list[str]) -> None:
        """Ensure a unique constraint exists on the specified columns."""
        try:
            qi = self.quote_identifier
            # Check if constraint already exists
            constraint_name = f"{table_name}_{'_'.join(key_columns)}_key"

            # Try to add unique constraint if it doesn't exist
            constraint_columns = ", ".join([qi(col) for col in key_columns])
            add_constraint_sql = f"""
                ALTER TABLE {qi(schema_name)}.{qi(table_name)}
                ADD CONSTRAINT {qi(constraint_name)} UNIQUE ({constraint_columns})
            """

            self.connection.raw_sql(add_constraint_sql)

        except Exception as e:
            # If constraint already exists, continue; otherwise bubble up for caller to handle
            if "already exists" in str(e).lower():
                pass
            else:
                raise

    def _fallback_upsert(self, schema_name: str, table_name: str, select_statement: Table, key_columns: list[str]) -> None:
        """Fallback upsert using DELETE + INSERT for tables without unique
        constraints."""
        qi = self.quote_identifier
        # Get table columns
        columns = self.get_table_columns(schema_name=schema_name, table_name=table_name)

        # Build WHERE clause for DELETE
        where_conditions = []
        for key_col in key_columns:
            where_conditions.append(f'{qi(key_col)} = source.{qi(key_col)}')
        where_clause = " AND ".join(where_conditions)

        # Compile the select statement
        compiled_select = select_statement.compile()

        # Build DELETE + INSERT query
        fallback_query = f"""
            WITH source_data AS ({compiled_select})
            DELETE FROM {qi(schema_name)}.{qi(table_name)} target
            WHERE EXISTS (
                SELECT 1 FROM source_data source
                WHERE {where_clause}
            );

            INSERT INTO {qi(schema_name)}.{qi(table_name)}
            ({', '.join([qi(col) for col in columns])})
            {compiled_select};
        """

        # Execute the fallback upsert
        self.connection.raw_sql(fallback_query)
