from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any, Union, Optional

import ibis
from ibis.common.exceptions import IbisError
from visitran.adapters.connection import BaseConnection
from visitran.errors import (
    ConnectionFailedError,
    DatabasePermissionDeniedError,
    SchemaAlreadyExist,
    SchemaCreationFailed,
)

if TYPE_CHECKING:  # pragma: no cover
    from ibis.backends.trino import Backend
    from ibis.expr.types.relations import Table

SCHEMA_FILE_PATH = Path(__file__).with_name("schema.json")


class TrinoQEConnection(BaseConnection):
    """Holds all Trino adapters methods."""

    connection_details: dict[str, Union[str, int]] = {}
    dbtype: str = "trino"

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        passw: str,
        catalog: str,
        schema: str,
        connection_url: Optional[str] = None,
    ) -> None:
        super().__init__(
            host=host, port=port, user=user, passw=passw, dbname=catalog, schema=schema, connection_url=connection_url
        )
        self._connection_string: str = f"trino://{user}:{passw}@{host}:{port}/{catalog}"
        self._connection_url: str = connection_url
        self.local = threading.local()

    @property
    def passw(self) -> str:
        """Get password."""
        return self._passw

    @passw.setter
    def passw(self, p: str) -> None:
        """Set password, can be empty."""
        self._passw = p.strip()

    @property
    def connection(self) -> Backend:
        """Returns connection object."""
        if not hasattr(self.local, "connection"):
            try:
                self.local.connection = ibis.trino.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    database=self.dbname,
                    schema=self.schema or 'default',
                )
            except IbisError as err:
                error_message = str(err).split("failed:")
                failed_message = error_message[len(error_message) - 1].strip()
                raise ConnectionFailedError(db_type="trino", error_message=failed_message) from err
            except Exception as err:
                raise ConnectionFailedError(db_type="trino", error_message=str(err)) from err
        return self.local.connection

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

    def create_schema(self, schema_name: str) -> None:
        try:
            qi = self.quote_identifier
            self.connection.raw_sql(f"CREATE SCHEMA IF NOT EXISTS {qi(schema_name)}")
        except Exception as e:
            if any(keyword in str(e).lower() for keyword in ["permission", "not authorized", "access denied"]):
                raise DatabasePermissionDeniedError(self.schema_name, str(e))
            elif "already exists" in str(e).lower():
                raise SchemaAlreadyExist(self.schema_name, str(e))
            else:
                raise SchemaCreationFailed(schema_name, str(e))

    def _build_connection_dict(self):
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "passw": self.passw,
            "catalog": self.dbname,
            "schema": self.schema,
            "connection_url": self.connection_url,
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
            "catalog": self.dbname,
        }
        for field, value in required.items():
            if not value:
                from visitran.errors import ConnectionFieldMissingException
                raise ConnectionFieldMissingException(missing_fields=field)

    def list_all_schemas(self) -> list[str]:
        # Get all databases and filter out system schemas
        all_databases = self.connection.list_databases()

        # Filter out system schemas
        non_system_databases = [
            db for db in all_databases
            if db not in ['information_schema', 'sys', 'system']
        ]

        return non_system_databases

    def upsert_into_table(
        self,
        schema_name: str,
        table_name: str,
        select_statement: Table,
        primary_key: Union[str, list[str]],
    ) -> None:
        """Efficient upsert using DELETE + INSERT strategy for Trino.

        Notes:
        - MERGE can be expensive in Trino due to full joins. We avoid it.
        - When a primary_key is provided (single or composite), we perform:
          1) DELETE matching rows in target
          2) INSERT all rows from the temp incremental table
        - Without a primary_key we fall back to simple INSERT (may create duplicates).
        """
        # Normalize primary key(s)
        if isinstance(primary_key, str):
            key_columns = [primary_key]
        else:
            key_columns = primary_key

        # Columns of the target table
        target_columns = self.get_table_columns(schema_name=schema_name, table_name=table_name)

        # Temp table name
        temp_table_name = f"{table_name}__temp"

        qi = self.quote_identifier

        try:
            # 1. Create temp table with incremental data
            self.connection.create_table(temp_table_name, select_statement, database=schema_name)

            if key_columns and len(key_columns) > 0:
                # 2a. DELETE matching rows based on key(s)
                where_conditions = [f"target.{qi(col)} = source.{qi(col)}" for col in key_columns]
                where_clause = " AND ".join(where_conditions)

                delete_sql = f"""
                    DELETE FROM {qi(schema_name)}.{qi(table_name)} AS target
                    WHERE EXISTS (
                        SELECT 1 FROM {qi(schema_name)}.{qi(temp_table_name)} AS source
                        WHERE {where_clause}
                    )
                """
                self.connection.raw_sql(delete_sql)

            # 2b. INSERT all rows from temp (includes new/updated)
            insert_cols = ", ".join([qi(c) for c in target_columns])
            insert_sql = f"""
                INSERT INTO {qi(schema_name)}.{qi(table_name)} ({insert_cols})
                SELECT {insert_cols}
                FROM {qi(schema_name)}.{qi(temp_table_name)}
            """
            self.connection.raw_sql(insert_sql)

        except Exception as e:
            logging.error(f"Trino upsert (DELETE+INSERT) failed for {schema_name}.{table_name}: {str(e)}")
            raise Exception(
                f"Trino upsert (DELETE+INSERT) failed for {schema_name}.{table_name}: {str(e)}"
            ) from e
        finally:
            # Clean up temp table
            try:
                self.connection.raw_sql(f"DROP TABLE IF EXISTS {qi(schema_name)}.{qi(temp_table_name)}")
            except Exception:
                pass
