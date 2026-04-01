from __future__ import annotations

import logging
import warnings
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Union

import numpy as np
import pandas as pd
import yaml
from ibis.common import exceptions as ibis_exceptions
from psycopg.errors import UndefinedTable
from sqlalchemy import text
from sqlalchemy.engine import Connection
from visitran.errors import TableNotFound, SqlQueryFailed, VisitranBaseExceptions, ConnectionFailedError
from visitran.events.functions import fire_event
from visitran.events.types import BulkExecuteError, InvalidProfileTemplateYAML

if TYPE_CHECKING:  # pragma: no cover
    from ibis.backends.base import BaseBackend
    from ibis.expr.types.relations import Table
    from psycopg import Cursor


class BaseConnection(ABC):
    """Base class for all SQL adapters.

    TBD: use pydantic dataclasses and add better validations.
    """

    connection_details: dict[str, Union[str, int]] = {}
    dbtype: str = ""
    # Fields that should be redacted in get_connection_details() responses
    _SENSITIVE_FIELDS = {"passw", "password", "access_token", "credentials", "credentials_dict"}

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
        super().__init__()
        self.host = host
        self.port = port
        self.user = user
        self.passw = passw
        self.dbname = dbname
        self.schema = schema
        self.connection_url = connection_url
        self.connection_type = connection_type

    def __str__(self) -> str:
        return f'"{self.dbname}"."{self.schema}"'

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(host={self.host}, user={self.user}, dbname={self.dbname})>"

    @staticmethod
    def quote_identifier(identifier: str) -> str:
        """Safely quote a SQL identifier (schema, table, column name).

        Uses standard SQL double-quote escaping: any embedded double-quote
        characters are doubled.  Override in subclasses that need different
        quoting (e.g. backticks for BigQuery / Databricks).
        """
        safe = identifier.replace('"', '""')
        return f'"{safe}"'

    @classmethod
    def _redact_connection_details(cls, details: dict) -> dict:
        """Return a copy of connection details with sensitive values masked."""
        redacted = {}
        for key, value in details.items():
            if key in cls._SENSITIVE_FIELDS:
                redacted[key] = "******" if value else ""
            elif key == "connection_url" and value:
                from urllib.parse import urlparse, urlunparse

                try:
                    parsed = urlparse(value)
                    if parsed.password:
                        redacted_netloc = f"{parsed.username}:******@{parsed.hostname}"
                        if parsed.port:
                            redacted_netloc += f":{parsed.port}"
                        redacted[key] = urlunparse(parsed._replace(netloc=redacted_netloc))
                    else:
                        redacted[key] = value
                except Exception:
                    redacted[key] = "******"
            else:
                redacted[key] = value
        return redacted

    @property
    def database_name(self):
        return self.dbname

    @property
    @abstractmethod
    def connection(self) -> BaseBackend:
        """Get connection object."""
        raise NotImplementedError

    @property
    @abstractmethod
    def connection_string(self) -> str:
        """Returns connection string."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def connection_fields(cls) -> dict[str, Any]:
        """Ask user db connection details."""
        raise NotImplementedError

    @abstractmethod
    def get_connection_details(self) -> dict[str, Any]:
        raise NotImplementedError

    def get_raw_connection_details(self) -> dict[str, Any]:
        """Return connection details WITHOUT masking sensitive fields.

        Used when the data needs to be persisted (the model layer handles
        encryption).  Subclasses that override ``get_connection_details``
        should also override this method if their raw dict differs from the
        redacted one.  By default this falls back to ``get_connection_details``
        for backward compatibility.
        """
        return self.get_connection_details()

    def export_database(self, export_path: str) -> None:
        """Abstract method to export database for given db path."""
        pass

    def import_database(self, db_path: str, import_path: str) -> None:
        """Abstract method to import database for given db path."""
        pass

    @abstractmethod
    def validate(self):
        raise NotImplementedError

    @classmethod
    def create_profile(cls, file_path: str, project_name: str) -> None:
        """Create a visitran profile."""
        conn_yaml = {cls.dbtype: cls.connection_details}
        proj_yaml = {project_name: conn_yaml}

        try:
            with open(file_path, encoding="utf-8") as readfd:
                data = yaml.safe_load(readfd)
                data = data or {}
                newdata = {**data, **proj_yaml}
            with open(file_path, "w", encoding="utf-8") as writefd:
                yaml.dump(newdata, writefd, default_flow_style=False, sort_keys=False)
        except Exception:
            fire_event(InvalidProfileTemplateYAML())
            raise

    def get_table_obj(self, schema_name: str, table_name: str):
        """Return table object."""
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                table_obj = self.connection.table(table_name, database=schema_name)
                if table_obj.columns:
                    return table_obj

                # Re-initialize connection to fix issue with newly created tables
                table_obj = self.connection.table(table_name, database=schema_name)
                return table_obj
        except VisitranBaseExceptions as err:
            raise err
        except ibis_exceptions.TableNotFound as err:
            raise TableNotFound(table_name=table_name, schema_name=schema_name, failure_reason=str(err)) from err
        except ibis_exceptions.IbisError as ibis_err:
            db_type = getattr(self, 'connection_type', None) or self.dbtype
            raise ConnectionFailedError(db_type=db_type, error_message=str(ibis_err)) from ibis_err
        except Exception as unhandled_err:
            db_type = getattr(self, 'connection_type', None) or self.dbtype
            raise ConnectionFailedError(db_type=db_type, error_message=str(unhandled_err)) from unhandled_err

    def is_table_exists(self, schema_name: str, table_name: str) -> bool:
        """Returns TRUE if table exists in DB."""
        try:
            self.get_table_obj(schema_name=schema_name, table_name=table_name)
            return True
        except TableNotFound:
            return False

    @staticmethod
    def _get_column_type(col_type: str) -> str:
        _col_mapper = {"int32": "integer", "string": "text", "boolean": "bool"}
        return _col_mapper.get(col_type, col_type)

    def add_column(self, schema_name: str, table_name: str, column_name: str, column_type: str) -> str:
        """Returns SQL statement for adding a new column."""
        qi = self.quote_identifier
        return (
            f"ALTER TABLE {qi(schema_name)}.{qi(table_name)} "
            f"ADD COLUMN {qi(column_name)} {self._get_column_type(column_type)};"
        )

    def get_table_columns(self, schema_name: str, table_name: str) -> list[str]:
        """Returns the list of coloumns in the given table name."""
        table_obj: Table = self.get_table_obj(schema_name=schema_name, table_name=table_name)
        return list(table_obj.columns)

    def get_table_columns_with_type(self, schema_name: str, table_name: str) -> list[dict[str, Any]]:
        """Returns the list of columns with their DB type from the table
        name."""
        columns = []
        table_obj: Table = self.get_table_obj(schema_name=schema_name, table_name=table_name)
        column_names = table_obj.columns
        for column_name in column_names:
            columns.append(
                {
                    "column_name": column_name,
                    "column_dbtype": str(table_obj[column_name].type()),
                    "nullable": table_obj[column_name].type().nullable,
                }
            )
        return columns

    def list_all_schemas(self) -> list[str]:
        return self.connection.list_databases()

    def list_all_tables(self, schema_name: str | List[str], database_name: str | None = None) -> list[str]:
        return self.connection.list_tables(database=schema_name)

    def create_schema(self, schema_name: str) -> None:
        qi = self.quote_identifier
        self.connection.execute(
            text(f"CREATE SCHEMA IF NOT EXISTS {qi(self.dbname.upper())}.{qi(schema_name.upper())};")
        )

    def get_table_records(
        self,
        schema_name: str,
        table_name: str,
        selective_columns: list[str] | None = None,
        order_by: str = "",
        limit: int = 100,
        pagination: int = 1,
    ) -> list[Any]:
        selective_columns = selective_columns or []
        # Load the table
        table = self.get_table_obj(schema_name=schema_name, table_name=table_name)

        # Select columns if specified
        if selective_columns and selective_columns != ["*"]:
            table = table.select(selective_columns)

        # Apply ordering if specified
        if order_by:
            table = table.order_by(order_by)

        # Apply pagination
        offset = limit * (pagination - 1)
        table = table[offset : offset + limit]

        # Execute and fetch results
        result = table.execute()
        result = self.custom_fill_na(result)
        return result.to_dict("records")

    def custom_fill_na(self, result) -> Any:
        try:
            for col in result.columns:

                if pd.api.types.is_datetime64_any_dtype(result[col]):
                    result[col] = result[col].replace({pd.NaT: None})

                # Handle numeric columns
                elif pd.api.types.is_numeric_dtype(result[col]):
                    result[col] = result[col].replace({np.nan: None})

                # Handle object/string columns
                else:
                    result[col] = result[col].replace({pd.NA: None})
                    result[col] = result[col].replace({np.nan: None})
        except Exception as e:
            logging.error(f"failed to parse null data from db content")
        return result

    def get_table_row_count(self, schema_name: str, table_name: str) -> int:
        """Returns the number of rows in a table."""
        table = self.get_table_obj(schema_name=schema_name, table_name=table_name)
        row_count = table.count().execute()
        return row_count

    def drop_view_if_exist(self, schema_name: str, view_name: str) -> None:
        """Returns SQL statement for Drop View only if it exists."""
        self.connection.drop_view(view_name, database=schema_name, force=True)

    def drop_table_if_exist(self, schema_name: str, table_name: str) -> None:
        """Returns SQL statement for Drop Table only if it exists."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.connection.drop_table(table_name, database=schema_name, force=True)

    def create_table(self, schema_name: str, table_name: str, table_statement: Table) -> None:
        """Returns SQL statement with params to create table."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.connection.create_table(table_name, table_statement, database=schema_name)

    def create_view(self, schema_name: str, view_name: str, table_statement: Table) -> None:
        """Returns SQL statement with params to create View."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.connection.create_view(view_name, table_statement, database=schema_name)

    def insert_into_table(self, schema_name: str, table_name: str, table_statement: Table) -> str:
        """Insert into Table."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.connection.insert(table_name, table_statement, database=schema_name)

    def bulk_execute_statements(self, statements: list[Any]) -> bool:
        """Executes the given list of statements in DB one by one."""
        try:
            for sql in statements:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    self.connection.raw_sql(sql)

            return True
        except Exception as err:
            fire_event(BulkExecuteError(str(statements), repr(err)))
            raise SqlQueryFailed(query_statements=statements, error_message=str(err)) from err

    def execute_llm_sql_query(self, sql_query: str, limit: int = 100) -> dict[str, Any]:
        if self.connection.name == "postgres":
            return self.execute_sql_postgres(sql_query, limit)

        if self.connection.name == "bigquery":
            return self.execute_sql_bigquery(sql_query, limit)

        if self.connection.name == "databricks":
            return self.execute_sql_databricks(sql_query, limit)

        return {"status": "failed", "error_message": "Unknown database engine type"}

    def execute_sql_query(self, sql_query: str, limit: int = 100) -> dict[str, Any]:
        """Executes the sql query in DB.

        Fetches and returns both column names and query result rows.
        """
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                cursor: Cursor = self.connection.raw_sql(sql_query)
                columns = []
                if column_description := cursor.description:
                    for desc in column_description:
                        if isinstance(desc, (tuple, list, set)):
                            desc = desc[0]
                        else:
                            desc = desc.name
                        columns.append(desc)
                    # Fetching only first 100 records
                    rows = cursor.fetchmany(size=limit)
                    if rows:
                        return {
                            "columns": columns,
                            "rows": rows,
                            "status": "success",
                            "result": f"Fetched {limit} records",
                        }
                return {
                    "result": "**Query executed successfully**\nNo data found for the executed query.",
                    "status": "success",
                }

        except UndefinedTable as err:
            invalid_tables: list[str] = str(err).split(" ")
            invalid_table_name: str = invalid_tables[len(invalid_tables) - 1]  # Extract table name
            # raise SqlQueryFailed(query_statements=[sql_query], error_message=invalid_table_name) from err
            return {"status": "failed",
                    "error_message": f'**SQL Transformation Error**\nThe query generated for transformation - "{sql_query}" failed with error: "{str(err)}".\nReview the SQL syntax or the referenced columns and tables.'}


        except Exception as err:
            fire_event(BulkExecuteError(str(sql_query), repr(err)))
            # raise SqlQueryFailed(query_statements=[sql_query], error_message=str(err)) from err
            return {"status": "failed", "error_message": f'**SQL Transformation Error**\nThe query generated for transformation - "{sql_query}" failed with error: "{str(err)}".\nReview the SQL syntax or the referenced columns and tables.'}

    def execute_sql_postgres(self, sql_query: str, limit: int = 100) -> dict[str, Any]:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                cursor: Cursor = self.connection.raw_sql(sql_query)

                # SELECT → cursor.description available
                if cursor.description:
                    columns = []
                    for desc in cursor.description:
                        if isinstance(desc, (tuple, list, set)):
                            col = desc[0]
                        else:
                            col = desc.name
                        columns.append(col)

                    rows = cursor.fetchmany(size=limit)

                    if rows:
                        return {
                            "columns": columns,
                            "rows": rows,
                            "status": "success",
                            "result": f"Fetched {len(rows)} records",
                        }

                    return {
                        "status": "success",
                        "result": "**Query executed successfully**\nNo data found for the executed query.",
                    }

                # DDL / DML → no cursor.description
                return {
                    "status": "success",
                    "result": "**Query executed successfully (DDL/DML)**",
                }

        except UndefinedTable as err:
            invalid_tables = str(err).split(" ")
            invalid_table_name = invalid_tables[-1]

            return {
                "status": "failed",
                "error_message": (
                    f'**SQL Transformation Error**\n'
                    f'The query generated for transformation - "{sql_query}" '
                    f'failed with error: "{str(err)}".\n'
                    f'Review invalid table "{invalid_table_name}".'
                ),
            }

        except Exception as err:
            fire_event(BulkExecuteError(str(sql_query), repr(err)))

            return {
                "status": "failed",
                "error_message": (
                    f'**SQL Transformation Error**\n'
                    f'The query generated for transformation - "{sql_query}" '
                    f'failed with error: "{str(err)}".\n'
                    f'Review the SQL syntax or the referenced columns and tables.'
                ),
            }

    def execute_sql_bigquery(self, sql_query: str, limit: int = 100) -> dict[str, Any]:
        try:
            # Run the query
            job = self.connection.raw_sql(sql_query)

            # DDL/DML check
            ddl_types = {
                "CREATE_TABLE",
                "CREATE_TABLE_AS_SELECT",
                "DROP_TABLE",
                "ALTER_TABLE",
                "INSERT",
                "UPDATE",
                "DELETE",
            }

            # If the query is DDL/DML, just return success
            if getattr(job, "statement_type", None) in ddl_types:
                return {
                    "status": "success",
                    "result": f"**Query executed successfully (BigQuery {job.statement_type})**",
                }

            # For SELECT queries, convert to pandas
            df = job.to_dataframe()  # no .result() needed
            if df.empty:
                return {
                    "status": "success",
                    "result": "**Query executed successfully**\nNo data found.",
                }

            # Limit rows if needed
            limited_df = df.head(limit)

            return {
                "columns": list(limited_df.columns),
                "rows": [tuple(x) for x in limited_df.to_numpy()],
                "status": "success",
                "result": f"Fetched {len(limited_df)} records",
            }

        except Exception as err:
            fire_event(BulkExecuteError(str(sql_query), repr(err)))
            return {
                "status": "failed",
                "error_message": (
                    f'**SQL Transformation Error**\n'
                    f'The query generated for transformation - "{sql_query}" '
                    f'failed with error: "{str(err)}".\n'
                    f'Review the SQL syntax or referenced tables.'
                ),
            }

    def execute_sql_databricks(self, sql_query: str, limit: int = 100) -> dict[str, Any]:
        """Execute SQL on Databricks via DBAPI2 cursor.

        Databricks cursors hold active ODBC connections and must be
        explicitly closed — handled via try/finally.
        """
        cursor = None
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                cursor = self.connection.raw_sql(sql_query)

                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchmany(size=limit)

                    if rows:
                        return {
                            "columns": columns,
                            "rows": rows,
                            "status": "success",
                            "result": f"Fetched {len(rows)} records",
                        }

                    return {
                        "status": "success",
                        "result": "**Query executed successfully**\nNo data found.",
                    }

                return {
                    "status": "success",
                    "result": "**Query executed successfully (DDL/DML)**",
                }

        except Exception as err:
            fire_event(BulkExecuteError(str(sql_query), repr(err)))
            return {
                "status": "failed",
                "error_message": (
                    f'**SQL Transformation Error**\n'
                    f'The query - "{sql_query}" failed with error: "{err}".\n'
                    f'Review the SQL syntax or the referenced columns and tables.'
                ),
            }
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass

    def close_connection(self) -> None:
        """This terminates the IBIS connection."""
        try:
            # Trying to close the db connections, This fails for duckdb kind of databases,.
            self.connection.disconnect()
        except Exception:
            pass
