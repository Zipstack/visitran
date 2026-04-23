from __future__ import annotations

import base64
import json
import logging
import os
import threading
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any, Union, Optional
import re
from urllib.parse import parse_qs
from urllib.parse import quote_plus

import ibis
from google.cloud import bigquery
from google.oauth2 import service_account
from ibis.common.exceptions import IbisError
from visitran.adapters.connection import BaseConnection
from visitran.errors import (
    TableNotFound,
    ConnectionFailedError,
    DatabasePermissionDeniedError,
    SchemaAlreadyExist,
    SchemaCreationFailed,
    InvalidConnectionUrlException,
    ConnectionFieldMissingException,
)
from visitran.events.functions import fire_event
from visitran.events.types import (
    MergeInToTable,
    SetExpiration,
    TableExists,
    UsingCachedObject,
)

if TYPE_CHECKING:  # pragma: no cover
    from ibis.backends import BaseBackend
    from ibis.expr.types.relations import Table
    from sqlalchemy.sql.selectable import Select

SCHEMA_FILE_PATH = Path(__file__).with_name("schema.json")
SET_EXPIRATION_TIME = "TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 1 hour)"


class BigQueryConnection(BaseConnection):
    """Holds all bigquery adapters methods."""

    connection_details: dict[str, Union[str, int]] = {}
    dbtype: str = "bigquery"

    @staticmethod
    def quote_identifier(identifier: str) -> str:
        """BigQuery uses backtick quoting for identifiers."""
        safe = identifier.replace("`", "\\`")
        return f"`{safe}`"

    def __init__(
        self,
        project_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
        credentials: Optional[dict] = None,
        connection_url: Optional[str] = None,
    ) -> None:
        if connection_url:
            self._parse_url(connection_url)
            self.connection_url: str = connection_url
        else:
            self.project_id = project_id
            self.dataset_id = dataset_id
            self.credentials_dict = credentials if isinstance(credentials, dict) else (json.loads(credentials) if isinstance(credentials, str) else "")
            self.connection_url = "" #self.build_bigquery_url()
        self.credentials = service_account.Credentials.from_service_account_info(self.credentials_dict)
        self._connection_string: str = f"bigquery://{self.project_id}/{self.dataset_id}"
        self.local = threading.local()

    @property
    def database_name(self):
        return self.project_id

    @property
    def connection_string(self) -> str:
        """Returns connection string."""
        return self._connection_string

    @property
    def schema(self):
        return self.dataset_id

    @classmethod
    def connection_fields(cls) -> dict[str, Any]:
        """Load the connection fields JSON schema from the file."""
        with open(SCHEMA_FILE_PATH, "r", encoding="utf-8") as file:
            connection_fields = json.load(file)
        return connection_fields

    @property
    def connection(self) -> BaseBackend:
        """Get the connection object to the BigQuery database.

        If the connection object has already been created, it is
        returned. Otherwise, a new connection object is created using
        the connection details provided during initialization.
        """
        if not hasattr(self.local, "connection"):
            try:
                fire_event(UsingCachedObject("bigquery_connection_object", False))
                self.local.connection = ibis.bigquery.connect(
                    project_id=self.project_id, dataset_id=self.dataset_id, credentials=self.credentials
                )
            except IbisError as err:
                error_message = str(err).split("failed:")
                failed_message = error_message[len(error_message) - 1].strip()
                raise ConnectionFailedError(db_type="bigquery", error_message=failed_message) from err
            except Exception as err:
                raise ConnectionFailedError(db_type="bigquery", error_message=str(err)) from err
        else:
            fire_event(UsingCachedObject("bigquery_connection_object", True))

        return self.local.connection

    def create_or_replace_table(
        self, schema_name: str, table_name: str, select_statement: Table, **kwargs: str
    ) -> None:
        """Creates or replaces a table in the specified BigQuery dataset with
        the given name and schema.

        The table is created using the specified SELECT statement. If
        the table already exists, it is replaced with the new table. The
        table is created with the specified options.
        """
        if os.environ.get("visitran_test_running", "") == "true":
            fire_event(SetExpiration(schema_name=schema_name, table_name=table_name, expiration=1))
            kwargs["expiration_timestamp"] = SET_EXPIRATION_TIME
        qi = self.quote_identifier
        full_table_name = f"{qi(schema_name)}.{qi(table_name)}"
        statement: Select = select_statement.compile()
        query = f"""
            CREATE OR REPLACE TABLE {full_table_name}
            OPTIONS({', '.join([f'{k}={v}' for k, v in kwargs.items()])})
            AS {statement}
        """
        self.connection.raw_sql(query)

    def create_or_replace_view(self, schema_name: str, table_name: str, select_statement: Table, **kwargs: str) -> None:
        """Creates or replaces a view in the specified BigQuery dataset with
        the given name and schema.

        The view is created using the specified SELECT statement. If the
        view already exists, it is replaced with the new view. The view
        is created with the specified options.
        """
        if os.environ.get("visitran_test_running", "") == "true":
            fire_event(SetExpiration(schema_name=schema_name, table_name=table_name, expiration=1))
            kwargs["expiration_timestamp"] = SET_EXPIRATION_TIME
        qi = self.quote_identifier
        full_table_name = f"{qi(schema_name)}.{qi(table_name)}"
        statement: Select = select_statement.compile()
        query = f"""
            CREATE OR REPLACE VIEW {full_table_name}
            OPTIONS({', '.join([f'{k}={v}' for k, v in kwargs.items()])})
            AS {statement}
        """
        self.connection.raw_sql(query)

        self.bulk_execute_statements(sql_statements=[query])

    def is_table_exists(self, schema_name: str, table_name: str) -> bool:
        """Returns TRUE if table exists in DB.

        Falls back to BigQuery client API if Ibis fails (e.g., for tables with INTERVAL columns).
        """
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=DeprecationWarning)
            from google.api_core.exceptions import NotFound

            try:
                self.get_table_obj(schema_name=schema_name, table_name=table_name)
                fire_event(TableExists(schema_name=schema_name, table_name=table_name, exists=True))
                return True
            except (NotFound, TableNotFound):
                fire_event(TableExists(schema_name=schema_name, table_name=table_name, exists=False))
                return False
            except ConnectionFailedError as e:
                # Handle Ibis INTERVAL parsing errors by using BigQuery client directly
                error_msg = str(e).lower()
                if "interval" in error_msg or "precision" in error_msg:
                    return self._is_table_exists_via_client(schema_name, table_name)
                raise

    def _is_table_exists_via_client(self, schema_name: str, table_name: str) -> bool:
        """Check table existence using BigQuery client (bypasses Ibis schema parsing).

        This is a fallback for tables with INTERVAL columns that Ibis can't handle.
        """
        from google.api_core.exceptions import NotFound

        try:
            client = bigquery.Client(project=self.project_id, credentials=self.credentials)
            table_ref = f"{self.project_id}.{schema_name}.{table_name}"
            client.get_table(table_ref)
            fire_event(TableExists(schema_name=schema_name, table_name=table_name, exists=True))
            return True
        except NotFound:
            fire_event(TableExists(schema_name=schema_name, table_name=table_name, exists=False))
            return False

    def get_table_obj(self, schema_name: str, table_name: str):
        """Return table object, handling INTERVAL columns that Ibis can't parse.

        If Ibis fails due to INTERVAL columns, creates a temporary view that casts
        INTERVAL columns to STRING, allowing the table to be used in transformations.
        """
        try:
            return super().get_table_obj(schema_name, table_name)
        except ConnectionFailedError as e:
            error_msg = str(e).lower()
            if "interval" in error_msg or "precision" in error_msg:
                return self._get_table_obj_with_interval_workaround(schema_name, table_name)
            raise

    def _get_table_obj_with_interval_workaround(self, schema_name: str, table_name: str):
        """Create a table object for tables with INTERVAL columns.

        Creates a temporary view that casts INTERVAL columns to STRING,
        allowing Ibis to work with the table.
        """
        import logging
        import uuid

        logging.warning(
            f"Table '{schema_name}.{table_name}' has INTERVAL columns that Ibis can't handle. "
            "Creating a workaround view with INTERVAL columns cast to STRING."
        )

        # Get table schema using BigQuery client
        client = bigquery.Client(project=self.project_id, credentials=self.credentials)
        table_ref = f"{self.project_id}.{schema_name}.{table_name}"
        bq_table = client.get_table(table_ref)

        qi = self.quote_identifier

        # Build SELECT with INTERVAL columns cast to STRING
        select_columns = []
        for field in bq_table.schema:
            if field.field_type == "INTERVAL":
                select_columns.append(f"CAST({qi(field.name)} AS STRING) AS {qi(field.name)}")
            else:
                select_columns.append(qi(field.name))

        # Create a temporary view name
        temp_view_name = f"__visitran_interval_workaround_{table_name}_{uuid.uuid4().hex[:8]}"

        # Create temporary view with expiration
        create_view_sql = f"""
            CREATE OR REPLACE VIEW {qi(schema_name)}.{qi(temp_view_name)}
            OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR))
            AS SELECT {', '.join(select_columns)} FROM {qi(schema_name)}.{qi(table_name)}
        """

        self.connection.raw_sql(create_view_sql)

        # Return the view as a table object
        return self.connection.table(temp_view_name, database=schema_name)

    def merge_into_table(
        self,
        schema_name: str,
        target_table_name: str,
        select_statement: Table,
        primary_key: Union[str, list[str]] = None,
    ) -> dict:
        """Efficient upsert using DELETE + INSERT for BigQuery.
        Returns dict with rows_affected.
        """
        try:
            fire_event(
                MergeInToTable(
                    schema_name=schema_name,
                    table_name=target_table_name,
                    temp_table_name=f"{target_table_name}__temp",
                )
            )



            # 1. Create temporary table with incremental data (includes transformations)
            self.create_or_replace_table(
                schema_name=schema_name,
                table_name=f"{target_table_name}__temp",
                select_statement=select_statement,
                expiration_timestamp=SET_EXPIRATION_TIME,
            )

            # 2. Get target table columns
            target_columns = self.get_table_columns(schema_name=schema_name, table_name=target_table_name)

            if not target_columns:
                raise ValueError(f"No columns found in target table {schema_name}.{target_table_name}")

            qi = self.quote_identifier

            # 3. If primary key is provided, use efficient DELETE + INSERT
            if primary_key:
                # Handle both single column and composite keys
                if isinstance(primary_key, str):
                    key_columns = [primary_key]
                else:
                    key_columns = primary_key

                # Validate that primary key columns exist in target table
                missing_columns = [col for col in key_columns if col not in target_columns]
                if missing_columns:
                    raise ValueError(
                        f"Primary key columns {missing_columns} not found in target table {schema_name}.{target_table_name}. "
                        f"Available columns: {target_columns}"
                    )

                # Build the WHERE clause for composite keys
                where_conditions = [f"dest.{qi(k)} = source.{qi(k)}" for k in key_columns]
                where_clause = " AND ".join(where_conditions)

                temp_tbl = f"{qi(schema_name)}.{qi(target_table_name + '__temp')}"
                target_tbl = f"{qi(schema_name)}.{qi(target_table_name)}"

                delete_query = f"""
                    DELETE FROM {target_tbl} dest
                    WHERE EXISTS (
                        SELECT 1 FROM {temp_tbl} source
                        WHERE {where_clause}
                    )
                """

                col_list = ", ".join([qi(col) for col in target_columns])
                insert_query = f"""
                    INSERT INTO {target_tbl}
                    ({col_list})
                    SELECT {col_list}
                    FROM {temp_tbl}
                """

                self.bulk_execute_statements([delete_query, insert_query])

            else:
                target_tbl = f"{qi(schema_name)}.{qi(target_table_name)}"
                temp_tbl = f"{qi(schema_name)}.{qi(target_table_name + '__temp')}"
                col_list = ", ".join([qi(col) for col in target_columns])

                merge_query = f"""
                    merge into {target_tbl} as VISITRAN_INTERNAL_DEST
                    using (
                        select * from {temp_tbl}
                    ) as VISITRAN_INTERNAL_SOURCE
                    on (FALSE)
                    when not matched then insert
                    ({col_list})
                    values
                    ({col_list})
                """
                self.bulk_execute_statements([merge_query])

        except Exception as e:
            # Clean up temporary table on error
            try:
                qi = self.quote_identifier
                self.connection.raw_sql(f"DROP TABLE IF EXISTS {qi(schema_name)}.{qi(target_table_name + '__temp')}")
            except Exception:
                pass  # Ignore cleanup errors

            # Re-raise the original error with context
            raise Exception(
                f"BigQuery incremental upsert failed for {schema_name}.{target_table_name}: {str(e)}"
            ) from e
        return {"rows_affected": None}  # BigQuery: fallback to get_table_row_count in BaseModel





    def create_schema(self, schema_name: str) -> None:
        try:
            dataset_name = schema_name
            client = bigquery.Client(project=self.project_id, credentials=self.credentials)
            dataset_id = f"{self.project_id}.{dataset_name}"
            dataset = bigquery.Dataset(dataset_id)
            client.create_dataset(dataset, exists_ok=True)
        except Exception as e:
            if any(keyword in str(e).lower() for keyword in ["permission", "not authorized", "access denied"]):
                raise DatabasePermissionDeniedError(self.schema_name, str(e))
            elif "already exists" in str(e).lower():
                raise SchemaAlreadyExist(self.schema_name, str(e))
            else:
                raise SchemaCreationFailed(dataset_name, f"Failed to create dataset_id {dataset_id} in BigQuery {str(e)}")

    def _parse_url(self, url: str) -> None:
        """Parse BigQuery connection URL"""
        try:
            # Pattern: bigquery://project_id/dataset?params
            pattern = re.compile(r"bigquery://([^/]+)(?:/([^?]+))?(?:\?(.*))?")
            match = pattern.match(url)

            if not match:
                raise InvalidConnectionUrlException(error_code="Invalid BigQuery URL format")

            self.project_id = match.group(1)
            self.dataset = match.group(2)
            self.host = "bigquery.googleapis.com"
            # self.port = 443

            # Parse query parameters
            if match.group(3):
                params = parse_qs(match.group(3))
                if "key_path" in params:
                    self.key_path = params["key_path"][0]
                if "credentials" in params:
                    try:
                        # Decode base64 and parse JSON
                        decoded = base64.b64decode(params["credentials"][0]).decode("utf-8")
                        self.credentials_dict = decoded
                        self.credentials = service_account.Credentials.from_service_account_info(json.loads(decoded))
                    except (base64.binascii.Error, json.JSONDecodeError) as e:
                        raise InvalidConnectionUrlException(error_code=f"Invalid credential encoding: {str(e)}")

                if "location" in params:
                    self.location = params["location"][0]
                if "use_legacy_sql" in params:
                    self.use_legacy_sql = params["use_legacy_sql"][0].lower() == "true"
        except Exception as e:
            raise InvalidConnectionUrlException(error_code=f"Failed to parse BigQuery URL: {str(e)}")

    def _build_connection_dict(self):
        return {
            "project_id": self.project_id,
            "dataset_id": self.dataset_id,
            "credentials": self.credentials_dict,
            "connection_url": self.connection_url,
        }

    def get_connection_details(self):
        return self._redact_connection_details(self._build_connection_dict())

    def get_raw_connection_details(self):
        return self._build_connection_dict()

    def build_bigquery_url(self) -> str:
        """
        Constructs a BigQuery URL with Base64-encoded credentials.

        Returns:
            bigquery:// URL with encoded credentials
        """
        # 1. Encode credentials to Base64
        creds_base64 = base64.b64encode(json.dumps(self.credentials_dict).encode("utf-8")).decode("utf-8")

        # 2. Build base URL
        url = f"bigquery://{self.project_id}/{self.dataset}"

        # 3. Add credentials parameter
        params = [f"credentials={quote_plus(creds_base64)}"]  # URL-encode the Base64

        # 4. Add optional parameters
        if self.location:
            params.append(f"location={self.location}")
        if self.use_legacy_sql:
            params.append("use_legacy_sql=true")

        # 5. Combine all components
        return f"{url}?{'&'.join(params)}"

    def validate(self) -> None:
        """Validate required connection parameters and dataset existence."""
        # First, validate required fields are present
        required = {
            "project_id": self.project_id,
            "dataset_id": self.dataset_id,
            "credentials": self.credentials,
        }

        for field, value in required.items():
            if not value:
                raise ConnectionFieldMissingException(missing_fields=field)

        # Then, validate that the dataset exists in the project
        try:
            # Get the BigQuery client
            client = self.connection.client

            # Get the dataset reference
            dataset_ref = client.dataset(self.dataset_id, project=self.project_id)

            # Try to get the dataset - this will raise an exception if it doesn't exist
            client.get_dataset(dataset_ref)
        except Exception as e:
            # Dataset doesn't exist or access denied
            error_message = f"Dataset '{self.dataset_id}' not found in project '{self.project_id}' or access denied: {str(e)}"
            raise ConnectionFailedError(
                db_type="bigquery",
                error_message=error_message
            )
