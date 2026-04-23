import logging
from typing import Any

from visitran.adapters.databricks.connection import DatabricksConnection
from visitran.adapters.model import BaseModel
from visitran.templates.model import VisitranModel


class DatabricksModel(BaseModel):
    """Databricks model implementation with Delta Lake support."""

    def __init__(self, db_connection: DatabricksConnection, model: VisitranModel) -> None:
        super().__init__(db_connection, model)
        self._statements: list[Any] = []
        self._db_connection: DatabricksConnection = db_connection

    @property
    def db_connection(self) -> DatabricksConnection:
        return self._db_connection

    def execute_ephemeral(self) -> None:
        """Ephemeral materialization - no physical table created."""
        return

    def execute_table(self) -> None:
        """Table materialization - creates Delta Lake table."""
        # Drop existing table
        self.db_connection.drop_table_if_exist(
            schema_name=self.model.destination_schema_name,
            table_name=self.model.destination_table_name,
        )

        # Create new table (Delta Lake format by default)
        self.db_connection.create_table(
            self.model.destination_schema_name,
            self.model.destination_table_name,
            self.model.select_statement,
        )

        # Get the created table object
        table_obj = self.db_connection.get_table_obj(
            schema_name=self.model.destination_schema_name,
            table_name=self.model.destination_table_name,
        )
        self.model.destination_table_obj = table_obj

    def execute_view(self) -> None:
        """View materialization - creates database view."""
        # Drop existing view
        self.db_connection.drop_view_if_exist(
            view_name=self.model.destination_table_name,
            schema_name=self.model.destination_schema_name,
        )

        # Create new view
        self.db_connection.create_view(
            view_name=self.model.destination_table_name,
            table_statement=self.model.select_statement,
            schema_name=self.model.destination_schema_name,
        )

        # Get the created view object
        table_obj = self.db_connection.get_table_obj(
            schema_name=self.model.destination_schema_name,
            table_name=self.model.destination_table_name,
        )
        self.model.destination_table_obj = table_obj

    def execute_incremental(self) -> None:
        """Incremental materialization using Delta Lake MERGE for upsert.

        Supports two modes:
        - MERGE mode (with primary_key): Updates existing records, inserts new ones
        - APPEND mode (without primary_key): Insert-only, no deduplication
        """
        if self.model.destination_table_exists:
            # Table exists - incremental update
            logging.info(
                f"Incremental update for {self.model.destination_schema_name}.{self.model.destination_table_name}"
            )

            # Get incremental data using delta strategy
            self.model.select_statement = self.model.select_if_incremental()

            # Check for schema changes first
            if self._has_schema_changed():
                logging.info(
                    f"Schema change detected for {self.model.destination_schema_name}.{self.model.destination_table_name}, performing full refresh"
                )
                self._full_refresh_table()
                return

            # Get primary key for upsert
            primary_key = getattr(self.model, "primary_key", None)

            if primary_key:
                # MERGE mode: Upsert with primary key (updates existing, inserts new)
                logging.info(f"Incremental MERGE mode: upserting with primary_key={primary_key}")
                result = self.db_connection.upsert_into_table(
                    schema_name=self.model.destination_schema_name,
                    table_name=self.model.destination_table_name,
                    select_statement=self.model.select_statement,
                    primary_key=primary_key,
                )
                if result and isinstance(result, dict):
                    self._upsert_metrics = result
            else:
                # APPEND mode: Insert-only, no deduplication (for event logs, time-series)
                logging.info("Incremental APPEND mode: inserting without deduplication")
                self.db_connection.insert_into_table(
                    schema_name=self.model.destination_schema_name,
                    table_name=self.model.destination_table_name,
                    table_statement=self.model.select_statement,
                )
        else:
            # First run - create table with all data
            logging.info(
                f"First run: creating table {self.model.destination_schema_name}.{self.model.destination_table_name}"
            )

            # Get all data for first run
            self.model.select_statement = self.model.select()

            # Drop if exists and create new table
            self.db_connection.drop_table_if_exist(
                table_name=self.model.destination_table_name,
                schema_name=self.model.destination_schema_name,
            )
            self.db_connection.create_table(
                table_name=self.model.destination_table_name,
                table_statement=self.model.select_statement,
                schema_name=self.model.destination_schema_name,
            )

        # Update table object reference
        table_obj = self.db_connection.get_table_obj(
            schema_name=self.model.destination_schema_name,
            table_name=self.model.destination_table_name,
        )
        self.model.destination_table_obj = table_obj

    def _full_refresh_table(self) -> None:
        """Perform full refresh using existing table transformation methods."""
        try:
            logging.info(
                f"Starting full refresh for {self.model.destination_schema_name}.{self.model.destination_table_name}"
            )

            # Drop existing table
            self.db_connection.drop_table_if_exist(
                table_name=self.model.destination_table_name,
                schema_name=self.model.destination_schema_name,
            )

            # Create new table with current transformation logic
            self.db_connection.create_table(
                table_name=self.model.destination_table_name,
                table_statement=self.model.select_statement,
                schema_name=self.model.destination_schema_name,
            )

            logging.info(
                f"Full refresh completed for {self.model.destination_schema_name}.{self.model.destination_table_name}"
            )

        except Exception as e:
            logging.error(
                f"Full refresh failed for {self.model.destination_schema_name}.{self.model.destination_table_name}: {str(e)}"
            )
            raise Exception(
                f"Databricks full refresh failed for {self.model.destination_schema_name}.{self.model.destination_table_name}: {str(e)}"
            ) from e
