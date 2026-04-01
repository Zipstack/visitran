import logging
from typing import Any

from visitran.adapters.model import BaseModel
from visitran.adapters.snowflake.connection import SnowflakeConnection
from visitran.templates.model import VisitranModel
from visitran.events.functions import fire_event
from visitran.events.types import (
    ExecuteEphemeral,
    ExecuteIncrementalCreate,
    ExecuteIncrementalUpdate,
    ExecuteTable,
    ExecuteView,
)


class SnowflakeModel(BaseModel):
    def __init__(self, db_connection: SnowflakeConnection, model: VisitranModel) -> None:
        super().__init__(db_connection, model)
        self._statements: list[Any] = []
        self._db_connection: SnowflakeConnection = db_connection

    @property
    def db_connection(self) -> SnowflakeConnection:
        return self._db_connection

    def execute_ephemeral(self) -> None:
        return

    def execute_table(self) -> None:
        self.db_connection.drop_table_if_exist(
            table_name=self.model.destination_table_name,
            schema_name=self.model.destination_schema_name,
        )
        self.db_connection.create_table(
            table_name=self.model.destination_table_name,
            table_statement=self.model.select_statement,
            schema_name=self.model.destination_schema_name,
        )
        table_obj = self.db_connection.get_table_obj(
            schema_name=self.model.destination_schema_name,
            table_name=self.model.destination_table_name,
        )
        self.model.destination_table_obj = table_obj

    def execute_view(self) -> None:
        self.db_connection.drop_view_if_exist(
            view_name=self.model.destination_table_name,
            schema_name=self.model.destination_schema_name,
        )
        self.db_connection.create_view(
            view_name=self.model.destination_table_name,
            table_statement=self.model.select_statement,
            schema_name=self.model.destination_schema_name,
        )
        table_obj = self.db_connection.get_table_obj(
            schema_name=self.model.destination_schema_name,
            table_name=self.model.destination_table_name,
        )
        self.model.destination_table_obj = table_obj

    def execute_incremental(self) -> None:
        """Executes an incremental materialization using Snowflake's MERGE INTO for upsert."""
        if self.model.destination_table_exists:
            # Incremental update path
            fire_event(
                ExecuteIncrementalUpdate(
                    self.model.destination_schema_name,
                    self.model.destination_table_name,
                )
            )
            
            # Get incremental data
            self.model.select_statement = self.model.select_if_incremental()
            
            # Check for schema changes first
            if self._has_schema_changed():
                logging.info(f"Schema change detected for {self.model.destination_schema_name}.{self.model.destination_table_name}, performing full refresh")
                self._full_refresh_table()
                return
            
            # Continue with incremental logic if no schema changes
            logging.info(f"No schema changes detected for {self.model.destination_schema_name}.{self.model.destination_table_name}, using incremental update")
            
            # Get primary key for upsert
            primary_key = getattr(self.model, 'primary_key', None)
            
            if primary_key:
                # MERGE mode: Upsert with primary key (updates existing, inserts new)
                logging.info(f"Incremental MERGE mode: upserting with primary_key={primary_key}")
                self.db_connection.upsert_into_table(
                    schema_name=self.model.destination_schema_name,
                    table_name=self.model.destination_table_name,
                    select_statement=self.model.select_statement,
                    primary_key=primary_key,
                )
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
            fire_event(
                ExecuteIncrementalCreate(
                    self.model.destination_schema_name,
                    self.model.destination_table_name,
                )
            )
            
            # Get all data for first run
            self.model.select_statement = self.model.select()
            
            # Create table with all data
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
            logging.info(f"Starting full refresh for {self.model.destination_schema_name}.{self.model.destination_table_name}")
            
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
            
            logging.info(f"Full refresh completed for {self.model.destination_schema_name}.{self.model.destination_table_name}")
            
        except Exception as e:
            logging.error(f"Full refresh failed for {self.model.destination_schema_name}.{self.model.destination_table_name}: {str(e)}")
            raise Exception(
                f"Snowflake full refresh failed for {self.model.destination_schema_name}.{self.model.destination_table_name}: {str(e)}"
            ) from e
