from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from visitran.adapters.model import BaseModel
from visitran.events.functions import fire_event
from visitran.events.types import (
    ExecuteEphemeral,
    ExecuteIncrementalCreate,
    ExecuteIncrementalUpdate,
    ExecuteTable,
    ExecuteView,
)

if TYPE_CHECKING:  # pragma: no cover
    from visitran.adapters.bigquery.connection import BigQueryConnection
    from visitran.templates.model import VisitranModel


class BigQueryModel(BaseModel):
    """A class representing a BigQuery model."""

    def __init__(self, db_connection: BigQueryConnection, model: VisitranModel) -> None:
        super().__init__(db_connection, model)
        self._statements: list[Any] = []
        self._db_connection: BigQueryConnection = db_connection

    @property
    def db_connection(self) -> BigQueryConnection:
        """Returns the BigQuery connection."""
        return self._db_connection

    def execute_ephemeral(self) -> None:
        """Executes an ephemeral query."""
        fire_event(ExecuteEphemeral(self.model.destination_schema_name, self.model.destination_table_name))

    def execute_table(self) -> None:
        """Executes a query and creates or replaces a table."""
        fire_event(ExecuteTable(self.model.destination_schema_name, self.model.destination_table_name))
        self.db_connection.create_or_replace_table(
            schema_name=self.model.destination_schema_name,
            table_name=self.model.destination_table_name,
            select_statement=self.model.select_statement,
        )

        table_obj = self.db_connection.get_table_obj(
            schema_name=self.model.destination_schema_name,
            table_name=self.model.destination_table_name,
        )
        self.model.destination_table_obj = table_obj

    def execute_view(self) -> None:
        """Executes a query and creates or replaces a view."""
        fire_event(ExecuteView(self.model.destination_schema_name, self.model.destination_table_name))
        self.db_connection.create_or_replace_view(
            schema_name=self.model.destination_schema_name,
            table_name=self.model.destination_table_name,
            select_statement=self.model.select_statement,
        )

        table_obj = self.db_connection.get_table_obj(
            schema_name=self.model.destination_schema_name,
            table_name=self.model.destination_table_name,
        )
        self.model.destination_table_obj = table_obj

    def execute_incremental(self) -> None:
        """Executes an incremental materialization."""
        if self.model.destination_table_exists:
            # then call select_if_incremental
            # insert the results into table
            fire_event(
                ExecuteIncrementalUpdate(
                    self.model.destination_schema_name,
                    self.model.destination_table_name,
                )
            )

            # Check for schema changes first
            if self._has_schema_changed():
                logging.info(
                    f"Schema change detected for {self.model.destination_schema_name}.{self.model.destination_table_name}, performing full refresh"
                )
                self._full_refresh_table()

            else:
                # Continue with incremental logic if no schema changes
                self.model.select_statement = self.model.select_if_incremental()

                logging.info(
                    f"No schema changes detected for {self.model.destination_schema_name}.{self.model.destination_table_name}, using incremental update"
                )

                # Get primary key from model if available
                primary_key = getattr(self.model, "primary_key", None)

                self.db_connection.merge_into_table(
                    schema_name=self.model.destination_schema_name,
                    target_table_name=self.model.destination_table_name,
                    select_statement=self.model.select_statement,
                    primary_key=primary_key,
                )
        else:
            fire_event(
                ExecuteIncrementalCreate(
                    self.model.destination_schema_name,
                    self.model.destination_table_name,
                )
            )
            self.db_connection.create_or_replace_table(
                schema_name=self.model.destination_schema_name,
                table_name=self.model.destination_table_name,
                select_statement=self.model.select_statement,
            )

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

            # Use BigQuery's create_or_replace_table which handles full refresh
            self.db_connection.create_or_replace_table(
                schema_name=self.model.destination_schema_name,
                table_name=self.model.destination_table_name,
                select_statement=self.model.select_statement,
            )

            logging.info(
                f"Full refresh completed for {self.model.destination_schema_name}.{self.model.destination_table_name}"
            )

        except Exception as e:
            logging.error(
                f"Full refresh failed for {self.model.destination_schema_name}.{self.model.destination_table_name}: {str(e)}"
            )
            raise Exception(
                f"BigQuery full refresh failed for {self.model.destination_schema_name}.{self.model.destination_table_name}: {str(e)}"
            ) from e
