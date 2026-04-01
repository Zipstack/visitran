from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

from visitran.adapters.connection import BaseConnection
from visitran.materialization import Materialization
from visitran.templates.model import VisitranModel


class BaseModel(ABC):
    def __init__(self, db_connection: BaseConnection, model: VisitranModel) -> None:
        super().__init__()

        self._db_connection: BaseConnection = db_connection
        self._model: VisitranModel = model

        self._statements: list[Any] = []

    @property
    def model(self) -> VisitranModel:
        return self._model

    @property
    def materialization(self) -> Materialization:
        return self.model.materialization

    def execute(self) -> None:
        if self.materialization == Materialization.EPHEMERAL:
            self.execute_ephemeral()

        self.model.select_statement = self.model.select()

        if self.materialization == Materialization.TABLE:
            self.execute_table()

        elif self.materialization == Materialization.VIEW:
            self.execute_view()

        elif self.materialization == Materialization.INCREMENTAL:
            self.execute_incremental()

    @abstractmethod
    def execute_ephemeral(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def execute_table(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def execute_view(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def execute_incremental(self) -> None:
        raise NotImplementedError

    def _has_schema_changed(self) -> bool:
        """Detect if schema has changed significantly.

        This method compares the current table columns with the new SELECT statement
        columns to determine if a full refresh is needed due to schema changes.

        Returns:
            True if schema has changed significantly, False otherwise
        """
        try:
            # Get current table columns
            current_columns = set(
                self.db_connection.get_table_columns(
                    schema_name=self.model.destination_schema_name, table_name=self.model.destination_table_name
                )
            )

            # Get new columns from SELECT statement
            new_columns = set(self.model.select_statement.columns)

            # Check for changes
            added_columns = new_columns - current_columns
            removed_columns = current_columns - new_columns

            # Log schema change details
            if added_columns or removed_columns:
                logging.info(
                    f"Schema change detected for {self.model.destination_schema_name}.{self.model.destination_table_name}"
                )
                if added_columns:
                    logging.info(f"  Added columns: {list(added_columns)}")
                if removed_columns:
                    logging.info(f"  Removed columns: {list(removed_columns)}")
                return True

            return False

        except Exception as e:
            # If we can't determine schema, assume it changed (safe default)
            logging.warning(
                f"Could not determine schema for {self.model.destination_schema_name}.{self.model.destination_table_name}: {str(e)}"
            )
            return True
