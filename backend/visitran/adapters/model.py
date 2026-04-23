from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional

from visitran.adapters.connection import BaseConnection
from visitran.materialization import Materialization
from visitran.templates.model import VisitranModel


@dataclass
class ExecutionMetrics:
    """Metrics returned from model execution."""
    rows_affected: Optional[int] = None
    rows_inserted: Optional[int] = None
    rows_updated: Optional[int] = None
    rows_deleted: Optional[int] = None
    materialization: str = ""


class BaseModel(ABC):
    def __init__(self, db_connection: BaseConnection, model: VisitranModel) -> None:
        super().__init__()

        self._db_connection: BaseConnection = db_connection
        self._model: VisitranModel = model

        self._statements: list[Any] = []
        self._upsert_metrics: Optional[dict] = None  # Populated by adapter's execute_incremental

    @property
    def model(self) -> VisitranModel:
        return self._model

    @property
    def materialization(self) -> Materialization:
        return self.model.materialization

    def execute(self) -> ExecutionMetrics:
        mat_name = self.materialization.value if hasattr(self.materialization, "value") else str(self.materialization)

        if self.materialization == Materialization.EPHEMERAL:
            self.execute_ephemeral()
            return ExecutionMetrics(rows_affected=None, materialization="ephemeral")

        self.model.select_statement = self.model.select()

        if self.materialization == Materialization.TABLE:
            self.execute_table()
            # Get row count after table creation — all rows are "inserted" (DROP + CREATE)
            rows = self._get_row_count_safe()
            return ExecutionMetrics(
                rows_affected=rows,
                rows_inserted=rows,
                rows_updated=0,
                rows_deleted=0,
                materialization="table",
            )

        elif self.materialization == Materialization.VIEW:
            self.execute_view()
            return ExecutionMetrics(rows_affected=None, materialization="view")

        elif self.materialization == Materialization.INCREMENTAL:
            self.execute_incremental()
            rows = self._get_row_count_safe()
            # Use upsert metrics if available (adapter captured cursor.rowcount)
            upsert = self._upsert_metrics or {}
            return ExecutionMetrics(
                rows_affected=rows,
                rows_inserted=upsert.get("rows_inserted"),
                rows_updated=upsert.get("rows_updated"),
                rows_deleted=upsert.get("rows_deleted"),
                materialization="incremental",
            )

        return ExecutionMetrics(materialization=mat_name)

    def _get_row_count_safe(self) -> Optional[int]:
        """Get row count after execution, return None on failure."""
        try:
            return self._db_connection.get_table_row_count(
                schema_name=self.model.destination_schema_name,
                table_name=self.model.destination_table_name,
            )
        except Exception as e:
            logging.debug(f"Could not get row count for {self.model.destination_table_name}: {e}")
            return None

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
            current_columns = set(self.db_connection.get_table_columns(
                schema_name=self.model.destination_schema_name,
                table_name=self.model.destination_table_name
            ))

            # Get new columns from SELECT statement
            new_columns = set(self.model.select_statement.columns)

            # Check for changes
            added_columns = new_columns - current_columns
            removed_columns = current_columns - new_columns

            # Log schema change details
            if added_columns or removed_columns:
                logging.info(f"Schema change detected for {self.model.destination_schema_name}.{self.model.destination_table_name}")
                if added_columns:
                    logging.info(f"  Added columns: {list(added_columns)}")
                if removed_columns:
                    logging.info(f"  Removed columns: {list(removed_columns)}")
                return True

            return False

        except Exception as e:
            # If we can't determine schema, assume it changed (safe default)
            logging.warning(f"Could not determine schema for {self.model.destination_schema_name}.{self.model.destination_table_name}: {str(e)}")
            return True
