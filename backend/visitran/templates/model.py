from __future__ import annotations

import warnings
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Union

import ibis
from ibis.expr.types.relations import Table
from sqlalchemy import exc

from visitran.adapters.connection import BaseConnection
from visitran.materialization import Materialization
from visitran.singleton import Singleton
from visitran.templates.delta_strategies import DeltaStrategyFactory

if TYPE_CHECKING:  # pragma: no cover
    from visitran.adapters.connection import BaseConnection
    from visitran.visitran import Visitran
    from visitran.visitran_context import VisitranContext


class VisitranModel(metaclass=Singleton):
    """The Singleton base class from which source models should inherit."""

    # Class-level db_connection for lazy initialization of source_table_obj
    _shared_db_connection: BaseConnection | None = None

    def __init__(self) -> None:
        super().__init__()
        self.dbtype: str = ""
        self.visitran_obj: Visitran | None = None

        self.database: str = ""
        self.materialization: Materialization = Materialization.VIEW
        self.select_statement: Table | None = None

        # below 2 variables is must for
        # class directly inherited from VisitranModel
        # not needed for others
        self.source_table_name: str = ""
        self.source_schema_name: str = ""

        # this is always required
        self.destination_table_name: str = ""
        self.destination_schema_name: str = ""

        # source_table_obj is populated for models starting from
        # VisitranModel, Incremental model
        self._source_table_obj: Table | None = None
        # destination_table_obj is populated for models
        # other than Ephemeral
        self._destination_table_obj: Table | None = None

        # this wil be always populated for
        # class with incremental materialization
        # this is get only
        self.destination_table_exists: bool = False

        # Primary key for efficient upserts (especially for BigQuery)
        # This should be set to the column name(s) that uniquely identify records
        # Can be a single column name (str) or list of column names for composite keys
        self.primary_key: Union[str, list[str]] = ""

        # Delta detection strategy for incremental processing
        # This defines how to identify new/changed records for incremental updates
        self.delta_strategy: dict[str, Any] = {
            "type": "",  # "timestamp", "date", "sequence", "checksum", "full_scan", "custom"
            "column": "",  # Column name for timestamp/date/sequence strategies
            "custom_logic": None,  # Custom function for "custom" strategy
        }

    @property
    def source_table_obj(self) -> Table | None:
        """Lazy initialization of source_table_obj using shared
        db_connection."""
        if self._source_table_obj is None and VisitranModel._shared_db_connection is not None:
            if self.source_schema_name and self.source_table_name:
                try:
                    self._source_table_obj = VisitranModel._shared_db_connection.get_table_obj(
                        self.source_schema_name, self.source_table_name
                    )
                except Exception:
                    pass  # Table may not exist yet, will be set during materialize()
        return self._source_table_obj

    @source_table_obj.setter
    def source_table_obj(self, value: Table | None) -> None:
        self._source_table_obj = value

    @property
    def visitran(self) -> Visitran:
        return self.visitran_obj

    @visitran.setter
    def visitran(self, visitran_obj: Visitran) -> None:
        self.visitran_obj = visitran_obj
        self.dbtype = visitran_obj.dbtype

    @property
    def visitran_context(self) -> VisitranContext:
        return self.visitran_obj.context

    @abstractmethod
    def select(self) -> Table:
        """A select method should be implemented by model."""
        pass

    def select_if_incremental(self) -> Table:
        """A select method which will called only while doing incremental
        update."""
        raise NotImplementedError

    def _validate_incremental_config(self) -> None:
        """Validate that incremental models have required configuration.

        Incremental models support two modes:
        - MERGE mode (with primary_key): Updates existing records, inserts new ones
        - APPEND mode (without primary_key): Insert-only, no deduplication
        """
        if self.materialization == Materialization.INCREMENTAL:
            # Primary key is optional - determines MERGE vs APPEND mode
            # If no primary_key: APPEND mode (insert-only)
            # If primary_key set: MERGE mode (upsert)

            # Check delta strategy is configured (required for both modes)
            if not self.delta_strategy.get("type"):
                raise ValueError(
                    f"Delta strategy is required for incremental models. "
                    f"Set self.delta_strategy in {self.__class__.__name__}.__init__()\n"
                    f"Example: self.delta_strategy = create_timestamp_strategy(column='updated_at')"
                )

            # Validate delta strategy configuration
            self._validate_delta_strategy_config()

    @property
    def incremental_mode(self) -> str:
        """Return the incremental mode based on primary_key presence.

        Returns:
            'merge' if primary_key is set (UPSERT behavior)
            'append' if primary_key is not set (INSERT-only behavior)
        """
        if self.primary_key:
            return "merge"
        return "append"

    def _validate_delta_strategy_config(self) -> None:
        """Validate delta strategy configuration."""
        strategy_type = self.delta_strategy.get("type")

        if strategy_type == "timestamp":
            if not self.delta_strategy.get("column"):
                raise ValueError(
                    f"Timestamp strategy requires 'column' configuration. "
                    f"Example: create_timestamp_strategy(column='updated_at')"
                )

        elif strategy_type == "date":
            if not self.delta_strategy.get("column"):
                raise ValueError(
                    f"Date strategy requires 'column' configuration. "
                    f"Example: create_date_strategy(column='created_date')"
                )

        elif strategy_type == "sequence":
            if not self.delta_strategy.get("column"):
                raise ValueError(
                    f"Sequence strategy requires 'column' configuration. "
                    f"Example: create_sequence_strategy(column='id')"
                )

        elif strategy_type == "checksum":
            if not self.delta_strategy.get("column"):
                raise ValueError(
                    f"Checksum strategy requires 'column' configuration. "
                    f"Example: create_checksum_strategy(checksum_column='content_hash', key_columns=['product_id'])"
                )
            if not self.delta_strategy.get("key_columns"):
                raise ValueError(
                    f"Checksum strategy requires 'key_columns' configuration. "
                    f"Example: create_checksum_strategy(checksum_column='content_hash', key_columns=['product_id'])"
                )

        elif strategy_type == "custom":
            if not self.delta_strategy.get("custom_logic"):
                raise ValueError(
                    f"Custom strategy requires 'custom_logic' function. "
                    f"Example: create_custom_strategy(custom_logic=self._my_logic)"
                )
            if not callable(self.delta_strategy.get("custom_logic")):
                raise ValueError(f"Custom strategy 'custom_logic' must be a callable function.")

        elif strategy_type == "full_scan":
            # No additional validation needed for full scan
            pass

        else:
            raise ValueError(
                f"Unknown delta strategy type: {strategy_type}. "
                f"Available strategies: {DeltaStrategyFactory.get_available_strategies()}"
            )

    def _execute_delta_strategy(self) -> Table:
        """Execute the configured delta strategy to get incremental data."""
        if not self.destination_table_exists:
            # First run - return all data
            return self.select()

        # Get the delta strategy
        strategy_type = self.delta_strategy["type"]
        strategy = DeltaStrategyFactory.get_strategy(strategy_type)

        # Execute the strategy
        source_table = self.select()
        destination_table = self.destination_table_obj

        # Get incremental data from strategy
        incremental_data = strategy.get_incremental_data(
            source_table=source_table, destination_table=destination_table, strategy_config=self.delta_strategy
        )

        # Return incremental data as-is (no additional transformation needed)
        return incremental_data

    def __str__(self) -> str:
        return f"{self.destination_schema_name}.{self.destination_table_name}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}"

    @property
    def destination_table_obj(self) -> Table:
        return self._destination_table_obj

    @destination_table_obj.setter
    def destination_table_obj(self, destination_table_obj: Table) -> None:
        """Initializes table_obj."""
        self._destination_table_obj = destination_table_obj

    def materialize(self, parent_class: object, db_connection: BaseConnection) -> None:
        """Initializes source_table_obj for models with immediate parent
        VisitranModel and materialization is incremental."""
        # Store db_connection at class level for lazy initialization of source_table_obj
        VisitranModel._shared_db_connection = db_connection

        if self.materialization == Materialization.INCREMENTAL:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=DeprecationWarning)
                from google.api_core.exceptions import NotFound

                try:
                    self.source_table_obj = db_connection.get_table_obj(self.source_schema_name, self.source_table_name)
                except (exc.NoSuchTableError, exc.ProgrammingError, NotFound):
                    pass
                try:
                    self.destination_table_obj: Table = db_connection.get_table_obj(
                        self.destination_schema_name, self.destination_table_name
                    )
                    self.destination_table_exists = True
                except (exc.NoSuchTableError, exc.ProgrammingError, NotFound):
                    self.destination_table_exists = False
        elif self.materialization == Materialization.EPHEMERAL or (parent_class is VisitranModel):
            self.source_table_obj = db_connection.get_table_obj(self.source_schema_name, self.source_table_name)
            self.select_statement = self.select()
        else:
            # TABLE/VIEW models that extend another model (not VisitranModel)
            # The source table should exist in the database from the parent model's execution
            # This is needed for EPHEMERAL source classes to access their source_table_obj
            self.source_table_obj = db_connection.get_table_obj(self.source_schema_name, self.source_table_name)

            # Traverse the entire inheritance chain and ensure ALL ancestor class Singletons
            # have their source_table_obj initialized. This is critical for when select()
            # calls ParentClass().select() - those Singletons need their source_table_obj set.
            self._initialize_ancestor_singletons(db_connection)

    def _initialize_ancestor_singletons(self, db_connection: BaseConnection) -> None:
        """Initialize source_table_obj for all ancestor class Singletons in the
        MRO.

        This ensures that when select() calls ParentClass().select(),
        the parent Singleton has its source_table_obj properly
        initialized, regardless of whether it was discovered and
        executed as a separate DAG node.
        """
        for ancestor_class in self.__class__.__mro__:
            if not self._is_valid_ancestor(ancestor_class):
                continue
            self._initialize_ancestor_source_table(ancestor_class, db_connection)

    def _is_valid_ancestor(self, ancestor_class: type) -> bool:
        """Check if an ancestor class should have its source_table_obj
        initialized."""
        if ancestor_class is VisitranModel or ancestor_class is self.__class__:
            return False
        if not isinstance(ancestor_class, type) or ancestor_class.__name__ == "object":
            return False
        try:
            return issubclass(ancestor_class, VisitranModel)
        except TypeError:
            return False

    def _initialize_ancestor_source_table(self, ancestor_class: type, db_connection: BaseConnection) -> None:
        """Initialize source_table_obj for a single ancestor class
        Singleton."""
        try:
            ancestor_instance = ancestor_class()
            if ancestor_instance.source_table_obj is not None:
                return
            if not (ancestor_instance.source_schema_name and ancestor_instance.source_table_name):
                return
            ancestor_instance.source_table_obj = db_connection.get_table_obj(
                ancestor_instance.source_schema_name, ancestor_instance.source_table_name
            )
        except Exception:
            pass  # Skip ancestors that can't be instantiated or tables that don't exist

    def save_table_columns(self, transformation_id: str, table_obj: Table) -> None:
        model_name: str = (str(self.__class__.__module__)).split(".")[-1]
        self.visitran_context.store_table_columns(
            transformation_id=transformation_id, model_name=model_name, table_obj=table_obj
        )

    def save_sql_query(self, sql_query: str):
        model_name: str = (str(self.__class__.__module__)).split(".")[-1]
        self.visitran_context.store_sql_data(model_name=model_name, sql_query=sql_query)

    @staticmethod
    def prepare_child_table(child_obj: Table, parent_obj: Table, mappings: dict):
        """This method is used by Unions transformations to add the existing
        columns from the parent table.

        :param child_obj:
        :param parent_obj:
        :param mappings:
        :return:
        """
        projections = []
        for parent_col in parent_obj.columns:
            parent_type = parent_obj[parent_col].type()
            if parent_col in mappings:
                # Get the mapped child column
                child_col = mappings[parent_col]
                projections.append(child_obj[child_col].cast(parent_type).name(parent_col))
            else:
                projections.append(ibis.literal(None, type=f"{parent_type}").name(parent_col))
        return child_obj.select(projections)
