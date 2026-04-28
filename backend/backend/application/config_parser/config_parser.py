from typing import Any, Optional

from backend.application.config_parser.base_parser import BaseParser
from backend.application.config_parser.presentation_parser import PresentationParser
from backend.application.config_parser.transformation_parser import TransformationParser
from backend.application.config_parser.yaml_source_tracker import (
    YAMLSourceTracker,
    SourceLocation,
)
from backend.application.config_parser.sql_builder import SQLQueryBuilder
from backend.errors import InvalidSourceTable, InvalidDestinationTable, InvalidMaterialization


class ConfigParser(BaseParser):
    """
    Configuration parser for model YAML files.

    This parser handles model configuration data and provides source location
    tracking for transformations to enable precise error reporting.

    Attributes:
        _instances: Class-level cache of ConfigParser instances by file name
        _source_map: Mapping of transformation IDs to (line, column) tuples
    """

    _instances: dict[str, "ConfigParser"] = {}

    def __new__(cls, model_data: dict[str, Any], file_name: str, *args, **kwargs) -> "ConfigParser":
        """
        Overrides the __new__ method to implement a singleton pattern
        based on the file_name parameter.

        Args:
            model_data (dict[str, Any]): Configuration data for the model.
            file_name (str): The name of the configuration file.

        Returns:
            ConfigParser: A singleton instance of the class for the given file name.
        """
        if file_name in cls._instances:
            return cls._instances[file_name]
        instance = super().__new__(cls)
        cls._instances[file_name] = instance
        return instance

    def __init__(self, model_data: dict[str, Any], file_name: str) -> None:
        self._model_name: str = file_name
        self._no_code_conf: dict[str, Any] = model_data
        self._presentation_parser: Any = None
        self._transformation_parser: Any = None
        self._reference: list[str] = []
        self._all_reference: dict[str, Any] = {}
        # Source location tracking
        self._source_map: dict[str, tuple[int, int]] = {}
        self._yaml_content: Optional[str] = None
        # Join table column metadata (for column aliasing like legacy rname pattern)
        # Format: {"{schema}.{table}": ["col1", "col2", ...]}
        self._join_table_columns: dict[str, list[str]] = {}
        # Source table columns (for find_and_replace and other column-aware operations)
        self._source_table_columns: list[str] = []
        # Database dialect for SQL generation (e.g., 'postgres', 'bigquery', 'duckdb')
        self._dialect: Optional[str] = None
        super().__init__(self._no_code_conf)

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def source_schema_name(self) -> str:
        if self.get("source", {}).get("schema_name", "") == "~":
            # Special case where "~" indicates no schema.
            return ""
        return self.get("source", {}).get("schema_name", "default")  # Return the schema name or "default".

    @property
    def source_table_name(self) -> str:
        source_table_name = self.get("source", {}).get("table_name", "")
        if not source_table_name:
            raise InvalidSourceTable(table_name=source_table_name)
        return source_table_name

    @property
    def destination_schema_name(self) -> str:
        if self.get("model", {}).get("schema_name", "") == "~":
            return ""
        return self.get("model", {}).get("schema_name", "default")

    @property
    def destination_table_name(self) -> str:
        destination_table_name = self.get("model", {}).get("table_name", "")
        if not destination_table_name:
            raise InvalidDestinationTable(table_name=destination_table_name)
        return destination_table_name

    @property
    def materialization(self) -> str:
        materialization = self.get("source", {}).get("materialization", "TABLE")
        if materialization not in ["TABLE", "VIEW", "EPHEMERAL", "INCREMENTAL"]:
            # Raise an exception if the materialization type is unsupported.
            raise InvalidMaterialization(
                materialization=materialization,
                supported_materializations=["TABLE", "VIEW", "EPHEMERAL", "INCREMENTAL"],
            )
        return self.get("source", {}).get("materialization", "TABLE")

    @property
    def incremental_config(self) -> dict[str, Any]:
        if self.materialization == "TABLE":
            return {}
        return self.get("source", {}).get("incremental_config", {})

    @property
    def unique_keys(self) -> list[str]:
        return self.incremental_config.get('primary_key', [])

    @property
    def delta_strategy(self) -> dict[str, Any]:
        return self.incremental_config.get("delta_strategy", {})

    @property
    def reference(self) -> list[str]:
        if not self._reference:
            self._reference = self.get("reference") or []
        return self._reference

    @property
    def source_model(self) -> str | None:
        """
        Returns the model name that produces this model's source table, if any.

        This is set by validate_table_usage_references() when the source table
        matches another model's destination. It explicitly tracks which model
        should be the parent class (for inheritance and data flow).

        Returns:
            str: Model name if source table is produced by another model
            None: If source table is a raw database table
        """
        return self.get("source_model")

    @property
    def all_reference(self) -> dict[str, Any]:
        return self._all_reference

    @all_reference.setter
    def all_reference(self, value: dict[str, Any]) -> None:
        if not isinstance(value, dict):
            raise TypeError("all_reference must be a dictionary, Passed a list instead.")
        self._all_reference = value

    @property
    def presentation_parser(self) -> PresentationParser:
        if not self._presentation_parser:
            self._presentation_parser: PresentationParser = PresentationParser(config_data=self.get("presentation", {}))
        return self._presentation_parser

    @property
    def transform_parser(self) -> TransformationParser:
        if not self._transformation_parser:
            self._transformation_parser: TransformationParser = TransformationParser(
                config_data={
                    "transform": self.get("transform", []),
                    "transform_order": self.get("transform_order", [])
                }
            )
        return self._transformation_parser

    # =========================================================================
    # Source Location Tracking Methods
    # =========================================================================

    def set_yaml_content(self, yaml_content: str) -> None:
        """
        Set the original YAML content for source location tracking.

        This method parses the YAML content and extracts source locations
        for all transformations. Should be called when loading from a file.

        Args:
            yaml_content: The raw YAML string from the configuration file
        """
        self._yaml_content = yaml_content
        tracker = YAMLSourceTracker()
        _, source_locations = tracker.load_with_locations(yaml_content)

        # Convert SourceLocation objects to simple tuples
        self._source_map = {
            key: loc.as_tuple() for key, loc in source_locations.items()
        }

    def set_source_map(self, source_map: dict[str, tuple[int, int]]) -> None:
        """
        Directly set the source location map.

        Use this when source locations have been parsed externally.

        Args:
            source_map: Dictionary mapping transformation IDs to (line, column) tuples
        """
        self._source_map = source_map.copy()

    def get_source_location(self, transformation_id: str) -> Optional[tuple[int, int]]:
        """
        Get the source location for a transformation.

        This method is used by error reporting components to provide
        contextual information when validation failures or execution
        errors occur.

        Args:
            transformation_id: The identifier of the transformation

        Returns:
            Tuple of (line_number, column_number) if found, None otherwise.
            Line and column numbers are 1-based.
        """
        return self._source_map.get(transformation_id)

    def set_join_table_columns(self, schema: str, table: str, columns: list[str]) -> None:
        """
        Set column metadata for a joined table.

        This enables legacy-compatible column aliasing (rname pattern) where
        joined table columns are prefixed with table name to avoid conflicts.

        Args:
            schema: The schema name of the joined table
            table: The table name of the joined table
            columns: List of column names in the table
        """
        key = f"{schema}.{table}" if schema else table
        self._join_table_columns[key] = columns

    def get_join_table_columns(self, schema: str, table: str) -> list[str]:
        """
        Get column metadata for a joined table.

        Args:
            schema: The schema name of the joined table
            table: The table name of the joined table

        Returns:
            List of column names if set, empty list otherwise
        """
        key = f"{schema}.{table}" if schema else table
        return self._join_table_columns.get(key, [])

    def set_source_table_columns(self, columns: list[str]) -> None:
        """
        Set column metadata for the source table.

        This enables PostgreSQL-compatible SQL generation for transformations
        like find_and_replace that need to replace columns in-place.

        Args:
            columns: List of column names in the source table
        """
        self._source_table_columns = columns

    def get_source_table_columns(self) -> list[str]:
        """
        Get column metadata for the source table.

        Returns:
            List of column names if set, empty list otherwise
        """
        return self._source_table_columns

    def set_dialect(self, dialect: str) -> None:
        """
        Set the database dialect for SQL generation.

        This determines database-specific SQL syntax (e.g., identifier quoting,
        SELECT * REPLACE support).

        Args:
            dialect: Database type ('postgres', 'bigquery', 'duckdb', 'snowflake', 'databricks', 'trino')
        """
        self._dialect = dialect

    def get_dialect(self) -> Optional[str]:
        """
        Get the database dialect for SQL generation.

        Returns:
            Dialect name if set, None otherwise
        """
        return self._dialect

    def get_joined_tables(self) -> list[tuple[str, str]]:
        """
        Get all tables referenced in JOIN transformations.

        Returns:
            List of (schema, table) tuples for each joined table.
            Schema may be empty string if not specified.
        """
        from backend.application.config_parser.transformation_parsers.join_parser import JoinParsers

        joined_tables: list[tuple[str, str]] = []

        # Access transformations through transform_parser.get_transforms()
        for transform in self.transform_parser.get_transforms():
            if isinstance(transform, JoinParsers):
                for join_parser in transform.get_joins():
                    schema = join_parser.rhs_schema_name or ""
                    table = join_parser.rhs_table_name
                    if table:
                        joined_tables.append((schema, table))

        return joined_tables

    def has_source_location(self, transformation_id: str) -> bool:
        """
        Check if a transformation has source location information.

        Args:
            transformation_id: The transformation identifier to check

        Returns:
            True if source location is available, False otherwise
        """
        return transformation_id in self._source_map

    @property
    def tracked_transformations(self) -> list[str]:
        """
        Get list of transformation IDs with source location tracking.

        Returns:
            List of transformation identifiers that have source location data
        """
        return list(self._source_map.keys())

    @property
    def yaml_content(self) -> Optional[str]:
        """
        Get the original YAML content if available.

        Returns:
            The raw YAML string, or None if not set
        """
        return self._yaml_content

    # =========================================================================
    # SQL Generation Methods for Direct Execution
    # =========================================================================

    def get_compiled_sql(self) -> str:
        """
        Generate SQL from the model configuration for direct execution.

        This method builds a SQL statement from the transformation configuration,
        respecting the transformation order specified in the YAML (transform_order).

        The implementation uses CTEs (Common Table Expressions) to ensure
        transformations are applied in the exact order specified, matching
        the legacy Ibis behavior where each transformation builds on the
        previous result.

        Supports all transformation types: filter, join, union, pivot, rename,
        combine_columns, synthesize, groups_and_aggregation, find_and_replace,
        distinct, and window functions.

        Returns:
            SQL string representing the model transformation
        """
        builder = SQLQueryBuilder(self)
        return builder.build()

