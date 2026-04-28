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

    def _find_transform_by_type(self, transforms: list, transform_type: str):
        """Find a transformation by type."""
        for transform in transforms:
            t_type = getattr(transform, 'transform_type', None) or transform.get("transformation_type", "")
            if t_type == transform_type:
                return transform
        return None

    def _get_source_table_ref(self) -> str:
        """Get the fully qualified source table reference."""
        source_schema = self.source_schema_name
        source_table = self.source_table_name
        if source_schema:
            return f'"{source_schema}"."{source_table}"'
        return f'"{source_table}"'

    def _build_select_columns(self, transforms: list) -> str:
        """
        Build SELECT columns including all transformation-generated columns.

        The logic follows transformation order and ensures:
        1. Base columns (from source or presentation) are always included
        2. Transformation-generated columns are added in order
        3. Certain transformations (rename, groups_and_aggregation) replace base columns
        4. JOIN columns are prefixed with table name (like legacy: rname='{table}_{name}')
        """
        additional_columns = []  # Columns added by transformations (synthesize, combine, window)
        replacement_columns = []  # Columns that replace base columns (rename, group_by)
        join_columns = []  # Columns from joined tables (prefixed)
        replaces_base = False  # Flag if transformation replaces all base columns
        has_join = False  # Flag if there's a join transformation

        # Process transformations in order
        for transform in transforms:
            # Use transform_type property (BaseParser stores it as _transform_type)
            t_type = getattr(transform, 'transform_type', None) or transform.get("transformation_type", "")

            if t_type == "rename_column":
                # Rename replaces base columns with aliased versions
                replacement_columns.extend(self._build_rename_columns(transform))
                replaces_base = True
            elif t_type == "combine_columns":
                # Combine adds new columns alongside existing ones
                additional_columns.extend(self._build_combine_columns(transform))
            elif t_type == "synthesize":
                # Synthesize adds new computed columns alongside existing ones
                additional_columns.extend(self._build_synthesize_columns(transform))
            elif t_type == "find_and_replace":
                # Find/replace modifies existing columns (replaces them)
                replacement_columns.extend(self._build_find_replace_columns(transform))
                replaces_base = True
            elif t_type == "window":
                # Window adds new columns alongside existing ones
                additional_columns.extend(self._build_window_columns(transform))
            elif t_type == "groups_and_aggregation":
                # Group by replaces base columns with group + aggregate columns
                replacement_columns.extend(self._build_aggregate_columns(transform))
                replaces_base = True
            elif t_type == "join":
                # Join adds prefixed columns from joined tables (like legacy rname)
                has_join = True
                join_columns.extend(self._build_join_select_columns(transform))

        # Build final column list
        if replaces_base:
            # Use replacement columns (rename, find_replace, or group_by)
            return ", ".join(replacement_columns) if replacement_columns else "*"
        else:
            # Start with base columns and add transformation columns
            base_cols = self._build_base_select_columns()

            # Build column list
            all_columns = []

            if has_join:
                # For JOINs, use source_table.* to be explicit about source columns
                source_table = self.source_table_name
                if base_cols == "*":
                    all_columns.append(f'"{source_table}".*')
                else:
                    # Prefix base columns with source table
                    for col in base_cols.split(", "):
                        col_name = col.strip('"')
                        all_columns.append(f'"{source_table}"."{col_name}"')
                # Add prefixed join columns
                all_columns.extend(join_columns)
            else:
                # No join, use base columns as-is
                if base_cols == "*":
                    all_columns.append("*")
                else:
                    all_columns.append(base_cols)

            # Add additional columns (synthesize, combine, window)
            all_columns.extend(additional_columns)

            return ", ".join(all_columns) if all_columns else "*"

    def _build_join_select_columns(self, transform) -> list[str]:
        """
        Build SELECT columns for joined tables.

        LEGACY BEHAVIOR (rname pattern):
        Legacy Ibis path uses: rname='{right_table}_{name}' which automatically
        prefixes ALL columns from joined tables (e.g., raw_orders.id -> raw_orders_id).

        This method matches legacy behavior by generating explicit column aliases
        when column metadata is available (via set_join_table_columns).

        Returns:
            List of column expressions for joined tables
        """
        from backend.application.config_parser.transformation_parsers.join_parser import JoinParsers

        if not isinstance(transform, JoinParsers):
            return []

        columns = []
        for join_parser in transform.get_joins():
            # Get table reference name (alias or table name)
            alias = join_parser.alias_name
            rhs_table = join_parser.rhs_table_name
            rhs_schema = join_parser.rhs_schema_name
            table_ref = alias or rhs_table

            # Try to get column information in order of precedence:
            # 1. Explicitly set join table columns (from database introspection)
            # 2. Referenced model columns
            # 3. Fallback to table.*
            join_columns = self.get_join_table_columns(rhs_schema, rhs_table)
            if not join_columns:
                join_columns = self._get_reference_columns(rhs_schema, rhs_table)

            if join_columns:
                # We have column info - generate explicit aliases with prefixes
                # This matches legacy behavior: {table_ref}_{column_name}
                for col in join_columns:
                    columns.append(f'"{table_ref}"."{col}" AS "{table_ref}_{col}"')
            else:
                # No column info available - use table.*
                # This returns all columns but without prefix aliases
                columns.append(f'"{table_ref}".*')

        return columns

    def _get_reference_columns(self, schema: str, table: str) -> list[str]:
        """
        Try to get column information from referenced models.

        If the joined table is produced by a model in all_reference,
        we can look up its column configuration.

        Args:
            schema: The schema name of the joined table
            table: The table name of the joined table

        Returns:
            List of column names if found, empty list otherwise
        """
        # Check if this table is produced by a referenced model
        for _ref_name, ref_data in self._all_reference.items():
            dest_table = ref_data.get("destination_table")
            dest_schema = ref_data.get("destination_schema")

            if dest_table == table and dest_schema == schema:
                # Found the model that produces this table
                # Try to get its presentation columns
                columns = ref_data.get("columns", [])
                if columns and columns != ["*"]:
                    return columns

        return []

    def _build_base_select_columns(self) -> str:
        """Build base SELECT columns from presentation parser."""
        hidden_columns = self.presentation_parser.hidden_columns

        if hidden_columns is None or hidden_columns == ["*"]:
            return "*"

        if not hidden_columns:
            return "*"

        columns = [f'"{col}"' for col in hidden_columns]
        return ", ".join(columns)

    def _build_rename_columns(self, transform) -> list[str]:
        """Build SELECT columns with renames (aliases)."""
        from backend.application.config_parser.transformation_parsers.rename_parser import RenameParsers

        if not isinstance(transform, RenameParsers):
            return []

        columns = []
        for rename_parser in transform.get_rename_parsers():
            old_name = rename_parser.old_name
            new_name = rename_parser.new_name
            if old_name and new_name:
                columns.append(f'"{old_name}" AS "{new_name}"')

        return columns

    def _build_combine_columns(self, transform) -> list[str]:
        """Build CONCAT expressions for combine_columns transformation."""
        from backend.application.config_parser.transformation_parsers.combine_parser import CombineColumnParser

        if not isinstance(transform, CombineColumnParser):
            return []

        columns = []
        for combine_col in transform.columns:
            parts = []
            for value in combine_col.values:
                if value.type == "column":
                    parts.append(f'CAST("{value.value}" AS VARCHAR)')
                else:
                    # Literal value
                    escaped = str(value.value).replace("'", "''")
                    parts.append(f"'{escaped}'")

            if parts:
                concat_expr = " || ".join(parts)
                col_name = combine_col.column_name
                columns.append(f'({concat_expr}) AS "{col_name}"')

        return columns

    def _build_synthesize_columns(self, transform) -> list[str]:
        """Build computed columns from synthesize transformation."""
        from backend.application.config_parser.transformation_parsers.synthesize_parser import SynthesizeParser
        from backend.application.config_parser.sql_builder import FormulaTranslator

        if not isinstance(transform, SynthesizeParser):
            return []

        columns = []
        for col_parser in transform.columns:
            col_name = col_parser.column_name
            formula = col_parser.formula

            if formula:
                # Translate Visitran formula functions to SQL
                translated_formula = FormulaTranslator.translate(str(formula))
                columns.append(f'({translated_formula}) AS "{col_name}"')

        return columns

    def _build_find_replace_columns(self, transform) -> list[str]:
        """Build REPLACE expressions for find_and_replace transformation."""
        from backend.application.config_parser.transformation_parsers.find_and_replace_parser import FindAndReplaceParser

        if not isinstance(transform, FindAndReplaceParser):
            return []

        columns = []
        for col_group in transform.columns:
            # col_group.columns returns the list from "column_list" key
            col_names = col_group.columns or []
            for col_name in col_names:
                expr = f'"{col_name}"'
                for operation in col_group.operations:
                    # Use find_value and replace_value properties
                    find_val = str(operation.find_value or "").replace("'", "''")
                    replace_val = str(operation.replace_value or "").replace("'", "''")
                    match_type = operation.match_type or "exact"

                    if match_type == "regex":
                        # Use REGEXP_REPLACE for regex matching
                        expr = f"REGEXP_REPLACE({expr}, '{find_val}', '{replace_val}')"
                    else:
                        # Use REPLACE for exact matching
                        expr = f"REPLACE({expr}, '{find_val}', '{replace_val}')"

                columns.append(f'{expr} AS "{col_name}"')

        return columns

    def _build_window_columns(self, transform) -> list[str]:
        """Build window function expressions."""
        from backend.application.config_parser.transformation_parsers.window_parser import WindowParser

        if not isinstance(transform, WindowParser):
            return []

        columns = []
        for col_parser in transform.columns:
            col_name = col_parser.column_name
            window_func = col_parser.window_function.upper()
            partition_by = col_parser.partition_by
            order_by = col_parser.order_by
            agg_column = col_parser.agg_column

            # Build window function
            if window_func in ("ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE"):
                func_expr = f"{window_func}()"
            elif window_func in ("SUM", "AVG", "MIN", "MAX", "COUNT"):
                agg_col = f'"{agg_column}"' if agg_column else "*"
                func_expr = f"{window_func}({agg_col})"
            elif window_func in ("LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE"):
                agg_col = f'"{agg_column}"' if agg_column else '""'
                func_expr = f"{window_func}({agg_col})"
            else:
                func_expr = f"{window_func}()"

            # Build OVER clause
            over_parts = []
            if partition_by:
                partition_cols = ", ".join([f'"{p}"' for p in partition_by])
                over_parts.append(f"PARTITION BY {partition_cols}")

            if order_by:
                order_specs = []
                for spec in order_by:
                    col = spec.get("column", "")
                    direction = spec.get("direction", "ASC").upper()
                    if col:
                        order_specs.append(f'"{col}" {direction}')
                if order_specs:
                    over_parts.append(f"ORDER BY {', '.join(order_specs)}")

            # Build frame specification if present
            if col_parser.has_frame_spec():
                frame_spec = self._build_window_frame_spec(col_parser)
                if frame_spec:
                    over_parts.append(frame_spec)

            over_clause = " ".join(over_parts)
            columns.append(f'{func_expr} OVER ({over_clause}) AS "{col_name}"')

        return columns

    def _build_window_frame_spec(self, col_parser) -> str:
        """Build window frame specification (ROWS BETWEEN ... AND ...)."""
        preceding = col_parser.preceding
        following = col_parser.following

        if preceding is None and following is None:
            return ""

        # Convert preceding
        if preceding == "unbounded":
            start = "UNBOUNDED PRECEDING"
        elif preceding == 0:
            start = "CURRENT ROW"
        elif isinstance(preceding, int):
            start = f"{preceding} PRECEDING"
        else:
            start = "UNBOUNDED PRECEDING"

        # Convert following
        if following == "unbounded":
            end = "UNBOUNDED FOLLOWING"
        elif following == 0:
            end = "CURRENT ROW"
        elif isinstance(following, int):
            end = f"{following} FOLLOWING"
        else:
            end = "CURRENT ROW"

        return f"ROWS BETWEEN {start} AND {end}"

    def _build_aggregate_columns(self, transform) -> list[str]:
        """Build aggregate function columns from groups_and_aggregation."""
        from backend.application.config_parser.transformation_parsers.groups_and_aggregation_parser import (
            GroupsAndAggregationParser
        )

        if not isinstance(transform, GroupsAndAggregationParser):
            return []

        columns = []

        # Add group columns
        for group_col in transform.group_columns:
            columns.append(f'"{group_col}"')

        # Add aggregate columns
        for agg_col in transform.aggregate_columns:
            if agg_col.is_formula_aggregate:
                # Formula-based aggregate
                expr = agg_col.expression
                alias = agg_col.alias
                columns.append(f'({expr}) AS "{alias}"')
            else:
                # Simple aggregate
                func = agg_col.function.upper()
                col = agg_col.column
                alias = agg_col.alias or f"{func}_{col}"

                if func == "COUNT" and col == "*":
                    columns.append(f'COUNT(*) AS "{alias}"')
                else:
                    columns.append(f'{func}("{col}") AS "{alias}"')

        return columns

    def _build_from_clause(self, transforms: list) -> str:
        """Build FROM clause including JOINs."""
        from_clause = self._get_source_table_ref()

        # Find JOIN transformations
        for transform in transforms:
            t_type = getattr(transform, 'transform_type', None) or transform.get("transformation_type", "")
            if t_type == "join":
                from_clause += self._build_join_clause(transform)

        return from_clause

    def _build_join_clause(self, transform) -> str:
        """Build JOIN clause from join transformation."""
        from backend.application.config_parser.transformation_parsers.join_parser import JoinParsers

        if not isinstance(transform, JoinParsers):
            return ""

        join_parts = []
        for join_parser in transform.get_joins():
            join_type = (join_parser.join_type or "INNER").upper()

            # Get joined table reference
            rhs_schema = join_parser.rhs_schema_name
            rhs_table = join_parser.rhs_table_name
            alias = join_parser.alias_name

            if rhs_schema:
                table_ref = f'"{rhs_schema}"."{rhs_table}"'
            else:
                table_ref = f'"{rhs_table}"'

            if alias:
                table_ref += f' AS "{alias}"'

            # Build join condition - check if columns are in direct properties or criteria
            lhs_col = join_parser.lhs_column_name
            rhs_col = join_parser.rhs_column_name

            # Get filter/criteria which may contain the join condition
            filter_parser = join_parser.join_filter

            if lhs_col and rhs_col:
                # Columns defined directly in source/joined_table
                operator = join_parser.operator or "="
                lhs_table = join_parser.lhs_table_name
                if lhs_table:
                    lhs_ref = f'"{lhs_table}"."{lhs_col}"'
                else:
                    lhs_ref = f'"{lhs_col}"'

                rhs_table_ref = alias or rhs_table
                rhs_ref = f'"{rhs_table_ref}"."{rhs_col}"'

                join_condition = f"{lhs_ref} {operator} {rhs_ref}"

                # Add additional filter conditions
                if filter_parser and filter_parser.conditions:
                    additional = self._parse_filter_to_sql(filter_parser)
                    if additional:
                        join_condition += f" AND {additional}"
            else:
                # Columns defined only in criteria - use filter parser for join condition
                if filter_parser and filter_parser.conditions:
                    join_condition = self._parse_filter_to_sql(filter_parser)
                else:
                    # Cross join case - no condition
                    join_condition = "1=1"

            join_parts.append(f" {join_type} JOIN {table_ref} ON {join_condition}")

        return "".join(join_parts)

    def _build_where_clause(self, transforms: list) -> str:
        """Build WHERE clause from filter transformations."""
        from backend.application.config_parser.transformation_parsers.filter_parser import FilterParser

        where_parts = []

        for transform in transforms:
            t_type = getattr(transform, 'transform_type', None) or transform.get("transformation_type", "")
            if t_type == "filter" and isinstance(transform, FilterParser):
                filter_sql = self._parse_filter_to_sql(transform)
                if filter_sql:
                    where_parts.append(f"({filter_sql})")

        if where_parts:
            return " AND ".join(where_parts)
        return ""

    def _parse_filter_to_sql(self, filter_parser) -> str:
        """Convert a FilterParser to SQL WHERE clause."""
        from backend.application.config_parser.transformation_parsers.filter_parser import FilterParser

        if not isinstance(filter_parser, FilterParser):
            return ""

        conditions = filter_parser.conditions
        if not conditions:
            return ""

        sql_conditions = []
        for i, cond in enumerate(conditions):
            # Handle LHS
            if cond.lhs_type == "FORMULA" and cond.lhs_expression:
                lhs = f"({cond.lhs_expression})"
            else:
                lhs_col = cond.lhs_column.column_name if cond.lhs_column else ""
                if not lhs_col:
                    continue
                # Include table name if present (for JOIN conditions)
                lhs_table = cond.lhs_column.table_name if cond.lhs_column else ""
                if lhs_table:
                    lhs = f'"{lhs_table}"."{lhs_col}"'
                else:
                    lhs = f'"{lhs_col}"'

            operator = cond.operator or "EQ"
            condition_type = cond.condition_type if i > 0 else ""

            # Handle no-RHS operators
            if operator in ("ISNULL", "ISNOTNULL", "NULL", "NOTNULL"):
                sql_op = "IS NULL" if operator in ("ISNULL", "NULL") else "IS NOT NULL"
                condition_sql = f'{lhs} {sql_op}'
            elif operator in ("TRUE", "FALSE"):
                condition_sql = f'{lhs} = {operator}'
            else:
                sql_op = self._get_sql_operator(operator)

                # Handle RHS
                if cond.rhs_type == "COLUMN":
                    rhs_col = cond.rhs_column.column_name if cond.rhs_column else ""
                    # Include table name if present (for JOIN conditions)
                    rhs_table = cond.rhs_column.table_name if cond.rhs_column else ""
                    if rhs_table:
                        rhs = f'"{rhs_table}"."{rhs_col}"'
                    else:
                        rhs = f'"{rhs_col}"'
                elif cond.rhs_type == "FORMULA" and cond.rhs_expression:
                    rhs = f"({cond.rhs_expression})"
                else:
                    rhs = self._format_sql_value(cond.rhs_value, operator)

                condition_sql = f'{lhs} {sql_op} {rhs}'

            if condition_type and sql_conditions:
                sql_conditions.append(f"{condition_type} {condition_sql}")
            else:
                sql_conditions.append(condition_sql)

        return " ".join(sql_conditions)

    def _build_group_by_clause(self, transforms: list) -> str:
        """Build GROUP BY clause from groups_and_aggregation transformation."""
        from backend.application.config_parser.transformation_parsers.groups_and_aggregation_parser import (
            GroupsAndAggregationParser
        )

        for transform in transforms:
            t_type = getattr(transform, 'transform_type', None) or transform.get("transformation_type", "")
            if t_type == "groups_and_aggregation" and isinstance(transform, GroupsAndAggregationParser):
                group_cols = transform.group_columns
                if group_cols:
                    return ", ".join([f'"{col}"' for col in group_cols])

        return ""

    def _build_having_clause(self, transforms: list) -> str:
        """Build HAVING clause from groups_and_aggregation transformation."""
        from backend.application.config_parser.transformation_parsers.groups_and_aggregation_parser import (
            GroupsAndAggregationParser
        )

        for transform in transforms:
            t_type = getattr(transform, 'transform_type', None) or transform.get("transformation_type", "")
            if t_type == "groups_and_aggregation" and isinstance(transform, GroupsAndAggregationParser):
                having_parser = transform.having
                if having_parser:
                    return self._parse_filter_to_sql(having_parser)

        return ""

    def _build_distinct_prefix(self, transforms: list) -> str:
        """Build DISTINCT prefix if distinct transformation is present."""
        from backend.application.config_parser.transformation_parsers.distinct_parser import DistinctParser

        for transform in transforms:
            t_type = getattr(transform, 'transform_type', None) or transform.get("transformation_type", "")
            if t_type == "distinct" and isinstance(transform, DistinctParser):
                distinct_cols = transform.columns
                if distinct_cols:
                    # DISTINCT ON specific columns (PostgreSQL syntax)
                    cols = ", ".join([f'"{col}"' for col in distinct_cols])
                    return f"DISTINCT ON ({cols}) "
                else:
                    return "DISTINCT "

        return ""

    def _build_union_sql(self, union_transform) -> str:
        """Build UNION SQL from union transformation."""
        from backend.application.config_parser.transformation_parsers.union_parser import UnionParsers

        if not isinstance(union_transform, UnionParsers):
            return self._get_source_table_ref()

        queries = []
        ignore_duplicates = union_transform.unions_duplicate

        if union_transform.is_branch_based():
            # New branch-based union format
            for branch in union_transform.get_branch_parsers():
                schema = branch.schema
                table = branch.table

                if schema:
                    table_ref = f'"{schema}"."{table}"'
                else:
                    table_ref = f'"{table}"'

                # Build SELECT for this branch
                col_exprs = []
                for col_expr in branch.get_column_expressions():
                    output_col = col_expr.output_column
                    expr_type = col_expr.expression_type

                    if expr_type == "COLUMN":
                        col_name = col_expr.column_name
                        if col_name == output_col:
                            col_exprs.append(f'"{col_name}"')
                        else:
                            col_exprs.append(f'"{col_name}" AS "{output_col}"')
                    elif expr_type == "LITERAL":
                        lit_val = col_expr.literal_value
                        lit_type = col_expr.literal_type
                        cast_type = col_expr.cast_type

                        if lit_type in ("Integer", "Float", "Number"):
                            val_expr = str(lit_val)
                        elif lit_type == "Boolean":
                            val_expr = "TRUE" if lit_val else "FALSE"
                        else:
                            escaped = str(lit_val).replace("'", "''")
                            val_expr = f"'{escaped}'"

                        if cast_type:
                            val_expr = f"CAST({val_expr} AS {cast_type})"

                        col_exprs.append(f'{val_expr} AS "{output_col}"')
                    elif expr_type == "FORMULA":
                        formula = col_expr.formula
                        col_exprs.append(f'({formula}) AS "{output_col}"')

                select_clause = ", ".join(col_exprs) if col_exprs else "*"

                # Build WHERE for this branch
                where_clause = ""
                if branch.filters:
                    filter_sql = self._parse_filter_to_sql(branch.filters)
                    if filter_sql:
                        where_clause = f" WHERE {filter_sql}"

                queries.append(f"SELECT {select_clause} FROM {table_ref}{where_clause}")
        else:
            # Legacy table-based union format
            # First, add the source table query
            source_ref = self._get_source_table_ref()
            source_cols = union_transform.column_names

            if source_cols:
                col_list = ", ".join([f'"{col}"' for col in source_cols])
                queries.append(f"SELECT {col_list} FROM {source_ref}")
            else:
                queries.append(f"SELECT * FROM {source_ref}")

            # Add union tables
            for union_parser in union_transform.get_union_parsers():
                merge_table = union_parser.merge_table
                merge_schema = union_parser.merge_schema
                merge_col = union_parser.merge_column

                if merge_table and merge_col:
                    # Include schema if available
                    if merge_schema:
                        merge_table_ref = f'"{merge_schema}"."{merge_table}"'
                    else:
                        merge_table_ref = f'"{merge_table}"'
                    query = f'SELECT "{merge_col}" FROM {merge_table_ref}'

                    # Add filters if present
                    if union_parser.filters:
                        filter_sql = self._parse_filter_to_sql(union_parser.filters)
                        if filter_sql:
                            query += f" WHERE {filter_sql}"

                    queries.append(query)

        # Combine queries with UNION
        union_keyword = "UNION" if ignore_duplicates else "UNION ALL"
        return f" {union_keyword} ".join(queries)

    def _build_pivot_sql(self, pivot_transform) -> str:
        """Build PIVOT SQL (database-specific, using CASE WHEN approach)."""
        from backend.application.config_parser.transformation_parsers.pivot_parser import PivotParser

        if not isinstance(pivot_transform, PivotParser):
            return ""

        # Note: PIVOT is complex and database-specific
        # This implements a portable CASE WHEN approach
        row_col = pivot_transform.to_rows
        col_col = pivot_transform.to_column_names
        value_col = pivot_transform.values_from
        aggregator = pivot_transform.aggregator or "SUM"

        # For a proper PIVOT, we'd need to know the distinct values in col_col
        # This is a simplified version that returns a grouped query
        source_ref = self._get_source_table_ref()

        return f'''SELECT "{row_col}", "{col_col}", {aggregator}("{value_col}") AS "{value_col}"
                   FROM {source_ref}
                   GROUP BY "{row_col}", "{col_col}"'''

    def _get_sql_operator(self, operator: str) -> str:
        """Map transformation operators to SQL operators."""
        operator_map = {
            "EQ": "=",
            "NEQ": "!=",
            "GT": ">",
            "GTE": ">=",
            "LT": "<",
            "LTE": "<=",
            "CONTAINS": "LIKE",
            "STARTSWITH": "LIKE",
            "ENDSWITH": "LIKE",
            "IN": "IN",
            "NOTIN": "NOT IN",
            "ISNULL": "IS NULL",
            "ISNOTNULL": "IS NOT NULL",
            "BETWEEN": "BETWEEN",
            "NOTBETWEEN": "NOT BETWEEN",
        }
        return operator_map.get(operator, "=")

    def _format_sql_value(self, value: Any, operator: str = "") -> str:
        """Format a value for SQL."""
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, list):
            if operator in ("IN", "NOTIN"):
                formatted = [self._format_sql_value(v) for v in value]
                return f"({', '.join(formatted)})"
            elif operator in ("BETWEEN", "NOTBETWEEN") and len(value) >= 2:
                return f"{self._format_sql_value(value[0])} AND {self._format_sql_value(value[1])}"
            # For other operators with list, take first value
            value = value[0] if value else ""

        # String value - escape single quotes
        escaped = str(value).replace("'", "''")

        # Handle LIKE operators with wildcards
        if operator == "CONTAINS":
            return f"'%{escaped}%'"
        elif operator == "STARTSWITH":
            return f"'{escaped}%'"
        elif operator == "ENDSWITH":
            return f"'%{escaped}'"

        return f"'{escaped}'"

    def _build_order_by_clause(self, transforms: list) -> str:
        """Build ORDER BY clause from presentation sort.

        When there's a JOIN, column names are qualified with the source table name
        to avoid ambiguity (e.g., both tables having 'id' column).

        Args:
            transforms: List of transforms to check for JOINs
        """
        sort_specs = self.presentation_parser.sort

        if not sort_specs:
            return ""

        # Check if there's a JOIN - if so, we need to qualify column names
        has_join = any(
            (getattr(t, 'transform_type', None) or t.get("transformation_type", "")) == "join"
            for t in transforms
        )

        # Get source table name for qualifying columns when there's a JOIN
        source_table = self.source_table_name if has_join else None

        order_parts = []
        for sort_spec in sort_specs:
            column = sort_spec.get("column", "")
            direction = sort_spec.get("order", "asc").upper()

            if not column:
                continue

            if direction not in ("ASC", "DESC"):
                direction = "ASC"

            # Qualify column with source table if there's a JOIN
            if source_table:
                order_parts.append(f'"{source_table}"."{column}" {direction}')
            else:
                order_parts.append(f'"{column}" {direction}')

        return ", ".join(order_parts)
