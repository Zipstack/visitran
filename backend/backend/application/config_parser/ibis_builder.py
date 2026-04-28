"""
IbisBuilder: SQL-to-Ibis Transformation Compiler.

This module provides the IbisBuilder class that compiles SQL transformation
strings from YAML model definitions into Ibis Table expressions. It supports
model reference resolution via ref('schema', 'model') patterns and chained
transformations.

Usage:
    builder = IbisBuilder(registry=registry, connection=con)

    # Compile a single transformation
    result = builder.compile_transformation(sql, source_table)

    # Compile chained transformations
    result = builder.compile_chain(transformations, source_table)
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Optional, Union

from visitran.errors import TransformationError, VisitranBaseExceptions
from visitran.errors.error_codes import ErrorCodeConstants

if TYPE_CHECKING:
    import ibis
    from ibis.expr.types import Table
    from ibis import BaseBackend

    from backend.application.config_parser.config_parser import ConfigParser
    from backend.application.config_parser.model_registry import ModelRegistry

logger = logging.getLogger(__name__)

# Pattern to match ref('schema', 'model') or ref("schema", "model")
REF_PATTERN = re.compile(
    r"""ref\s*\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)""",
    re.IGNORECASE,
)


@dataclass
class RefResolution:
    """
    Represents a resolved model reference.

    Attributes:
        schema: The schema name from the ref() call
        model: The model name from the ref() call
        qualified_name: The fully qualified name (schema.model)
        original_text: The original ref() text that was matched
        start_pos: Start position in the SQL string
        end_pos: End position in the SQL string
    """

    schema: str
    model: str
    qualified_name: str
    original_text: str
    start_pos: int
    end_pos: int


@dataclass
class CompilationResult:
    """
    Result of a SQL-to-Ibis compilation.

    Attributes:
        table: The resulting Ibis Table expression
        resolved_refs: List of model references that were resolved
        sql_original: The original SQL string
        sql_resolved: The SQL string with refs replaced
        warnings: Any warnings generated during compilation
    """

    table: Table
    resolved_refs: list[RefResolution] = field(default_factory=list)
    sql_original: str = ""
    sql_resolved: str = ""
    warnings: list[str] = field(default_factory=list)


@dataclass
class TransformationStep:
    """
    Represents a single transformation step in a chain.

    Attributes:
        operation: The operation type (filter, join, aggregate, select, sql)
        sql: The SQL expression or transformation SQL
        params: Additional parameters for the operation
        line_number: Optional line number from YAML source
    """

    operation: str
    sql: str
    params: dict[str, Any] = field(default_factory=dict)
    line_number: Optional[int] = None


class IbisBuildError(VisitranBaseExceptions):
    """
    Raised when Ibis expression building fails.

    Attributes:
        message: Description of the error
        sql: The SQL that caused the error
        line_number: Optional line number from YAML source
        column_number: Optional column number
    """

    def __init__(
        self,
        message: str,
        sql: Optional[str] = None,
        line_number: Optional[int] = None,
        column_number: Optional[int] = None,
    ):
        self.message = message
        self.sql = sql
        self.line_number = line_number
        self.column_number = column_number
        super().__init__(
            error_code=ErrorCodeConstants.IBIS_BUILD_ERROR,
            message=message,
        )

    def to_transformation_error(self, model_name: str = "unknown") -> TransformationError:
        """Convert to TransformationError for consistent error handling."""
        return TransformationError(
            model_name=model_name,
            transformation_id=None,
            error_message=self.message,
            line_number=self.line_number,
            column_number=self.column_number,
            yaml_snippet=self.sql,
        )

    def error_response(self) -> dict:
        """Return structured error response."""
        response = super().error_response()
        if self.sql:
            response["sql"] = self.sql
        if self.line_number:
            response["line_number"] = self.line_number
        if self.column_number:
            response["column_number"] = self.column_number
        return response


class MissingReferenceError(IbisBuildError):
    """Raised when a model reference cannot be resolved."""

    def __init__(
        self,
        schema: str,
        model: str,
        sql: Optional[str] = None,
        line_number: Optional[int] = None,
    ):
        self.schema = schema
        self.model = model
        message = f"Model reference not found: ref('{schema}', '{model}')"
        super().__init__(message, sql, line_number)


class IbisBuilder:
    """
    Compiles SQL transformations to Ibis Table expressions.

    The IbisBuilder serves as the SQL-to-Ibis compilation engine for transformation
    definitions. It parses SQL transformation strings from YAML model definitions
    and converts them into equivalent Ibis Table expressions.

    Features:
    - SQL-to-Ibis compilation using ibis.sql()
    - Model reference resolution via ref('schema', 'model') patterns
    - Transformation chaining with preserved execution order
    - Immutable transformations (each operation returns new Table)

    Attributes:
        _registry: Optional ModelRegistry for resolving model references
        _connection: Optional Ibis backend connection for SQL compilation
        _table_cache: Cache of resolved Ibis tables by qualified name
    """

    def __init__(
        self,
        registry: Optional[ModelRegistry] = None,
        connection: Optional[BaseBackend] = None,
    ) -> None:
        """
        Initialize the IbisBuilder.

        Args:
            registry: Optional ModelRegistry for resolving model references.
                     If not provided, ref() patterns will cause errors.
            connection: Optional Ibis backend connection for SQL compilation.
                       Required for compile_transformation() to work.
        """
        self._registry = registry
        self._connection = connection
        self._table_cache: dict[str, Table] = {}

    @property
    def registry(self) -> Optional[ModelRegistry]:
        """Return the model registry."""
        return self._registry

    @property
    def connection(self) -> Optional[BaseBackend]:
        """Return the Ibis connection."""
        return self._connection

    def set_connection(self, connection: BaseBackend) -> None:
        """
        Set the Ibis backend connection.

        Args:
            connection: Ibis backend connection for SQL compilation
        """
        self._connection = connection

    def set_registry(self, registry: ModelRegistry) -> None:
        """
        Set the model registry.

        Args:
            registry: ModelRegistry for resolving model references
        """
        self._registry = registry

    def clear_cache(self) -> None:
        """Clear the table cache."""
        self._table_cache.clear()

    def find_refs(self, sql: str) -> list[RefResolution]:
        """
        Find all ref() patterns in a SQL string.

        Args:
            sql: The SQL string to scan for ref() patterns

        Returns:
            List of RefResolution objects for each match
        """
        refs = []
        for match in REF_PATTERN.finditer(sql):
            schema = match.group(1)
            model = match.group(2)
            refs.append(
                RefResolution(
                    schema=schema,
                    model=model,
                    qualified_name=f"{schema}.{model}",
                    original_text=match.group(0),
                    start_pos=match.start(),
                    end_pos=match.end(),
                )
            )
        return refs

    def resolve_ref(
        self,
        schema: str,
        model: str,
        line_number: Optional[int] = None,
    ) -> Table:
        """
        Resolve a model reference to an Ibis Table.

        Args:
            schema: The schema name
            model: The model name
            line_number: Optional line number for error reporting

        Returns:
            The Ibis Table for the referenced model

        Raises:
            MissingReferenceError: If the model is not found in the registry
            IbisBuildError: If no registry is configured
        """
        qualified_name = f"{schema}.{model}"

        # Check cache first
        if qualified_name in self._table_cache:
            return self._table_cache[qualified_name]

        if self._registry is None:
            raise IbisBuildError(
                "Cannot resolve model references without a ModelRegistry",
                line_number=line_number,
            )

        # Check if model exists in registry
        if not self._registry.contains(schema, model):
            raise MissingReferenceError(schema, model, line_number=line_number)

        # Get the ConfigParser from registry
        try:
            config = self._registry.get(schema, model)
        except KeyError as e:
            raise MissingReferenceError(schema, model, line_number=line_number) from e

        # Get the Ibis table - for now we construct from connection
        # In future, ConfigParser may provide the table directly
        if self._connection is None:
            raise IbisBuildError(
                f"Cannot resolve table for {qualified_name}: no connection configured",
                line_number=line_number,
            )

        try:
            # Get table from connection using schema.model
            table = self._connection.table(model, schema=schema)
            self._table_cache[qualified_name] = table
            return table
        except Exception as e:
            raise IbisBuildError(
                f"Failed to get table for {qualified_name}: {str(e)}",
                line_number=line_number,
            ) from e

    def resolve_refs_in_sql(
        self,
        sql: str,
        line_number: Optional[int] = None,
    ) -> tuple[str, list[RefResolution]]:
        """
        Resolve all ref() patterns in SQL and return modified SQL.

        This replaces ref('schema', 'model') patterns with the actual
        qualified table name (schema.model) for SQL compilation.

        Args:
            sql: The SQL string with ref() patterns
            line_number: Optional line number for error reporting

        Returns:
            Tuple of (resolved_sql, list_of_resolutions)

        Raises:
            MissingReferenceError: If any referenced model is not found
        """
        refs = self.find_refs(sql)
        if not refs:
            return sql, []

        # Validate all refs exist before replacing
        for ref in refs:
            if self._registry is None:
                raise IbisBuildError(
                    "Cannot resolve model references without a ModelRegistry",
                    sql=sql,
                    line_number=line_number,
                )
            if not self._registry.contains(ref.schema, ref.model):
                raise MissingReferenceError(
                    ref.schema, ref.model, sql=sql, line_number=line_number
                )

        # Replace refs with qualified names (process in reverse order to maintain positions)
        resolved_sql = sql
        for ref in reversed(refs):
            resolved_sql = (
                resolved_sql[: ref.start_pos]
                + ref.qualified_name
                + resolved_sql[ref.end_pos :]
            )

        return resolved_sql, refs

    def compile_transformation(
        self,
        sql: str,
        source_table: Optional[Table] = None,
        line_number: Optional[int] = None,
    ) -> CompilationResult:
        """
        Compile a SQL transformation string to an Ibis Table expression.

        Args:
            sql: The SQL transformation string
            source_table: Optional source table for the transformation
            line_number: Optional line number for error reporting

        Returns:
            CompilationResult containing the Ibis Table and metadata

        Raises:
            IbisBuildError: If compilation fails
        """
        import ibis

        if self._connection is None:
            raise IbisBuildError(
                "Cannot compile SQL without a connection",
                sql=sql,
                line_number=line_number,
            )

        # Resolve any ref() patterns
        resolved_sql, refs = self.resolve_refs_in_sql(sql, line_number)

        try:
            # Use ibis.sql() to compile the SQL
            # Note: ibis.sql() requires a connection to be set
            result_table = self._connection.sql(resolved_sql)

            return CompilationResult(
                table=result_table,
                resolved_refs=refs,
                sql_original=sql,
                sql_resolved=resolved_sql,
            )

        except Exception as e:
            error_msg = f"SQL compilation failed: {str(e)}"
            logger.error(f"{error_msg}\nSQL: {resolved_sql}")
            raise IbisBuildError(
                error_msg,
                sql=sql,
                line_number=line_number,
            ) from e

    def compile_chain(
        self,
        transformations: list[TransformationStep],
        source_table: Optional[Table] = None,
    ) -> CompilationResult:
        """
        Compile a chain of transformations in sequence.

        Each transformation is applied to the result of the previous one,
        maintaining the order defined in the YAML configuration.

        Args:
            transformations: List of TransformationStep objects
            source_table: Optional initial source table

        Returns:
            CompilationResult for the final transformation

        Raises:
            IbisBuildError: If any transformation in the chain fails
        """
        if not transformations:
            raise IbisBuildError("No transformations provided")

        current_table = source_table
        all_refs: list[RefResolution] = []
        all_warnings: list[str] = []

        for i, step in enumerate(transformations):
            try:
                if step.operation == "sql":
                    # Full SQL transformation
                    result = self.compile_transformation(
                        step.sql,
                        source_table=current_table,
                        line_number=step.line_number,
                    )
                    current_table = result.table
                    all_refs.extend(result.resolved_refs)
                    all_warnings.extend(result.warnings)

                elif step.operation == "filter":
                    # Filter operation
                    if current_table is None:
                        raise IbisBuildError(
                            "Filter requires a source table",
                            sql=step.sql,
                            line_number=step.line_number,
                        )
                    current_table = self._apply_filter(
                        current_table, step.sql, step.line_number
                    )

                elif step.operation == "select":
                    # Select operation
                    if current_table is None:
                        raise IbisBuildError(
                            "Select requires a source table",
                            sql=step.sql,
                            line_number=step.line_number,
                        )
                    current_table = self._apply_select(
                        current_table, step.sql, step.params, step.line_number
                    )

                elif step.operation == "join":
                    # Join operation
                    current_table = self._apply_join(
                        current_table, step.sql, step.params, step.line_number
                    )

                elif step.operation == "aggregate":
                    # Aggregate operation
                    if current_table is None:
                        raise IbisBuildError(
                            "Aggregate requires a source table",
                            sql=step.sql,
                            line_number=step.line_number,
                        )
                    current_table = self._apply_aggregate(
                        current_table, step.params, step.line_number
                    )

                else:
                    all_warnings.append(
                        f"Unknown operation '{step.operation}' at step {i}"
                    )

            except IbisBuildError:
                raise
            except Exception as e:
                raise IbisBuildError(
                    f"Transformation step {i} ({step.operation}) failed: {str(e)}",
                    sql=step.sql,
                    line_number=step.line_number,
                ) from e

        if current_table is None:
            raise IbisBuildError("Transformation chain produced no result")

        return CompilationResult(
            table=current_table,
            resolved_refs=all_refs,
            sql_original="",  # Chain doesn't have single SQL
            sql_resolved="",
            warnings=all_warnings,
        )

    def _apply_filter(
        self,
        table: Table,
        condition: str,
        line_number: Optional[int] = None,
    ) -> Table:
        """
        Apply a filter condition to a table.

        Args:
            table: The source Ibis Table
            condition: SQL WHERE clause condition
            line_number: Optional line number for error reporting

        Returns:
            New Ibis Table with filter applied
        """
        try:
            # Use Ibis's filter with SQL expression
            # This requires parsing the condition string
            import ibis

            # For complex conditions, we may need to use raw SQL
            # For now, try to parse simple conditions
            return table.filter(ibis.literal(condition))
        except Exception as e:
            raise IbisBuildError(
                f"Filter condition failed: {str(e)}",
                sql=condition,
                line_number=line_number,
            ) from e

    def _apply_select(
        self,
        table: Table,
        columns: str,
        params: dict[str, Any],
        line_number: Optional[int] = None,
    ) -> Table:
        """
        Apply a select operation to a table.

        Args:
            table: The source Ibis Table
            columns: Comma-separated column names or SQL expressions
            params: Additional parameters (e.g., aliases)
            line_number: Optional line number for error reporting

        Returns:
            New Ibis Table with select applied
        """
        try:
            # Parse column names
            column_list = [c.strip() for c in columns.split(",") if c.strip()]

            if not column_list:
                return table

            # Build selection
            selections = []
            for col in column_list:
                if col == "*":
                    return table
                selections.append(table[col])

            return table.select(selections)
        except Exception as e:
            raise IbisBuildError(
                f"Select operation failed: {str(e)}",
                sql=columns,
                line_number=line_number,
            ) from e

    def _apply_join(
        self,
        left_table: Optional[Table],
        right_ref: str,
        params: dict[str, Any],
        line_number: Optional[int] = None,
    ) -> Table:
        """
        Apply a join operation.

        Args:
            left_table: The left table (or None for first table)
            right_ref: Reference to the right table (may be ref() pattern)
            params: Join parameters (on, how)
            line_number: Optional line number for error reporting

        Returns:
            New Ibis Table with join applied
        """
        # Resolve right table reference
        refs = self.find_refs(right_ref)
        if refs:
            ref = refs[0]
            right_table = self.resolve_ref(ref.schema, ref.model, line_number)
        else:
            raise IbisBuildError(
                f"Invalid join reference: {right_ref}",
                sql=right_ref,
                line_number=line_number,
            )

        if left_table is None:
            return right_table

        # Get join parameters
        join_on = params.get("on", [])
        join_how = params.get("how", "inner")

        try:
            if isinstance(join_on, str):
                # Single column join
                return left_table.join(
                    right_table,
                    left_table[join_on] == right_table[join_on],
                    how=join_how,
                )
            elif isinstance(join_on, list) and len(join_on) == 2:
                # Two-column specification [left_col, right_col]
                return left_table.join(
                    right_table,
                    left_table[join_on[0]] == right_table[join_on[1]],
                    how=join_how,
                )
            else:
                raise IbisBuildError(
                    f"Invalid join 'on' specification: {join_on}",
                    line_number=line_number,
                )
        except Exception as e:
            raise IbisBuildError(
                f"Join operation failed: {str(e)}",
                sql=right_ref,
                line_number=line_number,
            ) from e

    def _apply_aggregate(
        self,
        table: Table,
        params: dict[str, Any],
        line_number: Optional[int] = None,
    ) -> Table:
        """
        Apply an aggregation operation.

        Args:
            table: The source Ibis Table
            params: Aggregation parameters (group_by, metrics)
            line_number: Optional line number for error reporting

        Returns:
            New Ibis Table with aggregation applied
        """
        group_by = params.get("group_by", [])
        metrics = params.get("metrics", {})

        if not metrics:
            raise IbisBuildError(
                "Aggregate requires at least one metric",
                line_number=line_number,
            )

        try:
            # Build grouping
            if isinstance(group_by, str):
                group_by = [group_by]

            grouped = table.group_by(group_by) if group_by else table

            # Build aggregations
            agg_exprs = {}
            for name, expr in metrics.items():
                if isinstance(expr, dict):
                    # e.g., {"column": "amount", "func": "sum"}
                    col = expr.get("column")
                    func = expr.get("func", "count")
                    if col:
                        if func == "sum":
                            agg_exprs[name] = table[col].sum()
                        elif func == "count":
                            agg_exprs[name] = table[col].count()
                        elif func == "avg":
                            agg_exprs[name] = table[col].mean()
                        elif func == "min":
                            agg_exprs[name] = table[col].min()
                        elif func == "max":
                            agg_exprs[name] = table[col].max()
                        else:
                            raise IbisBuildError(
                                f"Unknown aggregation function: {func}",
                                line_number=line_number,
                            )
                else:
                    # Simple expression
                    agg_exprs[name] = expr

            return grouped.aggregate(**agg_exprs)
        except IbisBuildError:
            raise
        except Exception as e:
            raise IbisBuildError(
                f"Aggregate operation failed: {str(e)}",
                line_number=line_number,
            ) from e


def create_ibis_builder(
    registry: Optional[ModelRegistry] = None,
    connection: Optional[BaseBackend] = None,
) -> IbisBuilder:
    """
    Convenience function to create an IbisBuilder.

    Args:
        registry: Optional ModelRegistry for resolving model references
        connection: Optional Ibis backend connection

    Returns:
        Configured IbisBuilder instance
    """
    return IbisBuilder(registry=registry, connection=connection)
