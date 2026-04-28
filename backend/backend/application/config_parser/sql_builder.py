"""
SQL Query Builder for transformation-order-aware SQL generation.

This module implements CTE-based SQL generation that respects the transformation
order specified in YAML configurations, matching legacy Ibis behavior.

Design Principles:
- Single Responsibility: Each class has one clear purpose
- Open/Closed: Extensible transformation handlers without modifying core logic
- Dependency Inversion: ConfigParser depends on abstract builder interface

The key insight: Legacy Ibis applies transformations sequentially where each
transformation builds on the previous result. SQL has a fixed clause order,
so we use CTEs (Common Table Expressions) to achieve the same semantics.

Example: If transform_order is [filter, join], legacy would:
1. Start with source table
2. Apply filter (WHERE) -> filtered result
3. Apply join to filtered result

SQL equivalent with CTEs:
    WITH step_1 AS (
        SELECT * FROM source_table WHERE filter_condition
    )
    SELECT step_1.*, joined_table.col AS joined_table_col
    FROM step_1
    JOIN joined_table ON ...
"""

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from backend.application.config_parser.config_parser import ConfigParser
    from backend.application.config_parser.base_parser import BaseParser


class FormulaTranslator:
    """
    Translates Visitran formula functions to SQL expressions.

    Handles two types of translations:
    1. Visitran-specific functions (DUPLICATE, ISIN, NOTIN, etc.) -> SQL equivalents
    2. All function arguments -> proper SQL types (columns, strings, numbers)

    Data type detection follows the legacy FormulaSQL approach:
    - Numbers (123, 45.67) -> numeric literals
    - Double-quoted strings ("hello") -> string literals (converted to 'hello')
    - NULL/NONE -> NULL
    - true/false -> TRUE/FALSE
    - Unquoted identifiers -> column references (wrapped with "column")
    """

    @classmethod
    def translate(cls, formula: str) -> str:
        """
        Translate Visitran formula to SQL.

        Args:
            formula: The formula string from YAML

        Returns:
            SQL-compatible expression
        """
        if not formula:
            return formula

        # Process the formula recursively
        result = cls._translate_expression(formula.strip())
        return result

    @classmethod
    def _get_data_type(cls, value: str) -> str:
        """
        Detect the data type of a value following legacy FormulaSQL logic.

        Returns: 'numeric', 'string', 'none', 'boolean', or 'column'
        """
        value = value.strip()

        # Check for numeric
        try:
            float(value)
            return 'numeric'
        except ValueError:
            pass

        if value.isnumeric():
            return 'numeric'

        # Check for string literal (double-quoted)
        if value.startswith('"') and value.endswith('"') and len(value) >= 2:
            return 'string'

        # Check for NULL/NONE
        if value.upper() in ('NULL', 'NONE'):
            return 'none'

        # Check for boolean
        if value.lower() in ('true', 'false'):
            return 'boolean'

        # Everything else is a column reference
        return 'column'

    @classmethod
    def _convert_value_to_sql(cls, value: str) -> str:
        """
        Convert a simple value (not a function) to its SQL representation.

        - Column references -> "column_name" (double-quoted identifier)
        - String literals "hello" -> 'hello' (single-quoted SQL string)
        - Numbers -> pass through
        - NULL/NONE -> NULL
        - true/false -> TRUE/FALSE
        """
        value = value.strip()
        data_type = cls._get_data_type(value)

        if data_type == 'numeric':
            return value
        elif data_type == 'string':
            # Convert double-quoted string to single-quoted SQL string
            inner = value[1:-1]
            # Escape single quotes in the string
            inner = inner.replace("'", "''")
            return f"'{inner}'"
        elif data_type == 'none':
            return 'NULL'
        elif data_type == 'boolean':
            return value.upper()
        else:  # column
            return f'"{value}"'

    @classmethod
    def _translate_expression(cls, expr: str) -> str:
        """
        Translate an expression, handling function calls recursively.
        """
        expr = expr.strip()
        if not expr:
            return expr

        # Check if this is a function call: NAME(...)
        # Find the first '(' that's not inside quotes
        func_start = -1
        in_string = False
        string_char = None

        for i, char in enumerate(expr):
            if char in ('"', "'") and not in_string:
                in_string = True
                string_char = char
            elif char == string_char and in_string:
                in_string = False
                string_char = None
            elif char == '(' and not in_string:
                func_start = i
                break

        if func_start > 0:
            # This looks like a function call
            func_name = expr[:func_start].strip()
            # Validate it's a valid identifier
            if func_name and func_name[0].isalpha() or func_name[0] == '_':
                # Find the matching closing parenthesis
                paren_depth = 0
                args_end = -1
                for i in range(func_start, len(expr)):
                    char = expr[i]
                    if char in ('"', "'") and not in_string:
                        in_string = True
                        string_char = char
                    elif char == string_char and in_string:
                        in_string = False
                        string_char = None
                    elif char == '(' and not in_string:
                        paren_depth += 1
                    elif char == ')' and not in_string:
                        paren_depth -= 1
                        if paren_depth == 0:
                            args_end = i
                            break

                if args_end > func_start:
                    args_str = expr[func_start + 1:args_end]
                    suffix = expr[args_end + 1:].strip()

                    # Parse and translate arguments
                    raw_args = cls._parse_arguments(args_str)
                    translated_args = [cls._translate_expression(arg) for arg in raw_args]

                    # Translate the function
                    func_name_upper = func_name.upper()
                    result = cls._translate_function(func_name, func_name_upper, translated_args)

                    # Handle any suffix (like " + 1" or other operators)
                    if suffix:
                        result = result + " " + cls._translate_expression(suffix)

                    return result

        # Not a function call - convert as a simple value
        return cls._convert_value_to_sql(expr)

    @classmethod
    def _translate_function(cls, func_name: str, func_name_upper: str, args: list[str]) -> str:
        """
        Translate a function call to SQL.
        """
        # Handle Visitran-specific functions
        if func_name_upper == "DUPLICATE":
            if len(args) >= 1:
                return args[0]
            return f"{func_name}()"

        elif func_name_upper == "ISIN":
            if len(args) >= 2:
                col = args[0]
                values = ", ".join(args[1:])
                return f"{col} IN ({values})"
            return f"{func_name}({', '.join(args)})"

        elif func_name_upper == "NOTIN":
            if len(args) >= 2:
                col = args[0]
                values = ", ".join(args[1:])
                return f"{col} NOT IN ({values})"
            return f"{func_name}({', '.join(args)})"

        elif func_name_upper == "BETWEEN":
            if len(args) >= 3:
                return f"{args[0]} BETWEEN {args[1]} AND {args[2]}"
            return f"{func_name}({', '.join(args)})"

        elif func_name_upper == "DIFFERENCE":
            if len(args) >= 2:
                return f"({args[0]} - {args[1]})"
            return f"{func_name}({', '.join(args)})"

        elif func_name_upper == "CONCATENATE":
            # CONCATENATE -> CONCAT
            return f"CONCAT({', '.join(args)})"

        else:
            # For all other functions, keep as-is with translated arguments
            return f"{func_name}({', '.join(args)})"

    @classmethod
    def _parse_arguments(cls, args_str: str) -> list[str]:
        """
        Parse function arguments, handling nested parentheses and quoted strings.
        """
        args = []
        current_arg = []
        paren_depth = 0
        in_string = False
        string_char = None

        for char in args_str:
            if char in ('"', "'") and not in_string:
                in_string = True
                string_char = char
                current_arg.append(char)
            elif char == string_char and in_string:
                in_string = False
                string_char = None
                current_arg.append(char)
            elif char == '(' and not in_string:
                paren_depth += 1
                current_arg.append(char)
            elif char == ')' and not in_string:
                paren_depth -= 1
                current_arg.append(char)
            elif char == ',' and paren_depth == 0 and not in_string:
                arg = ''.join(current_arg).strip()
                if arg:
                    args.append(arg)
                current_arg = []
            else:
                current_arg.append(char)

        arg = ''.join(current_arg).strip()
        if arg:
            args.append(arg)

        return args


@dataclass
class CTEStep:
    """
    Represents a single CTE step in the query building process.

    Each transformation that modifies the data flow creates a new CTE step,
    allowing transformations to be applied in the exact order specified.

    Attributes:
        name: CTE name (e.g., 'step_1', 'step_2')
        sql: The SQL for this CTE
        source_ref: Reference to use in subsequent steps (the CTE name)
    """
    name: str
    sql: str
    source_ref: str


@dataclass
class JoinedTableInfo:
    """Information about a joined table for column aliasing."""
    schema: str
    table: str
    alias: str  # The reference name to use (alias or table name)


@dataclass
class QueryState:
    """
    Mutable state tracking the current query being built.

    This state is passed through transformation handlers and accumulated
    as each transformation is processed.

    Attributes:
        select_columns: Columns for SELECT clause
        additional_columns: Columns to add alongside base columns (synthesize, combine, window)
        replaced_columns: Columns to replace in-place using SELECT * REPLACE syntax (find_and_replace)
        from_table: Current source table/CTE reference
        from_schema: Schema of source table (empty string if none)
        join_clauses: List of JOIN clause strings
        joined_tables: List of JoinedTableInfo for column aliasing
        where_conditions: List of WHERE conditions
        group_by_columns: Columns for GROUP BY
        having_conditions: HAVING clause conditions
        order_by_specs: ORDER BY specifications
        is_distinct: Whether DISTINCT is applied
        distinct_columns: Specific columns for DISTINCT ON
        cte_steps: Accumulated CTE steps
        step_counter: Counter for generating CTE names
        has_aggregate: Whether aggregation has been applied
        replaces_base_columns: Whether select_columns replace base columns entirely
    """
    select_columns: list[str] = field(default_factory=list)
    additional_columns: list[str] = field(default_factory=list)
    replaced_columns: dict[str, str] = field(default_factory=dict)  # col_name -> replacement_expr
    from_table: str = ""
    from_schema: str = ""
    join_clauses: list[str] = field(default_factory=list)
    joined_tables: list[JoinedTableInfo] = field(default_factory=list)
    where_conditions: list[str] = field(default_factory=list)
    group_by_columns: list[str] = field(default_factory=list)
    having_conditions: list[str] = field(default_factory=list)
    order_by_specs: list[str] = field(default_factory=list)
    is_distinct: bool = False
    distinct_columns: list[str] = field(default_factory=list)
    cte_steps: list[CTEStep] = field(default_factory=list)
    step_counter: int = 0
    has_aggregate: bool = False
    replaces_base_columns: bool = False
    # Track column names accumulated from previous CTEs (synthesize, combine, window columns)
    cte_accumulated_columns: list[str] = field(default_factory=list)
    # Track available columns after UNION (empty means all columns available)
    union_columns: list[str] = field(default_factory=list)

    def get_source_ref(self) -> str:
        """Get the current source table reference (latest CTE or original table)."""
        if self.cte_steps:
            return f'"{self.cte_steps[-1].name}"'
        if self.from_schema:
            return f'"{self.from_schema}"."{self.from_table}"'
        return f'"{self.from_table}"'

    def get_source_table_name(self) -> str:
        """Get just the table name without schema for column qualification."""
        if self.cte_steps:
            return self.cte_steps[-1].name
        return self.from_table

    def create_cte(self, sql: str) -> str:
        """
        Create a new CTE step and return its name.

        This wraps the current SQL state into a CTE and resets state
        for the next transformation.
        """
        self.step_counter += 1
        cte_name = f"step_{self.step_counter}"
        cte = CTEStep(name=cte_name, sql=sql, source_ref=cte_name)
        self.cte_steps.append(cte)

        # Track additional columns from this CTE for later use
        # Extract column names from additional_columns expressions (e.g., 'expr AS "col"' -> 'col')
        for col_expr in self.additional_columns:
            if ' AS "' in col_expr:
                # Extract column name from 'expr AS "col_name"'
                col_name = col_expr.split(' AS "')[-1].rstrip('"')
                if col_name not in self.cte_accumulated_columns:
                    self.cte_accumulated_columns.append(col_name)

        # Reset state for next transformation
        self.select_columns = []
        self.additional_columns = []
        self.replaced_columns = {}
        self.join_clauses = []
        self.joined_tables = []
        self.where_conditions = []
        self.group_by_columns = []
        self.having_conditions = []
        self.is_distinct = False
        self.distinct_columns = []
        self.has_aggregate = False
        self.replaces_base_columns = False

        return cte_name


class TransformationHandler(ABC):
    """
    Abstract base class for transformation SQL handlers.

    Each transformation type (filter, join, synthesize, etc.) has a handler
    that knows how to generate SQL for that transformation.

    This follows the Strategy pattern, allowing different SQL generation
    strategies for different transformation types.
    """

    @property
    @abstractmethod
    def transform_type(self) -> str:
        """The transformation type this handler processes."""
        pass

    @abstractmethod
    def apply(
        self,
        state: QueryState,
        transform: "BaseParser",
        config_parser: "ConfigParser"
    ) -> None:
        """
        Apply this transformation to the query state.

        Args:
            state: Current query state to modify
            transform: The transformation parser with configuration
            config_parser: Parent config parser for context
        """
        pass

    def requires_cte_before(
        self,
        state: QueryState,
        transform: "BaseParser",
        config_parser: "ConfigParser"
    ) -> bool:
        """
        Check if this transformation requires wrapping previous work in a CTE.

        Override in subclasses where order matters. For example, if we have
        a filter after a join, we might need to wrap the join in a CTE first
        if the filter references only the original table.

        Default: False (no CTE needed before this transformation)
        """
        return False


class FilterHandler(TransformationHandler):
    """Handler for filter transformation - generates WHERE clause."""

    @property
    def transform_type(self) -> str:
        return "filter"

    def requires_cte_before(
        self,
        state: QueryState,
        transform: "BaseParser",
        config_parser: "ConfigParser"
    ) -> bool:
        # If we have joins or aggregates already applied, wrap in CTE
        # This ensures filter applies to the joined/aggregated result
        if bool(state.join_clauses) or state.has_aggregate:
            return True

        # Check if any additional_columns contain window functions (OVER clause)
        # SQL doesn't allow filtering on window function results in the same query
        for col in state.additional_columns:
            if " OVER " in col.upper():
                return True

        return False

    def apply(
        self,
        state: QueryState,
        transform: "BaseParser",
        config_parser: "ConfigParser"
    ) -> None:
        from backend.application.config_parser.transformation_parsers.filter_parser import FilterParser

        if not isinstance(transform, FilterParser):
            return

        filter_sql = self._parse_filter_to_sql(transform, state)
        if filter_sql:
            state.where_conditions.append(f"({filter_sql})")

    def _parse_filter_to_sql(self, filter_parser, state: QueryState) -> str:
        """Convert FilterParser conditions to SQL WHERE clause.

        Uses state.get_source_table_name() to correctly qualify columns
        when no explicit table is specified and we're working with a CTE.
        """
        conditions = filter_parser.conditions
        if not conditions:
            return ""

        sql_conditions = []
        # Get current source table for column qualification
        current_source = state.get_source_table_name() if state else None

        for i, cond in enumerate(conditions):
            # Handle LHS
            if cond.lhs_type == "FORMULA" and cond.lhs_expression:
                lhs = f"({cond.lhs_expression})"
            else:
                lhs_col = cond.lhs_column.column_name if cond.lhs_column else ""
                if not lhs_col:
                    continue
                lhs_table = cond.lhs_column.table_name if cond.lhs_column else ""
                if lhs_table:
                    # Check if lhs_table matches original source - if so, use current source
                    # This handles the case where YAML specifies original table but we're in a CTE
                    if current_source and state.cte_steps:
                        # Use CTE name instead of original table name
                        lhs = f'"{current_source}"."{lhs_col}"'
                    elif current_source and not state.cte_steps:
                        # No CTE yet - don't qualify column (avoids wrong table name from YAML)
                        # The column will be resolved from the FROM clause
                        lhs = f'"{lhs_col}"'
                    else:
                        lhs = f'"{lhs_table}"."{lhs_col}"'
                else:
                    lhs = f'"{lhs_col}"'

            operator = cond.operator or "EQ"
            condition_type = cond.condition_type if i > 0 else ""

            # Handle no-RHS operators
            if operator in ("ISNULL", "ISNOTNULL", "NULL", "NOTNULL"):
                sql_op = "IS NULL" if operator in ("ISNULL", "NULL") else "IS NOT NULL"
                condition_sql = f"{lhs} {sql_op}"
            elif operator in ("TRUE", "FALSE"):
                condition_sql = f"{lhs} = {operator}"
            else:
                sql_op = self._get_sql_operator(operator)

                # Handle RHS
                if cond.rhs_type == "COLUMN":
                    rhs_col = cond.rhs_column.column_name if cond.rhs_column else ""
                    rhs_table = cond.rhs_column.table_name if cond.rhs_column else ""
                    if rhs_table:
                        if current_source and state.cte_steps:
                            # Use CTE name instead of original table name
                            rhs = f'"{current_source}"."{rhs_col}"'
                        elif current_source and not state.cte_steps:
                            # No CTE yet - don't qualify column
                            rhs = f'"{rhs_col}"'
                        else:
                            rhs = f'"{rhs_table}"."{rhs_col}"'
                    else:
                        rhs = f'"{rhs_col}"'
                elif cond.rhs_type == "FORMULA" and cond.rhs_expression:
                    rhs = f"({cond.rhs_expression})"
                else:
                    rhs = self._format_sql_value(cond.rhs_value, operator)

                condition_sql = f"{lhs} {sql_op} {rhs}"

            if condition_type and sql_conditions:
                sql_conditions.append(f"{condition_type} {condition_sql}")
            else:
                sql_conditions.append(condition_sql)

        return " ".join(sql_conditions)

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
            value = value[0] if value else ""

        escaped = str(value).replace("'", "''")

        if operator == "CONTAINS":
            return f"'%{escaped}%'"
        elif operator == "STARTSWITH":
            return f"'{escaped}%'"
        elif operator == "ENDSWITH":
            return f"'%{escaped}'"

        return f"'{escaped}'"


class JoinHandler(TransformationHandler):
    """Handler for join transformation - generates JOIN clauses."""

    @property
    def transform_type(self) -> str:
        return "join"

    def requires_cte_before(
        self,
        state: QueryState,
        transform: "BaseParser",
        config_parser: "ConfigParser"
    ) -> bool:
        # If we already have WHERE conditions (filter before join), wrap in CTE
        # This ensures the filter is applied before the join
        return bool(state.where_conditions) or state.has_aggregate

    def apply(
        self,
        state: QueryState,
        transform: "BaseParser",
        config_parser: "ConfigParser"
    ) -> None:
        from backend.application.config_parser.transformation_parsers.join_parser import JoinParsers

        if not isinstance(transform, JoinParsers):
            return

        for join_parser in transform.get_joins():
            join_clause = self._build_single_join(join_parser, state, config_parser)
            if join_clause:
                state.join_clauses.append(join_clause)

                # Track joined table info for column aliasing
                rhs_schema = join_parser.rhs_schema_name or ""
                rhs_table = join_parser.rhs_table_name
                alias = join_parser.alias_name or rhs_table
                state.joined_tables.append(JoinedTableInfo(
                    schema=rhs_schema,
                    table=rhs_table,
                    alias=alias
                ))

    def _build_single_join(
        self,
        join_parser,
        state: QueryState,
        config_parser: "ConfigParser"
    ) -> str:
        """Build a single JOIN clause.

        Uses state.get_source_table_name() to correctly reference the current
        source (which may be a CTE if previous transformations created one).
        """
        join_type = (join_parser.join_type or "INNER").upper()

        rhs_schema = join_parser.rhs_schema_name
        rhs_table = join_parser.rhs_table_name
        alias = join_parser.alias_name

        if rhs_schema:
            table_ref = f'"{rhs_schema}"."{rhs_table}"'
        else:
            table_ref = f'"{rhs_table}"'

        if alias:
            table_ref += f' AS "{alias}"'

        # Build join condition - use current source table reference
        # This ensures we reference the CTE if one was created
        lhs_col = join_parser.lhs_column_name
        rhs_col = join_parser.rhs_column_name
        filter_parser = join_parser.join_filter

        # Get the table references for JOIN condition qualification
        lhs_table = state.get_source_table_name()
        rhs_table_ref = alias or rhs_table

        if lhs_col and rhs_col:
            operator = join_parser.operator or "="
            lhs_ref = f'"{lhs_table}"."{lhs_col}"'
            rhs_ref = f'"{rhs_table_ref}"."{rhs_col}"'

            join_condition = f"{lhs_ref} {operator} {rhs_ref}"

            if filter_parser and filter_parser.conditions:
                additional = self._parse_join_filter_to_sql(
                    filter_parser, lhs_table, rhs_table_ref
                )
                if additional:
                    join_condition += f" AND {additional}"
        else:
            if filter_parser and filter_parser.conditions:
                # For criteria-based joins, we need to qualify columns properly:
                # LHS columns -> source table, RHS columns -> joined table
                join_condition = self._parse_join_filter_to_sql(
                    filter_parser, lhs_table, rhs_table_ref
                )
            else:
                join_condition = "1=1"

        return f"{join_type} JOIN {table_ref} ON {join_condition}"

    def _parse_join_filter_to_sql(
        self,
        filter_parser,
        lhs_table: str,
        rhs_table: str
    ) -> str:
        """Parse filter conditions for JOIN ON clause with proper table qualification.

        For JOIN conditions:
        - LHS columns are qualified with the source table (or CTE)
        - RHS columns are qualified with the joined table

        Args:
            filter_parser: Filter parser with conditions
            lhs_table: Table name for LHS column qualification
            rhs_table: Table name for RHS column qualification

        Returns:
            SQL string for JOIN ON condition
        """
        conditions = filter_parser.conditions
        if not conditions:
            return ""

        sql_conditions = []
        filter_handler = FilterHandler()

        for i, cond in enumerate(conditions):
            # Handle LHS - qualify with source table
            if cond.lhs_type == "FORMULA" and cond.lhs_expression:
                lhs = f"({cond.lhs_expression})"
            else:
                lhs_col = cond.lhs_column.column_name if cond.lhs_column else ""
                if not lhs_col:
                    continue
                # Always qualify LHS with source table for JOIN conditions
                lhs = f'"{lhs_table}"."{lhs_col}"'

            operator = cond.operator or "EQ"
            condition_type = cond.condition_type if i > 0 else ""

            # Handle no-RHS operators
            if operator in ("ISNULL", "ISNOTNULL", "NULL", "NOTNULL"):
                sql_op = "IS NULL" if operator in ("ISNULL", "NULL") else "IS NOT NULL"
                condition_sql = f"{lhs} {sql_op}"
            elif operator in ("TRUE", "FALSE"):
                condition_sql = f"{lhs} = {operator}"
            else:
                sql_op = filter_handler._get_sql_operator(operator)

                # Handle RHS - qualify with joined table
                if cond.rhs_type == "COLUMN":
                    rhs_col = cond.rhs_column.column_name if cond.rhs_column else ""
                    # Always qualify RHS with joined table for JOIN conditions
                    rhs = f'"{rhs_table}"."{rhs_col}"'
                elif cond.rhs_type == "FORMULA" and cond.rhs_expression:
                    rhs = f"({cond.rhs_expression})"
                else:
                    rhs = filter_handler._format_sql_value(cond.rhs_value, operator)

                condition_sql = f"{lhs} {sql_op} {rhs}"

            if condition_type and sql_conditions:
                sql_conditions.append(f"{condition_type} {condition_sql}")
            else:
                sql_conditions.append(condition_sql)

        return " ".join(sql_conditions)


class SynthesizeHandler(TransformationHandler):
    """Handler for synthesize transformation - generates computed columns.

    Synthesize adds NEW columns alongside existing ones (additive).
    """

    @property
    def transform_type(self) -> str:
        return "synthesize"

    def apply(
        self,
        state: QueryState,
        transform: "BaseParser",
        config_parser: "ConfigParser"
    ) -> None:
        from backend.application.config_parser.transformation_parsers.synthesize_parser import SynthesizeParser

        if not isinstance(transform, SynthesizeParser):
            return

        for col_parser in transform.columns:
            col_name = col_parser.column_name
            formula = col_parser.formula

            if formula:
                # Translate Visitran formula functions to SQL
                translated_formula = FormulaTranslator.translate(str(formula))
                # Use additional_columns - these are added alongside base columns
                state.additional_columns.append(f'({translated_formula}) AS "{col_name}"')


class CombineColumnsHandler(TransformationHandler):
    """Handler for combine_columns transformation - generates CONCAT expressions.

    Combine adds NEW columns alongside existing ones (additive).
    """

    @property
    def transform_type(self) -> str:
        return "combine_columns"

    def apply(
        self,
        state: QueryState,
        transform: "BaseParser",
        config_parser: "ConfigParser"
    ) -> None:
        from backend.application.config_parser.transformation_parsers.combine_parser import CombineColumnParser

        if not isinstance(transform, CombineColumnParser):
            return

        for combine_col in transform.columns:
            parts = []
            for value in combine_col.values:
                if value.type == "column":
                    parts.append(f'CAST("{value.value}" AS VARCHAR)')
                else:
                    escaped = str(value.value).replace("'", "''")
                    parts.append(f"'{escaped}'")

            if parts:
                concat_expr = " || ".join(parts)
                col_name = combine_col.column_name
                # Use additional_columns - these are added alongside base columns
                state.additional_columns.append(f'({concat_expr}) AS "{col_name}"')


class RenameHandler(TransformationHandler):
    """Handler for rename_column transformation - generates column aliases."""

    @property
    def transform_type(self) -> str:
        return "rename_column"

    def apply(
        self,
        state: QueryState,
        transform: "BaseParser",
        config_parser: "ConfigParser"
    ) -> None:
        from backend.application.config_parser.transformation_parsers.rename_parser import RenameParsers

        if not isinstance(transform, RenameParsers):
            return

        for rename_parser in transform.get_rename_parsers():
            old_name = rename_parser.old_name
            new_name = rename_parser.new_name
            if old_name and new_name:
                state.select_columns.append(f'"{old_name}" AS "{new_name}"')


class FindAndReplaceHandler(TransformationHandler):
    """Handler for find_and_replace transformation.

    Find and replace modifies existing columns in-place. Uses SELECT * REPLACE
    syntax (supported by DuckDB, BigQuery) to replace columns without duplicates.
    """

    @property
    def transform_type(self) -> str:
        return "find_and_replace"

    def requires_cte_before(
        self,
        state: QueryState,
        transform: "BaseParser",
        config_parser: "ConfigParser"
    ) -> bool:
        # If we have joins, filters, or other complex state, wrap in CTE first
        # This ensures find_and_replace operates on the complete result
        return bool(state.join_clauses) or bool(state.where_conditions) or state.has_aggregate or bool(state.additional_columns)

    def apply(
        self,
        state: QueryState,
        transform: "BaseParser",
        config_parser: "ConfigParser"
    ) -> None:
        from backend.application.config_parser.transformation_parsers.find_and_replace_parser import FindAndReplaceParser

        if not isinstance(transform, FindAndReplaceParser):
            return

        for col_group in transform.columns:
            col_names = col_group.columns or []
            for col_name in col_names:
                # Start with column reference, or existing replacement if already processed
                expr = state.replaced_columns.get(col_name, f'"{col_name}"')

                for operation in col_group.operations:
                    find_val = str(operation.find_value or "").replace("'", "''")
                    replace_val = str(operation.replace_value or "").replace("'", "''")
                    match_type = (operation.match_type or "exact").upper()

                    # Handle different match types
                    if match_type == "REGEX":
                        expr = f"REGEXP_REPLACE({expr}, '{find_val}', '{replace_val}')"
                    else:
                        # TEXT, EXACT, or default - use REPLACE
                        expr = f"REPLACE({expr}, '{find_val}', '{replace_val}')"

                # Store in replaced_columns - uses SELECT * REPLACE syntax
                state.replaced_columns[col_name] = expr


class WindowHandler(TransformationHandler):
    """Handler for window transformation - generates window functions."""

    @property
    def transform_type(self) -> str:
        return "window"

    def requires_cte_before(
        self,
        state: QueryState,
        transform: "BaseParser",
        config_parser: "ConfigParser"
    ) -> bool:
        # Window functions should wrap previous aggregations in CTE
        # This ensures window operates on the aggregated result
        # Also wrap if there are explicit select_columns or replaced_columns
        return (
            bool(state.group_by_columns) or
            bool(state.select_columns) or
            bool(state.replaced_columns)
        )

    def apply(
        self,
        state: QueryState,
        transform: "BaseParser",
        config_parser: "ConfigParser"
    ) -> None:
        from backend.application.config_parser.transformation_parsers.window_parser import WindowParser

        if not isinstance(transform, WindowParser):
            return

        for col_parser in transform.columns:
            col_name = col_parser.column_name
            window_func = col_parser.window_function.upper()
            partition_by = col_parser.partition_by
            order_by = col_parser.order_by
            agg_column = col_parser.agg_column

            # Build window function expression
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

            # Build frame specification
            frame_spec = self._build_frame_spec(col_parser)
            if frame_spec:
                over_parts.append(frame_spec)

            over_clause = " ".join(over_parts)
            # Use additional_columns - window functions add columns alongside base columns
            state.additional_columns.append(f'{func_expr} OVER ({over_clause}) AS "{col_name}"')

    def _build_frame_spec(self, col_parser) -> str:
        """Build window frame specification."""
        preceding = col_parser.preceding
        following = col_parser.following

        if preceding is None and following is None:
            return ""

        if preceding == "unbounded":
            start = "UNBOUNDED PRECEDING"
        elif preceding == 0:
            start = "CURRENT ROW"
        elif isinstance(preceding, int):
            start = f"{preceding} PRECEDING"
        else:
            start = "UNBOUNDED PRECEDING"

        if following == "unbounded":
            end = "UNBOUNDED FOLLOWING"
        elif following == 0:
            end = "CURRENT ROW"
        elif isinstance(following, int):
            end = f"{following} FOLLOWING"
        else:
            end = "CURRENT ROW"

        return f"ROWS BETWEEN {start} AND {end}"


class GroupsAndAggregationHandler(TransformationHandler):
    """Handler for groups_and_aggregation transformation."""

    @property
    def transform_type(self) -> str:
        return "groups_and_aggregation"

    def requires_cte_before(
        self,
        state: QueryState,
        transform: "BaseParser",
        config_parser: "ConfigParser"
    ) -> bool:
        # CTE is needed if we have previous transformations that add/modify columns
        # This includes:
        # - select_columns: from rename, previous aggregation
        # - additional_columns: from synthesize, combine, window
        # - replaced_columns: from find_and_replace
        # - join_clauses: joins need to be wrapped before aggregation
        # - where_conditions: filters that need to be preserved
        return (
            bool(state.select_columns) or
            bool(state.additional_columns) or
            bool(state.replaced_columns) or
            bool(state.join_clauses) or
            bool(state.where_conditions)
        )

    def apply(
        self,
        state: QueryState,
        transform: "BaseParser",
        config_parser: "ConfigParser"
    ) -> None:
        from backend.application.config_parser.transformation_parsers.groups_and_aggregation_parser import (
            GroupsAndAggregationParser
        )

        if not isinstance(transform, GroupsAndAggregationParser):
            return

        state.has_aggregate = True

        # Add group columns
        for group_col in transform.group_columns:
            state.group_by_columns.append(f'"{group_col}"')
            state.select_columns.append(f'"{group_col}"')

        # Add aggregate columns
        for agg_col in transform.aggregate_columns:
            if agg_col.is_formula_aggregate:
                expr = agg_col.expression
                alias = agg_col.alias
                state.select_columns.append(f'({expr}) AS "{alias}"')
            else:
                func = agg_col.function.upper()
                col = agg_col.column
                alias = agg_col.alias or f"{func}_{col}"

                if func == "COUNT" and col == "*":
                    state.select_columns.append(f'COUNT(*) AS "{alias}"')
                else:
                    state.select_columns.append(f'{func}("{col}") AS "{alias}"')

        # Handle HAVING clause
        having_parser = transform.having
        if having_parser:
            filter_handler = FilterHandler()
            having_sql = filter_handler._parse_filter_to_sql(having_parser, state)
            if having_sql:
                state.having_conditions.append(having_sql)


class DistinctHandler(TransformationHandler):
    """Handler for distinct transformation."""

    @property
    def transform_type(self) -> str:
        return "distinct"

    def apply(
        self,
        state: QueryState,
        transform: "BaseParser",
        config_parser: "ConfigParser"
    ) -> None:
        from backend.application.config_parser.transformation_parsers.distinct_parser import DistinctParser

        if not isinstance(transform, DistinctParser):
            return

        state.is_distinct = True
        distinct_cols = transform.columns
        if distinct_cols:
            state.distinct_columns = distinct_cols


class UnionHandler(TransformationHandler):
    """Handler for union transformation - requires special query structure."""

    @property
    def transform_type(self) -> str:
        return "union"

    def apply(
        self,
        state: QueryState,
        transform: "BaseParser",
        config_parser: "ConfigParser"
    ) -> None:
        # Union is handled specially in SQLQueryBuilder
        # This handler is for detection purposes
        pass


class PivotHandler(TransformationHandler):
    """
    Handler for pivot transformation - generates PIVOT SQL.

    Pivot transforms row values into column headers, aggregating values.
    This is a complex transformation with database-specific implementations:
    - DuckDB, Snowflake, BigQuery, Databricks: Support native PIVOT syntax
    - PostgreSQL: Uses crosstab() from tablefunc extension
    - Generic fallback: GROUP BY with aggregation (simplified)

    Legacy Ibis behavior uses .pivot_wider() which works dynamically at
    execution time. For SQL generation, we use a grouped aggregate approach
    as a cross-database compatible fallback.
    """

    @property
    def transform_type(self) -> str:
        return "pivot"

    def requires_cte_before(
        self,
        state: QueryState,
        transform: "BaseParser",
        config_parser: "ConfigParser"
    ) -> bool:
        # Pivot replaces the entire query structure, wrap previous work in CTE
        # This includes any previous transformations
        return (
            bool(state.select_columns) or
            bool(state.additional_columns) or
            bool(state.replaced_columns) or
            bool(state.join_clauses) or
            bool(state.where_conditions)
        )

    def apply(
        self,
        state: QueryState,
        transform: "BaseParser",
        config_parser: "ConfigParser"
    ) -> None:
        from backend.application.config_parser.transformation_parsers.pivot_parser import PivotParser

        if not isinstance(transform, PivotParser):
            return

        # Pivot replaces base columns with group + aggregate columns
        state.replaces_base_columns = True
        state.has_aggregate = True

        # Get pivot configuration
        row_col = transform.to_rows  # id_cols - becomes GROUP BY
        col_col = transform.to_column_names  # names_from - column to pivot
        value_col = transform.values_from  # values_from - column to aggregate
        aggregator = transform.aggregator or "SUM"

        # Add group by columns (row identifier + pivot column)
        if row_col:
            state.group_by_columns.append(f'"{row_col}"')
            state.select_columns.append(f'"{row_col}"')

        if col_col:
            state.group_by_columns.append(f'"{col_col}"')
            state.select_columns.append(f'"{col_col}"')

        # Add aggregate column
        if value_col:
            agg_func = aggregator.upper()
            alias = f"{agg_func}_{value_col}"
            state.select_columns.append(f'{agg_func}("{value_col}") AS "{alias}"')


class SortHandler(TransformationHandler):
    """
    Handler for sort transformation - generates ORDER BY clause.

    While sorting is typically handled via presentation_parser.sort,
    this handler supports sort as an explicit transformation type.
    """

    @property
    def transform_type(self) -> str:
        return "sort"

    def apply(
        self,
        state: QueryState,
        transform: "BaseParser",
        config_parser: "ConfigParser"
    ) -> None:
        # Sort is typically handled in _build_order_by from presentation
        # This handler is for explicit sort transformations if needed
        pass


class SQLQueryBuilder:
    """
    Builds SQL queries respecting transformation order using CTEs.

    This is the main entry point for SQL generation. It coordinates
    transformation handlers and manages CTE generation to ensure
    transformations are applied in the exact order specified.

    The builder uses a SQLDialect abstraction to handle database-specific
    SQL syntax differences (SOLID: Dependency Inversion Principle).

    Usage:
        builder = SQLQueryBuilder(config_parser)
        sql = builder.build()

        # With specific dialect:
        builder = SQLQueryBuilder(config_parser, dialect="bigquery")
        sql = builder.build()
    """

    # Registry of transformation handlers
    _handlers: dict[str, TransformationHandler] = {}

    @classmethod
    def register_handler(cls, handler: TransformationHandler) -> None:
        """Register a transformation handler."""
        cls._handlers[handler.transform_type] = handler

    @classmethod
    def _ensure_handlers_registered(cls) -> None:
        """Ensure all handlers are registered."""
        if cls._handlers:
            return

        cls.register_handler(FilterHandler())
        cls.register_handler(JoinHandler())
        cls.register_handler(SynthesizeHandler())
        cls.register_handler(CombineColumnsHandler())
        cls.register_handler(RenameHandler())
        cls.register_handler(FindAndReplaceHandler())
        cls.register_handler(WindowHandler())
        cls.register_handler(GroupsAndAggregationHandler())
        cls.register_handler(DistinctHandler())
        cls.register_handler(UnionHandler())
        cls.register_handler(PivotHandler())
        cls.register_handler(SortHandler())

    def __init__(
        self,
        config_parser: "ConfigParser",
        dialect: Optional[str] = None
    ) -> None:
        """
        Initialize the SQL query builder.

        Args:
            config_parser: The model configuration parser
            dialect: Database dialect name (e.g., 'postgres', 'bigquery').
                    If None, uses dialect from config_parser or defaults to PostgreSQL.
        """
        from backend.application.config_parser.sql_dialect import SQLDialectFactory

        self._config_parser = config_parser
        self._ensure_handlers_registered()

        # Get dialect - check config_parser first, then parameter, then default
        dialect_name = dialect or getattr(config_parser, '_dialect', None)
        self._dialect = SQLDialectFactory.get_dialect(dialect_name)

    def build(self) -> str:
        """
        Build the complete SQL query respecting transformation order.

        Returns:
            SQL string with CTEs if needed, or simple query if not
        """
        transforms = self._config_parser.transform_parser.get_transforms()

        # Find union transform and its position
        union_transform = None
        union_index = -1
        for idx, transform in enumerate(transforms):
            t_type = getattr(transform, 'transform_type', None) or transform.get("transformation_type", "")
            if t_type == "union":
                union_transform = transform
                union_index = idx
                break

        # Initialize query state
        state = QueryState(
            from_table=self._config_parser.source_table_name,
            from_schema=self._config_parser.source_schema_name
        )

        if union_transform:
            # Process transformations BEFORE union
            for transform in transforms[:union_index]:
                t_type = getattr(transform, 'transform_type', None) or transform.get("transformation_type", "")
                handler = self._handlers.get(t_type)
                if not handler:
                    continue
                if handler.requires_cte_before(state, transform, self._config_parser):
                    self._flush_to_cte(state)
                handler.apply(state, transform, self._config_parser)

            # Flush any pending work before union
            self._flush_to_cte(state)

            # Build union SQL and add as CTE
            # Pass the current source reference (may be a CTE from previous transforms)
            current_source_ref = state.get_source_ref()
            union_sql, union_columns = self._build_union_sql(union_transform, source_ref=current_source_ref)
            state.step_counter += 1
            union_cte_name = f"step_{state.step_counter}"
            union_cte = CTEStep(name=union_cte_name, sql=union_sql, source_ref=union_cte_name)
            state.cte_steps.append(union_cte)

            # Reset state to use union CTE as source
            state.from_table = union_cte_name
            state.from_schema = ""
            state.select_columns = []
            state.additional_columns = []
            state.replaced_columns = {}
            state.where_conditions = []
            state.join_clauses = []
            state.joined_tables = []
            state.group_by_columns = []
            state.having_conditions = []
            state.order_by_specs = []
            state.is_distinct = False
            state.distinct_columns = []
            state.has_aggregate = False
            state.replaces_base_columns = False
            state.union_columns = union_columns  # Track columns available after UNION

            # Process transformations AFTER union
            for transform in transforms[union_index + 1:]:
                t_type = getattr(transform, 'transform_type', None) or transform.get("transformation_type", "")
                handler = self._handlers.get(t_type)
                if not handler:
                    continue
                if handler.requires_cte_before(state, transform, self._config_parser):
                    self._flush_to_cte(state)
                handler.apply(state, transform, self._config_parser)

            # Build final SQL
            return self._build_final_sql(state, transforms[union_index + 1:])

        # No union - process all transformations normally
        for transform in transforms:
            t_type = getattr(transform, 'transform_type', None) or transform.get("transformation_type", "")

            handler = self._handlers.get(t_type)
            if not handler:
                continue

            # Check if we need to wrap current work in a CTE first
            if handler.requires_cte_before(state, transform, self._config_parser):
                self._flush_to_cte(state)

            # Apply the transformation
            handler.apply(state, transform, self._config_parser)

        # Build final SQL
        return self._build_final_sql(state, transforms)

    def _find_union_transform(self, transforms: list) -> Optional["BaseParser"]:
        """Find union transformation if present."""
        for transform in transforms:
            t_type = getattr(transform, 'transform_type', None) or transform.get("transformation_type", "")
            if t_type == "union":
                return transform
        return None

    def _flush_to_cte(self, state: QueryState) -> None:
        """
        Flush current state to a CTE.

        This wraps all accumulated SQL components into a CTE step,
        allowing subsequent transformations to build on this result.

        When JOINs are present, we must use explicit column aliases to avoid
        duplicate column names (e.g., both tables having 'id' column).
        """
        has_content = (
            state.select_columns or
            state.where_conditions or
            state.join_clauses or
            state.additional_columns or
            state.replaced_columns or
            state.group_by_columns
        )
        if not has_content:
            # Nothing to flush
            return

        # When there are JOINs and no explicit columns, we need to build
        # explicit column list with aliases to avoid duplicate column names
        if state.join_clauses and not state.select_columns:
            source_table = state.get_source_table_name()
            state.select_columns = [f'"{source_table}".*']
            # Add aliased columns from joined tables
            join_columns = self._build_join_columns_for_cte(state)
            state.select_columns.extend(join_columns)

        sql = self._build_sql_from_state(state)
        state.create_cte(sql)

    def _build_join_columns_for_cte(self, state: QueryState) -> list[str]:
        """
        Build aliased columns for joined tables to avoid duplicate column names.

        Uses the joined_tables info tracked in state to generate column aliases
        following the legacy rname pattern: table.col AS table_col
        """
        columns = []

        for joined_table in state.joined_tables:
            table_ref = joined_table.alias
            schema = joined_table.schema
            table = joined_table.table

            # Try to get column info from config_parser
            join_columns = self._config_parser.get_join_table_columns(schema, table)

            if join_columns:
                # Generate explicit aliases: table.col AS table_col
                for col in join_columns:
                    columns.append(f'"{table_ref}"."{col}" AS "{table_ref}_{col}"')
            else:
                # No column info available - use table.* but this may cause issues
                # with duplicate columns. Log a warning in production.
                columns.append(f'"{table_ref}".*')

        return columns

    def _build_sql_from_state(self, state: QueryState) -> str:
        """Build SQL string from current state."""
        # SELECT clause
        # Combine base columns with additional columns (synthesize, combine, window)
        all_columns = []

        if state.select_columns:
            all_columns.extend(state.select_columns)
        elif state.additional_columns or state.replaced_columns:
            # No explicit columns but have additional/replaced - need base columns
            all_columns.append("*")

        if state.additional_columns:
            all_columns.extend(state.additional_columns)

        if all_columns:
            select_clause = ", ".join(all_columns)
        else:
            select_clause = "*"

        # Handle replaced columns - PostgreSQL-compatible approach
        # Instead of SELECT * REPLACE (DuckDB-specific), we build explicit column list
        if state.replaced_columns:
            select_clause = self._build_select_with_replacements(state)

        # DISTINCT handling
        distinct_prefix = ""
        if state.is_distinct:
            if state.distinct_columns:
                cols = ", ".join([f'"{col}"' for col in state.distinct_columns])
                distinct_prefix = f"DISTINCT ON ({cols}) "
            else:
                distinct_prefix = "DISTINCT "

        # FROM clause
        from_clause = state.get_source_ref()

        # JOIN clauses
        if state.join_clauses:
            from_clause += " " + " ".join(state.join_clauses)

        # Build SELECT
        sql = f"SELECT {distinct_prefix}{select_clause} FROM {from_clause}"

        # WHERE clause
        if state.where_conditions:
            sql += f" WHERE {' AND '.join(state.where_conditions)}"

        # GROUP BY clause
        if state.group_by_columns:
            sql += f" GROUP BY {', '.join(state.group_by_columns)}"

        # HAVING clause
        if state.having_conditions:
            sql += f" HAVING {' AND '.join(state.having_conditions)}"

        return sql

    def _build_select_with_replacements(self, state: QueryState) -> str:
        """
        Build SELECT clause with column replacements using database-specific dialect.

        Uses the SQLDialect abstraction to generate appropriate SQL:
        - For DuckDB/BigQuery: Can use SELECT * REPLACE syntax when columns unknown
        - For PostgreSQL/Snowflake/etc: Uses explicit column lists

        Args:
            state: Current query state with replaced_columns

        Returns:
            SELECT clause string
        """
        # Get source table columns and other known columns
        source_columns = self._config_parser.get_source_table_columns()
        join_columns = self._get_join_column_names()
        cte_columns = state.cte_accumulated_columns if state else []

        # If we have source column metadata, use explicit column list (works on all DBs)
        if source_columns:
            return self._dialect.build_select_with_replacements(
                source_columns=source_columns,
                replaced_columns=state.replaced_columns,
                additional_columns=state.additional_columns,
                join_columns=join_columns,
                cte_columns=cte_columns,
            )
        else:
            # Fallback: No source column metadata available
            # Use dialect-specific fallback behavior
            return self._dialect.build_select_with_replacements_fallback(
                replaced_columns=state.replaced_columns,
                additional_columns=state.additional_columns,
            )

    def _get_join_column_names(self) -> list[str]:
        """
        Get aliased column names from joined tables.

        Returns:
            List of column names in 'table_col' format
        """
        columns = []
        for joined_table in self._get_joined_table_info():
            schema = joined_table.get("schema", "")
            table = joined_table.get("table", "")
            alias = joined_table.get("alias", table)

            join_columns = self._config_parser.get_join_table_columns(schema, table)
            for col in join_columns:
                columns.append(f"{alias}_{col}")

        return columns

    def _get_all_known_columns(self, state: Optional[QueryState] = None) -> list[str]:
        """
        Get all known column names from source, joined tables, and CTE-added columns.

        Args:
            state: Optional query state to get CTE-accumulated columns

        Returns:
            List of column names, or empty list if metadata not available
        """
        columns = []

        # Get source table columns
        source_columns = self._config_parser.get_source_table_columns()
        columns.extend(source_columns)

        # Get joined table columns (with aliases)
        for joined_table in self._get_joined_table_info():
            schema = joined_table.get("schema", "")
            table = joined_table.get("table", "")
            alias = joined_table.get("alias", table)

            join_columns = self._config_parser.get_join_table_columns(schema, table)
            for col in join_columns:
                # Use aliased column name (table_col format)
                columns.append(f"{alias}_{col}")

        # Add columns accumulated from previous CTEs (synthesize, combine, window)
        if state and state.cte_accumulated_columns:
            for col in state.cte_accumulated_columns:
                if col not in columns:
                    columns.append(col)

        return columns

    def _get_joined_table_info(self) -> list[dict]:
        """
        Extract joined table information from transformations.

        Returns:
            List of dicts with schema, table, alias for each joined table
        """
        from backend.application.config_parser.transformation_parsers.join_parser import JoinParsers

        joined_tables = []

        for transform in self._config_parser.transform_parser.get_transforms():
            if isinstance(transform, JoinParsers):
                for join_parser in transform.get_joins():
                    joined_tables.append({
                        "schema": join_parser.rhs_schema_name or "",
                        "table": join_parser.rhs_table_name,
                        "alias": join_parser.alias_name or join_parser.rhs_table_name,
                    })

        return joined_tables

    def _build_final_sql(self, state: QueryState, transforms: list) -> str:
        """
        Build the final SQL including all CTEs.

        This combines all CTE steps with the final SELECT statement.

        Column selection logic:
        - Aggregation: use select_columns (group + agg columns)
        - Additive transforms (synthesize/combine/window): base columns + additional_columns
        - No transforms: base columns from presentation
        """
        # Build the final query from remaining state
        # Handle base columns if no explicit columns were added
        if not state.select_columns and not state.has_aggregate:
            # No explicit replacement columns - use base columns
            # additional_columns will be added by _build_sql_from_state
            if not state.additional_columns:
                # No additional columns either - get from presentation
                state.select_columns = self._build_base_columns(state, transforms)

        # Handle JOIN columns if joins are present
        if state.join_clauses and not state.has_aggregate:
            join_columns = self._build_join_columns(transforms)
            # If we're using *, we need explicit columns for joins
            if state.select_columns == ["*"] or not state.select_columns:
                source_table = state.get_source_table_name()
                state.select_columns = [f'"{source_table}".*'] + join_columns
            else:
                state.select_columns.extend(join_columns)

        # Add ORDER BY from presentation
        order_by_clause = self._build_order_by(state, transforms)

        final_sql = self._build_sql_from_state(state)

        if order_by_clause:
            final_sql += f" ORDER BY {order_by_clause}"

        # If we have CTEs, wrap everything
        if state.cte_steps:
            cte_parts = [f'"{cte.name}" AS ({cte.sql})' for cte in state.cte_steps]
            return f"WITH {', '.join(cte_parts)} {final_sql}"

        return final_sql

    def _build_base_columns(self, state: QueryState, transforms: list) -> list[str]:
        """Build base SELECT columns from presentation parser."""
        hidden_columns = self._config_parser.presentation_parser.hidden_columns

        if hidden_columns is None or hidden_columns == ["*"] or not hidden_columns:
            return ["*"]

        return [f'"{col}"' for col in hidden_columns]

    def _build_join_columns(self, transforms: list) -> list[str]:
        """
        Build SELECT columns for joined tables with legacy-compatible aliasing.

        Legacy Ibis uses rname='{table}_{name}' pattern to prefix all columns
        from joined tables, avoiding name conflicts.
        """
        from backend.application.config_parser.transformation_parsers.join_parser import JoinParsers

        columns = []
        for transform in transforms:
            t_type = getattr(transform, 'transform_type', None) or transform.get("transformation_type", "")
            if t_type != "join" or not isinstance(transform, JoinParsers):
                continue

            for join_parser in transform.get_joins():
                alias = join_parser.alias_name
                rhs_table = join_parser.rhs_table_name
                rhs_schema = join_parser.rhs_schema_name
                table_ref = alias or rhs_table

                # Get column info
                join_columns = self._config_parser.get_join_table_columns(rhs_schema, rhs_table)

                if join_columns:
                    # Generate explicit aliases: table.col AS table_col
                    for col in join_columns:
                        columns.append(f'"{table_ref}"."{col}" AS "{table_ref}_{col}"')
                else:
                    # Fallback to table.*
                    columns.append(f'"{table_ref}".*')

        return columns

    def _build_order_by(self, state: QueryState, transforms: list) -> str:
        """Build ORDER BY clause from presentation parser.

        When there are CTEs or JOINs, columns must be qualified to avoid
        ambiguity (e.g., when both tables have an 'id' column).

        However, aggregate aliases (columns computed in SELECT with AS)
        should NOT be qualified as they don't exist in the source/CTE.

        After UNION, only columns in the UNION result are available.
        """
        sort_specs = self._config_parser.presentation_parser.sort

        if not sort_specs:
            return ""

        # Qualify columns when:
        # 1. There are CTEs (the CTE may contain joins with duplicate column names)
        # 2. There are JOINs in current state
        needs_qualification = bool(state.cte_steps) or bool(state.join_clauses)
        source_table = state.get_source_table_name() if needs_qualification else None

        # Extract aggregate aliases from select_columns (e.g., 'SUM(col) AS "alias"')
        aggregate_aliases = set()
        for sel_col in state.select_columns:
            # Look for 'AS "alias"' pattern
            if ' AS "' in sel_col:
                alias_match = sel_col.split(' AS "')
                if len(alias_match) > 1:
                    alias = alias_match[-1].rstrip('"')
                    aggregate_aliases.add(alias)

        # Get available columns after UNION (if any)
        available_columns = set(state.union_columns) if state.union_columns else None

        order_parts = []
        for sort_spec in sort_specs:
            column = sort_spec.get("column", "")
            # Support both "order_by" (actual YAML format) and "order" (legacy/alternative)
            direction = sort_spec.get("order_by") or sort_spec.get("order", "asc")
            direction = direction.upper() if direction else "ASC"

            if not column:
                continue

            if direction not in ("ASC", "DESC"):
                direction = "ASC"

            # Skip columns that don't exist after UNION
            if available_columns is not None and column not in available_columns:
                continue

            # Don't qualify aggregate aliases - they're computed in SELECT, not in source
            if column in aggregate_aliases:
                order_parts.append(f'"{column}" {direction}')
            elif source_table:
                order_parts.append(f'"{source_table}"."{column}" {direction}')
            else:
                order_parts.append(f'"{column}" {direction}')

        return ", ".join(order_parts)

    def _build_union_sql(self, union_transform, source_ref: str = None) -> tuple[str, list[str]]:
        """Build UNION SQL from union transformation.

        Args:
            union_transform: The union transformation parser
            source_ref: Optional source reference (e.g., CTE name from previous transforms)

        Returns:
            Tuple of (sql_string, column_names) where column_names are the columns
            available after the UNION operation.
        """
        from backend.application.config_parser.transformation_parsers.union_parser import UnionParsers

        if not isinstance(union_transform, UnionParsers):
            return (source_ref or self._get_source_table_ref()), []

        queries = []
        union_columns = []  # Track columns in the UNION result
        ignore_duplicates = union_transform.unions_duplicate

        # Get the source table reference (may be a CTE from previous transforms)
        current_source = source_ref or self._get_source_table_ref()

        if union_transform.is_branch_based():
            # Branch-based union format
            branches = union_transform.get_branch_parsers()

            # Get source table info for comparison
            source_schema = self._config_parser.source_schema_name
            source_table = self._config_parser.source_table_name

            # Check if first branch is the source table (avoid duplicate)
            first_branch_is_source = False
            if branches:
                first_branch = branches[0]
                first_branch_is_source = (
                    first_branch.table == source_table and
                    (first_branch.schema or "") == (source_schema or "")
                )

            # If branches don't include source table, add it first
            if branches and not first_branch_is_source:
                # Get column expressions from first branch to match output schema
                first_branch = branches[0]
                col_exprs = []
                for col_expr in first_branch.get_column_expressions():
                    output_col = col_expr.output_column
                    # For source, just select the column directly
                    col_exprs.append(f'"{output_col}"')
                    # Track the column names available after UNION
                    if output_col not in union_columns:
                        union_columns.append(output_col)

                select_clause = ", ".join(col_exprs) if col_exprs else "*"
                queries.append(f"SELECT {select_clause} FROM {current_source}")

            # Track column names from first branch for UNION result
            if branches:
                first_branch = branches[0]
                for col_expr in first_branch.get_column_expressions():
                    output_col = col_expr.output_column
                    if output_col not in union_columns:
                        union_columns.append(output_col)

            # Add all branch tables
            for branch in branches:
                schema = branch.schema
                table = branch.table

                if schema:
                    table_ref = f'"{schema}"."{table}"'
                else:
                    table_ref = f'"{table}"'

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

                where_clause = ""
                if branch.filters:
                    filter_handler = FilterHandler()
                    dummy_state = QueryState()
                    filter_sql = filter_handler._parse_filter_to_sql(branch.filters, dummy_state)
                    if filter_sql:
                        where_clause = f" WHERE {filter_sql}"

                queries.append(f"SELECT {select_clause} FROM {table_ref}{where_clause}")
        else:
            # Legacy table-based union format
            source_ref = self._get_source_table_ref()
            source_cols = union_transform.column_names

            if source_cols:
                col_list = ", ".join([f'"{col}"' for col in source_cols])
                queries.append(f"SELECT {col_list} FROM {source_ref}")
                # Track column names for legacy format
                union_columns.extend(source_cols)
            else:
                queries.append(f"SELECT * FROM {source_ref}")

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

                    if union_parser.filters:
                        filter_handler = FilterHandler()
                        dummy_state = QueryState()
                        filter_sql = filter_handler._parse_filter_to_sql(union_parser.filters, dummy_state)
                        if filter_sql:
                            query += f" WHERE {filter_sql}"

                    queries.append(query)

        union_keyword = "UNION" if ignore_duplicates else "UNION ALL"
        return f" {union_keyword} ".join(queries), union_columns

    def _get_source_table_ref(self) -> str:
        """Get the fully qualified source table reference."""
        source_schema = self._config_parser.source_schema_name
        source_table = self._config_parser.source_table_name
        if source_schema:
            return f'"{source_schema}"."{source_table}"'
        return f'"{source_table}"'
