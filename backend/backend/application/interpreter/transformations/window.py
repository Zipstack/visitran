from backend.application.config_parser.transformation_parsers.column_parser import ColumnParser
from backend.application.config_parser.transformation_parsers.window_parser import WindowParser
from backend.application.interpreter.constants import TemplateNames
from backend.application.interpreter.transformations.base_transformation import BaseTransformation

# Supported window functions
WINDOW_FUNCTIONS = {
    # Ranking functions (standalone ibis functions - ordering via .over())
    "ROW_NUMBER": "ibis.row_number()",
    "RANK": "ibis.rank()",
    "DENSE_RANK": "ibis.dense_rank()",
    "PERCENT_RANK": "ibis.percent_rank()",
    # Navigation functions
    "LAG": None,  # Requires column and offset: column.lag(offset)
    "LEAD": None,  # Requires column and offset: column.lead(offset)
    "FIRST": None,  # Requires column: column.first()
    "LAST": None,  # Requires column: column.last()
    # Aggregate functions (require agg_column)
    "SUM": "sum",
    "AVG": "mean",  # ibis uses mean() for average
    "COUNT": "count",
    "MIN": "min",
    "MAX": "max",
    "STDDEV": "std",  # ibis uses std() for standard deviation
    "STD": "std",
    "VARIANCE": "var",  # ibis uses var() for variance
    "VAR": "var",
}

# Aggregate window functions that require agg_column
AGGREGATE_WINDOW_FUNCTIONS = {"SUM", "AVG", "COUNT", "MIN", "MAX", "STDDEV", "STD", "VARIANCE", "VAR"}


class WindowTransformation(BaseTransformation):
    """Transformation for window functions.

    Handles window function operations like:
    - ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary)
    - RANK() OVER (ORDER BY score DESC)
    - LAG(amount, 1) OVER (ORDER BY date)
    - SUM(amount) OVER (PARTITION BY customer)
    """

    def __init__(self, parser: WindowParser, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.window_parser: WindowParser = parser

    def add_window_headers(self):
        self.add_headers("import ibis")

    def _build_window_spec(self, column_parser: ColumnParser) -> str:
        """
        Build the ibis window specification string from partition_by, order_by, and frame spec.

        Returns:
            String like: ibis.window(group_by=[source_table.col1], order_by=[source_table.col2.desc()], preceding=2, following=0)
        """
        parts = []

        # Build group_by (partition_by in SQL terms)
        if column_parser.partition_by:
            group_cols = ", ".join(
                f"source_table.{col}" for col in column_parser.partition_by
            )
            parts.append(f"group_by=[{group_cols}]")

        # Build order_by with direction
        if column_parser.order_by:
            order_parts = []
            for order_spec in column_parser.order_by:
                col = order_spec.get("column", "")
                direction = order_spec.get("direction", "ASC").upper()
                if direction == "DESC":
                    order_parts.append(f"source_table.{col}.desc()")
                else:
                    order_parts.append(f"source_table.{col}")
            parts.append(f"order_by=[{', '.join(order_parts)}]")

        # Build frame specification (preceding/following)
        # ibis uses: preceding=None for UNBOUNDED, preceding=n for n rows
        # following=0 for CURRENT ROW, following=None for UNBOUNDED
        if column_parser.has_frame_spec():
            preceding = column_parser.preceding
            following = column_parser.following

            # Handle preceding: "unbounded" -> None, number -> number
            if preceding is not None:
                if preceding == "unbounded":
                    parts.append("preceding=None")
                else:
                    parts.append(f"preceding={preceding}")

            # Handle following: "unbounded" -> None, 0 -> CURRENT ROW, number -> number
            if following is not None:
                if following == "unbounded":
                    parts.append("following=None")
                else:
                    parts.append(f"following={following}")

        if parts:
            return f"ibis.window({', '.join(parts)})"
        return ""

    def _get_first_order_column(self, column_parser: ColumnParser, func_name: str) -> str:
        """Get the first order_by column, raising error if not present."""
        if not column_parser.order_by:
            raise ValueError(f"{func_name} function requires at least one order_by column")
        return column_parser.order_by[0].get("column", "")

    def _build_rank_expr(self, column_parser: ColumnParser, func_name: str) -> str:
        """Build expression for RANK, DENSE_RANK, or PERCENT_RANK functions.

        Note: ibis.rank() and ibis.dense_rank() are standalone functions,
        NOT column methods. The ordering is specified via .over() clause.
        ibis ranking functions are 0-based, so we add 1 for SQL-standard 1-based ranking.
        PERCENT_RANK returns values from 0 to 1 - no adjustment needed.
        """
        # Validate that order_by is present (required for ranking functions)
        if not column_parser.order_by:
            raise ValueError(f"{func_name} function requires at least one order_by column")

        # Use ibis standalone functions (not column methods)
        if func_name == "RANK":
            # Add +1 because ibis returns 0-based ranking, SQL standard is 1-based
            return "(ibis.rank() + 1)"
        elif func_name == "DENSE_RANK":
            return "(ibis.dense_rank() + 1)"
        elif func_name == "PERCENT_RANK":
            # PERCENT_RANK returns 0-1 range, no adjustment needed
            return "ibis.percent_rank()"
        else:
            raise ValueError(f"Unknown ranking function: {func_name}")

    def _build_lag_lead_expr(self, column_parser: ColumnParser, func_name: str) -> str:
        """Build expression for LAG or LEAD functions."""
        agg_col = column_parser.agg_column
        if not agg_col:
            # Fall back to first order column if no agg_column specified
            agg_col = self._get_first_order_column(column_parser, func_name)
        offset = column_parser.get("operation", {}).get("offset", 1)
        method = "lag" if func_name == "LAG" else "lead"
        return f"source_table.{agg_col}.{method}({offset})"

    def _build_first_last_expr(self, column_parser: ColumnParser, func_name: str) -> str:
        """Build expression for FIRST or LAST functions."""
        agg_col = column_parser.agg_column
        if not agg_col:
            raise ValueError(f"{func_name} window function requires an agg_column")
        method = "first" if func_name == "FIRST" else "last"
        return f"source_table.{agg_col}.{method}()"

    def _is_expression(self, agg_col: str) -> bool:
        """Check if agg_col contains operators or parentheses (i.e., is an expression)."""
        return any(op in agg_col for op in ['+', '-', '*', '/', '%', '(', ')'])

    def _parse_expression_to_ibis(self, expr: str) -> str:
        """
        Parse an arithmetic expression and convert column references to Ibis syntax.

        Examples:
            "l_extendedprice*(1-l_discount)" -> "(source_table['l_extendedprice'] * (1 - source_table['l_discount']))"
        """
        import re
        expr = expr.strip()

        # Handle parentheses - check if outer parens wrap the whole expression
        if expr.startswith('(') and expr.endswith(')'):
            depth = 0
            is_outer = True
            for i, c in enumerate(expr):
                if c == '(':
                    depth += 1
                elif c == ')':
                    depth -= 1
                if depth == 0 and i < len(expr) - 1:
                    is_outer = False
                    break
            if is_outer:
                return f"({self._parse_expression_to_ibis(expr[1:-1])})"

        # Find lowest precedence operator (+ or -) not inside parentheses
        depth = 0
        for i in range(len(expr) - 1, -1, -1):
            c = expr[i]
            if c == ')':
                depth += 1
            elif c == '(':
                depth -= 1
            elif depth == 0 and c in ['+', '-'] and i > 0:
                # Check it's not unary
                prev_idx = i - 1
                while prev_idx >= 0 and expr[prev_idx] == ' ':
                    prev_idx -= 1
                if prev_idx >= 0 and expr[prev_idx] not in ['(', '+', '-', '*', '/', '%']:
                    left = expr[:i].strip()
                    right = expr[i+1:].strip()
                    return f"({self._parse_expression_to_ibis(left)} {c} {self._parse_expression_to_ibis(right)})"

        # Find * / % operators
        depth = 0
        for i in range(len(expr) - 1, -1, -1):
            c = expr[i]
            if c == ')':
                depth += 1
            elif c == '(':
                depth -= 1
            elif depth == 0 and c in ['*', '/', '%']:
                left = expr[:i].strip()
                right = expr[i+1:].strip()
                return f"({self._parse_expression_to_ibis(left)} {c} {self._parse_expression_to_ibis(right)})"

        # No operators - check if it's a number or column
        try:
            float(expr)
            return expr  # Number literal
        except ValueError:
            pass

        # It's a column reference
        return f"source_table['{expr}']"

    def _build_aggregate_expr(self, column_parser: ColumnParser, func_name: str) -> str:
        """Build expression for aggregate window functions (SUM, AVG, COUNT, MIN, MAX)."""
        agg_col = column_parser.agg_column
        if not agg_col:
            # COUNT(*) case - count all rows using first available column
            # ibis.literal(1) is an IntegerScalar which doesn't have .count()
            # so we use source_table's first column to count rows
            if func_name == "COUNT":
                return "source_table[source_table.columns[0]].count()"
            raise ValueError(f"{func_name} window function requires an agg_column")
        method = WINDOW_FUNCTIONS.get(func_name)

        # Check if agg_col is an expression (contains operators/parentheses)
        if self._is_expression(agg_col):
            # Parse the expression and convert to Ibis syntax
            ibis_expr = self._parse_expression_to_ibis(agg_col)
            return f"({ibis_expr}).{method}()"
        else:
            # Simple column name
            return f"source_table['{agg_col}'].{method}()"

    def _build_window_function_expr(self, column_parser: ColumnParser) -> str:
        """Build the ibis window function expression."""
        func_name = column_parser.window_function.upper()

        if func_name == "ROW_NUMBER":
            # ibis.row_number() is 0-based, add 1 for SQL-standard 1-based numbering
            return "(ibis.row_number() + 1)"
        elif func_name in ("RANK", "DENSE_RANK", "PERCENT_RANK"):
            return self._build_rank_expr(column_parser, func_name)
        elif func_name in ("LAG", "LEAD"):
            return self._build_lag_lead_expr(column_parser, func_name)
        elif func_name in ("FIRST", "LAST"):
            return self._build_first_last_expr(column_parser, func_name)
        elif func_name in AGGREGATE_WINDOW_FUNCTIONS:
            return self._build_aggregate_expr(column_parser, func_name)
        else:
            raise ValueError(f"Unsupported window function: {func_name}")

    def _build_window_function_statement(self, column_parser: ColumnParser) -> str:
        """
        Build a mutate statement for a window function column.

        Returns:
            String like: .mutate(col_name=ibis.row_number().over(ibis.window(...)))
        """
        col_name = column_parser.column_name
        window_spec = self._build_window_spec(column_parser)
        func_expr = self._build_window_function_expr(column_parser)

        if window_spec:
            func_expr = f"{func_expr}.over({window_spec})"

        return f".mutate({col_name}={func_expr})"

    def _build_result_order_by(self, column_parsers: list[ColumnParser]) -> str:
        """Build an .order_by() statement from window ORDER BY specifications.

        The ORDER BY inside a window function's OVER clause only controls the
        window calculation (e.g. row-number assignment), NOT the result-set
        ordering.  PostgreSQL happens to preserve the window order as a
        side-effect, but Snowflake (and other distributed databases) do not.
        Adding an explicit .order_by() ensures consistent behaviour everywhere.

        Uses the ORDER BY from the last window function that specifies one.
        """
        last_order_by = next(
            (p.order_by for p in reversed(column_parsers) if p.order_by),
            [],
        )

        if not last_order_by:
            return ""

        order_parts = []
        for order_spec in last_order_by:
            col = order_spec.get("column", "")
            direction = order_spec.get("direction", "ASC").upper()
            if direction == "DESC":
                order_parts.append(f'ibis.desc("{col}")')
            else:
                order_parts.append(f'ibis.asc("{col}")')
        return f".order_by([{', '.join(order_parts)}])"

    def _parse_window_transformations(self) -> list[str]:
        """
        Parse window transformations from column parsers and return a list of
        Ibis mutate statements using window functions.

        Raises:
            ValueError: If a column does not have a valid window function.
        """
        self.add_window_headers()
        window_statements = []
        column_parsers: list[ColumnParser] = self.window_parser.columns

        for column_parser in column_parsers:
            col_name = column_parser.column_name

            if not column_parser.has_window_function():
                raise ValueError(
                    f"No window function provided for window column '{col_name}'"
                )
            statement = self._build_window_function_statement(column_parser)
            window_statements.append(statement)

        # Add explicit result-set ordering so that all databases (including
        # Snowflake) return rows sorted by the window ORDER BY.
        sort_statement = self._build_result_order_by(column_parsers)
        if sort_statement:
            window_statements.append(sort_statement)

        return window_statements

    def construct_code(self) -> str:
        window_statements = self._parse_window_transformations()
        template_data = {
            "window_statements": window_statements,
            "transformation_id": self.window_parser.transform_id,
        }
        self._transformed_code: str = self.template_render(
            template_file_name=TemplateNames.WINDOW, template_content=template_data
        )
        return self._transformed_code

    def transform(self) -> str:
        return self.construct_code()
