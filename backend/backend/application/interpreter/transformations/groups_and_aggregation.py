import re

from backend.application.config_parser.constants import AGGREGATE_DICT
from backend.application.config_parser.transformation_parsers.groups_and_aggregation_parser import (
    GroupsAndAggregationParser,
    HavingParser,
)
from backend.application.interpreter.constants import ConditionTypes, Operators, TemplateNames
from backend.application.interpreter.transformations.base_transformation import BaseTransformation


class AggregateFormulaParser:
    """Parser for aggregate formulas like SUM(col)/COUNT(*) or ROUND(AVG(col),
    2). Converts expression strings to Ibis expression code strings.

    Supports two types of aggregate patterns:
    1. Calculations BETWEEN aggregates: SUM(a) / COUNT(*), ROUND(AVG(col), 2)
    2. Expressions INSIDE aggregates: SUM(a * b), SUM(COALESCE(col, 0)), AVG(price - cost)

    Note: The expression is received without the leading '=' prefix.
    The '=' is stripped by the frontend before sending to backend.
    """

    # Mapping of formula function names to Ibis method names
    AGG_FUNC_MAP = {
        "SUM": "sum",
        "COUNT": "count",
        "AVG": "mean",
        "AVERAGE": "mean",
        "MIN": "min",
        "MAX": "max",
        "STDDEV": "std",
        "VARIANCE": "var",
    }

    # Mapping of SQL types to Ibis types for CAST operations
    TYPE_MAP = {
        "INT": "int64",
        "INTEGER": "int64",
        "BIGINT": "int64",
        "SMALLINT": "int16",
        "TINYINT": "int8",
        "FLOAT": "float64",
        "DOUBLE": "float64",
        "DOUBLE PRECISION": "float64",
        "REAL": "float32",
        "DECIMAL": "decimal",
        "NUMERIC": "decimal",
        "VARCHAR": "string",
        "CHAR": "string",
        "TEXT": "string",
        "STRING": "string",
        "BOOLEAN": "bool",
        "BOOL": "bool",
        "DATE": "date",
        "TIMESTAMP": "timestamp",
        "DATETIME": "timestamp",
    }

    # Operators for expression parsing
    OPERATORS = ["+", "-", "*", "/", "%"]

    @classmethod
    def parse(cls, expression: str, alias: str) -> str:
        """Parse an aggregate expression and return Ibis expression code
        string.

        Args:
            expression: Expression string like "SUM(amount)/COUNT(*)" or "SUM(amount * qty)"
                       (without leading '=' - stripped by frontend)
            alias: Column alias for the result

        Returns:
            Ibis expression code string like "(_['amount'].sum() / _.count()).name('avg_amount')"
        """
        # Normalize whitespace
        expression = expression.strip()

        # Parse and convert the expression
        ibis_expr = cls._convert_formula(expression)

        # Add .name() for the alias
        return f"({ibis_expr}).name('{alias}')"

    @classmethod
    def _is_bare_column(cls, content: str) -> bool:
        """Check if content is a bare column name (no operators or
        functions)."""
        content = content.strip()
        # Bare column: just alphanumeric and underscores, no operators or parentheses
        if content == "*":
            return True
        # Check for operators
        for op in cls.OPERATORS:
            if op in content:
                return False
        # Check for function calls (contains parentheses)
        if "(" in content or ")" in content:
            return False
        # Check for CASE expressions
        if re.search(r"\bCASE\b", content, re.IGNORECASE):
            return False
        return True

    @classmethod
    def _extract_function_args(cls, expr: str, func_name: str) -> tuple:
        """Extract arguments from a function call using proper parenthesis
        matching. Returns (arg1, arg2, ...) or None if not a matching function
        call.

        Handles nested parentheses correctly, e.g.:
        - COALESCE(price, 0) -> ('price', '0')
        - COALESCE(CAST(x, INT), 0) -> ('CAST(x, INT)', '0')
        """
        expr = expr.strip()
        pattern = re.match(rf"^{func_name}\s*\(", expr, re.IGNORECASE)
        if not pattern:
            return None

        # Find the matching closing parenthesis for the function
        start = pattern.end() - 1  # Position of opening (
        depth = 1
        i = start + 1
        while i < len(expr) and depth > 0:
            if expr[i] == "(":
                depth += 1
            elif expr[i] == ")":
                depth -= 1
            i += 1

        if depth != 0:
            return None  # Unbalanced parentheses

        end = i - 1  # Position of closing )

        # Check if this function call spans the entire expression
        # (i.e., nothing comes after the closing parenthesis)
        if end != len(expr) - 1:
            return None  # There's more after the function call

        # Extract content inside parentheses
        content = expr[start + 1 : end].strip()

        # Split by comma, respecting nested parentheses
        args = []
        current_arg = ""
        depth = 0
        for char in content:
            if char == "(":
                depth += 1
                current_arg += char
            elif char == ")":
                depth -= 1
                current_arg += char
            elif char == "," and depth == 0:
                args.append(current_arg.strip())
                current_arg = ""
            else:
                current_arg += char
        if current_arg.strip():
            args.append(current_arg.strip())

        return tuple(args) if args else None

    @classmethod
    def _convert_inner_expression(cls, expr: str) -> str:
        """Convert an expression inside an aggregate to Ibis code string.

        Examples:
            "amount * quantity" -> "_['amount'] * _['quantity']"
            "price - cost" -> "_['price'] - _['cost']"
            "COALESCE(amount, 0)" -> "_['amount'].fill_null(0)"
            "ABS(value)" -> "_['value'].abs()"
        """
        expr = expr.strip()

        # Handle COALESCE: COALESCE(col, default) -> _['col'].fill_null(default)
        coalesce_args = cls._extract_function_args(expr, "COALESCE")
        if coalesce_args and len(coalesce_args) == 2:
            col, default_val = coalesce_args
            inner = cls._convert_inner_expression(col)
            try:
                float(default_val)
                return f"({inner}).fill_null({default_val})"
            except ValueError:
                # String default - strip quotes if present
                default_val = default_val.strip("'\"")
                return f"({inner}).fill_null('{default_val}')"

        # Handle ABS: ABS(col) -> _['col'].abs()
        abs_args = cls._extract_function_args(expr, "ABS")
        if abs_args and len(abs_args) == 1:
            inner = cls._convert_inner_expression(abs_args[0])
            return f"({inner}).abs()"

        # Handle CAST - supports two syntaxes:
        # 1. SQL syntax (from LLM): CAST(col AS type) - e.g., CAST(ssn AS DOUBLE PRECISION)
        # 2. NoCode UI syntax: cast(col, type) - e.g., cast(ssn, float64)
        cast_args = cls._extract_function_args(expr, "CAST")
        if cast_args:
            if len(cast_args) == 2:
                # Comma syntax: CAST(col, type)
                return cls._convert_inner_expression(cast_args[0])
            elif len(cast_args) == 1:
                # SQL AS syntax: CAST(col AS type) - the AS part is in the single arg
                as_match = re.match(r"^(.+)\s+AS\s+.+$", cast_args[0], re.IGNORECASE)
                if as_match:
                    return cls._convert_inner_expression(as_match.group(1).strip())

        # Handle CASE WHEN expressions
        case_match = re.match(r"^CASE\s+WHEN\s+(.+)\s+THEN\s+(.+)\s+ELSE\s+(.+)\s+END$", expr, re.IGNORECASE)
        if case_match:
            condition = case_match.group(1).strip()
            then_val = case_match.group(2).strip()
            else_val = case_match.group(3).strip()

            # Parse condition
            cond_ibis = cls._parse_condition(condition)

            # Parse then/else values
            then_ibis = cls._parse_value(then_val)
            else_ibis = cls._parse_value(else_val)

            return f"ibis.case().when({cond_ibis}, {then_ibis}).else_({else_ibis}).end()"

        # Handle arithmetic expressions with operators
        # We need to handle operator precedence, so process in order: +/- then */%
        # Simple approach: split by operators and convert each part

        # First, try to parse as arithmetic expression
        result = cls._parse_arithmetic(expr)
        if result:
            return result

        # If nothing else, treat as bare column
        return f"_['{expr}']"

    @classmethod
    def _find_prev_non_space(cls, expr: str, pos: int) -> str:
        """Find the previous non-space character before position pos."""
        prev_idx = pos - 1
        while prev_idx >= 0 and expr[prev_idx] == " ":
            prev_idx -= 1
        return expr[prev_idx] if prev_idx >= 0 else ""

    @classmethod
    def _parse_arithmetic(cls, expr: str) -> str:
        """Parse arithmetic expression and convert to Ibis."""
        expr = expr.strip()

        # Handle parentheses first
        if expr.startswith("(") and expr.endswith(")"):
            # Check if these are matching outer parens
            depth = 0
            is_outer = True
            for i, c in enumerate(expr):
                if c == "(":
                    depth += 1
                elif c == ")":
                    depth -= 1
                if depth == 0 and i < len(expr) - 1:
                    is_outer = False
                    break
            if is_outer:
                return f"({cls._parse_arithmetic(expr[1:-1])})"

        # Find the lowest precedence operator (+ or -) not inside parentheses
        depth = 0
        for i in range(len(expr) - 1, -1, -1):
            c = expr[i]
            if c == ")":
                depth += 1
            elif c == "(":
                depth -= 1
            elif depth == 0 and c in ["+", "-"] and i > 0:
                # Make sure it's not a unary operator
                # Look back past any spaces to find the actual previous character
                prev = cls._find_prev_non_space(expr, i)
                if prev and prev not in ["(", "+", "-", "*", "/", "%"]:
                    left = expr[:i].strip()
                    right = expr[i + 1 :].strip()
                    left_ibis = cls._parse_arithmetic(left)
                    right_ibis = cls._parse_arithmetic(right)
                    return f"({left_ibis} {c} {right_ibis})"

        # Find * / % operators
        depth = 0
        for i in range(len(expr) - 1, -1, -1):
            c = expr[i]
            if c == ")":
                depth += 1
            elif c == "(":
                depth -= 1
            elif depth == 0 and c in ["*", "/", "%"]:
                left = expr[:i].strip()
                right = expr[i + 1 :].strip()
                left_ibis = cls._parse_arithmetic(left)
                right_ibis = cls._parse_arithmetic(right)
                return f"({left_ibis} {c} {right_ibis})"

        # No operators found, check if it's a number or column
        try:
            float(expr)
            return expr  # It's a number literal
        except ValueError:
            pass

        # Check for function calls
        func_match = re.match(r"^(\w+)\s*\((.+)\)$", expr)
        if func_match:
            func_name = func_match.group(1).upper()

            # Window functions are NOT supported in aggregate formulas
            # They should use the Window transformation instead
            if func_name in cls.WINDOW_FUNCTIONS:
                raise ValueError(
                    f"Window function '{func_name}' is not supported in aggregate formulas. "
                    f"Use the Window transformation for window functions instead."
                )

            # Only recurse for known functions that _convert_inner_expression can handle
            # to prevent infinite recursion for unknown functions
            KNOWN_FUNCTIONS = {"COALESCE", "ABS", "CAST"}
            if func_name in KNOWN_FUNCTIONS:
                return cls._convert_inner_expression(expr)

            # For unknown functions, treat as column reference (the expression as-is)
            # This handles cases where a column name might look like a function
            return f"_['{expr}']"

        # It's a column reference
        return f"_['{expr}']"

    @classmethod
    def _parse_condition(cls, condition: str) -> str:
        """Parse a condition expression for CASE WHEN."""
        condition = condition.strip()

        # Handle comparison operators
        for op, ibis_op in [
            (">=", ">="),
            ("<=", "<="),
            ("!=", "!="),
            ("<>", "!="),
            ("=", "=="),
            (">", ">"),
            ("<", "<"),
        ]:
            if op in condition:
                parts = condition.split(op, 1)
                if len(parts) == 2:
                    left = cls._parse_value(parts[0].strip())
                    right = cls._parse_value(parts[1].strip())
                    return f"({left} {ibis_op} {right})"

        return condition

    @classmethod
    def _parse_value(cls, value: str) -> str:
        """Parse a value (column, number, or string literal)."""
        value = value.strip()

        # Check if it's a number
        try:
            float(value)
            return value
        except ValueError:
            pass

        # Check if it's a string literal
        if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
            return value

        # It's a column reference
        return f"_['{value}']"

    # Window functions that are NOT supported in aggregate formulas
    WINDOW_FUNCTIONS = {
        "LAG",
        "LEAD",
        "ROW_NUMBER",
        "RANK",
        "DENSE_RANK",
        "PERCENT_RANK",
        "NTILE",
        "CUME_DIST",
        "NTH_VALUE",
        "FIRST_VALUE",
        "LAST_VALUE",
        "FIRST",
        "LAST",
    }

    @classmethod
    def _check_for_window_function(cls, expr: str) -> None:
        """Check if expression contains a window function and raise an error if
        found.

        Window functions should use the Window transformation, not
        aggregate formulas.
        """
        # Check for window function at the start of expression
        func_match = re.match(r"^(\w+)\s*\(", expr.strip())
        if func_match:
            func_name = func_match.group(1).upper()
            if func_name in cls.WINDOW_FUNCTIONS:
                raise ValueError(
                    f"Window function '{func_name}' is not supported in aggregate formulas. "
                    f"Use the Window transformation for window functions instead."
                )

        # Also check for window functions anywhere in the expression (nested)
        for wf in cls.WINDOW_FUNCTIONS:
            if re.search(rf"\b{wf}\s*\(", expr, re.IGNORECASE):
                raise ValueError(
                    f"Window function '{wf}' is not supported in aggregate formulas. "
                    f"Use the Window transformation for window functions instead."
                )

    @classmethod
    def _convert_formula(cls, formula: str) -> str:
        """Convert formula string to Ibis expression code."""
        # First, check for window functions which are not supported
        cls._check_for_window_function(formula)

        # Handle ROUND wrapper: ROUND(expr, n) -> (expr).round(n)
        round_match = re.match(r"^ROUND\s*\(\s*(.+)\s*,\s*(\d+)\s*\)$", formula, re.IGNORECASE)
        if round_match:
            inner_expr = round_match.group(1)
            decimals = round_match.group(2)
            inner_ibis = cls._convert_formula(inner_expr)
            return f"({inner_ibis}).round({decimals})"

        # Handle COALESCE wrapper (wrapping an aggregate): COALESCE(SUM(...), default)
        coalesce_match = re.match(r"^COALESCE\s*\(\s*(.+)\s*,\s*(.+)\s*\)$", formula, re.IGNORECASE)
        if coalesce_match:
            inner_expr = coalesce_match.group(1).strip()
            default_val = coalesce_match.group(2).strip()

            # Check if inner contains an aggregate function
            if cls._contains_aggregate(inner_expr):
                inner_ibis = cls._convert_formula(inner_expr)
                try:
                    float(default_val)
                    return f"({inner_ibis}).fill_null({default_val})"
                except ValueError:
                    return f"({inner_ibis}).fill_null('{default_val}')"

        # Handle top-level CAST wrapper: CAST(expr, TYPE) or CAST(expr AS TYPE) -> (expr).cast('ibis_type')
        # This handles casting aggregate expression results (e.g., CAST(MAX(date) - MIN(date), INT))
        cast_args = cls._extract_function_args(formula, "CAST")
        if cast_args:
            inner_expr = None
            type_str = None
            if len(cast_args) == 2:
                # Comma syntax: CAST(expr, type)
                inner_expr, type_str = cast_args[0], cast_args[1]
            elif len(cast_args) == 1:
                # SQL AS syntax: CAST(expr AS type)
                as_match = re.match(r"^(.+)\s+AS\s+(.+)$", cast_args[0], re.IGNORECASE)
                if as_match:
                    inner_expr = as_match.group(1).strip()
                    type_str = as_match.group(2).strip()

            if inner_expr and type_str and cls._contains_aggregate(inner_expr):
                inner_ibis = cls._convert_formula(inner_expr)
                ibis_type = cls.TYPE_MAP.get(type_str.upper(), "float64")
                return f"({inner_ibis}).cast('{ibis_type}')"

        # Replace aggregate functions with Ibis expressions
        result = cls._replace_aggregates(formula)

        # Replace CAST functions with Ibis .cast() method
        result = cls._replace_casts(result)

        # Auto-cast date subtraction patterns to int64 to avoid Ibis interval type inference issues
        # Pattern: MAX(col) - MIN(col) becomes _['col'].max() - _['col'].min()
        # In PostgreSQL, DATE - DATE returns integer, but Ibis infers interval which causes type mismatch
        date_sub_pattern = re.compile(r"_\['(\w+)'\]\.max\(\)\s*-\s*_\['\1'\]\.min\(\)")
        if date_sub_pattern.search(result):
            result = f"({result}).cast('int64')"

        return result

    @classmethod
    def _contains_aggregate(cls, expr: str) -> bool:
        """Check if expression contains an aggregate function."""
        agg_pattern = r"\b(SUM|COUNT|AVG|AVERAGE|MIN|MAX|STDDEV|VARIANCE)\s*\("
        return bool(re.search(agg_pattern, expr, re.IGNORECASE))

    @classmethod
    def _find_matching_paren(cls, s: str, start: int) -> int:
        """Find the matching closing parenthesis for the opening paren at
        start."""
        depth = 1
        i = start + 1
        while i < len(s) and depth > 0:
            if s[i] == "(":
                depth += 1
            elif s[i] == ")":
                depth -= 1
            i += 1
        return i - 1 if depth == 0 else -1

    @classmethod
    def _replace_aggregates(cls, formula: str) -> str:
        """Replace aggregate function calls with Ibis expressions."""
        result = formula

        # Pattern to match aggregate functions with proper parenthesis matching
        agg_funcs = "|".join(cls.AGG_FUNC_MAP.keys())

        # We need to handle nested parentheses, so we can't use simple regex
        # Instead, find aggregate functions and extract their content properly

        i = 0
        new_result = ""
        while i < len(result):
            # Look for aggregate function
            match = re.match(r"\b(" + agg_funcs + r")\s*\(", result[i:], re.IGNORECASE)
            if match:
                func_name = match.group(1).upper()
                ibis_method = cls.AGG_FUNC_MAP.get(func_name, func_name.lower())

                # Find the opening paren
                paren_start = i + match.end() - 1

                # Find matching closing paren
                paren_end = cls._find_matching_paren(result, paren_start)

                if paren_end > paren_start:
                    # Extract content inside parentheses
                    content = result[paren_start + 1 : paren_end].strip()

                    # Check for OVER clause after the aggregate (window function)
                    remaining = result[paren_end + 1 :].lstrip()
                    if remaining.upper().startswith("OVER"):
                        raise ValueError(
                            f"Window functions ({func_name}(...) OVER (...)) are not supported in aggregate formulas. "
                            f"Use the Window transformation for window functions instead."
                        )

                    if content == "*":
                        # COUNT(*) -> _.count()
                        if func_name == "COUNT":
                            new_result += "_.count()"
                        else:
                            raise ValueError(f"Aggregate function {func_name} does not support *")
                    elif cls._is_bare_column(content):
                        # SUM(amount) -> _['amount'].sum()
                        new_result += f"_['{content}'].{ibis_method}()"
                    else:
                        # Expression inside aggregate: SUM(a * b) -> (_['a'] * _['b']).sum()
                        inner_ibis = cls._convert_inner_expression(content)
                        new_result += f"({inner_ibis}).{ibis_method}()"

                    i = paren_end + 1
                else:
                    # No matching paren found, keep as-is
                    new_result += result[i]
                    i += 1
            else:
                new_result += result[i]
                i += 1

        return new_result

    @classmethod
    def _parse_cast_content(cls, content: str) -> tuple:
        """Parse CAST content into (expression, type_string).

        Handles: 'expr AS TYPE' and 'expr, TYPE' syntaxes.
        Returns (None, None) if parsing fails.
        """
        # Try AS syntax: CAST(expr AS TYPE)
        as_match = re.search(r"\s+AS\s+(.+)$", content, re.IGNORECASE)
        if as_match:
            return content[: as_match.start()].strip(), as_match.group(1).strip()

        # Try comma syntax: find last comma at depth 0
        depth, last_comma = 0, -1
        for i, c in enumerate(content):
            depth += (c == "(") - (c == ")")
            if c == "," and depth == 0:
                last_comma = i

        if last_comma > 0:
            return content[:last_comma].strip(), content[last_comma + 1 :].strip()

        return None, None

    @classmethod
    def _replace_casts(cls, formula: str) -> str:
        """Replace CAST(expr, TYPE) with (expr).cast('ibis_type')."""
        result = []
        i = 0

        while i < len(formula):
            match = re.match(r"\bCAST\s*\(", formula[i:], re.IGNORECASE)
            if not match:
                result.append(formula[i])
                i += 1
                continue

            paren_start = i + match.end() - 1
            paren_end = cls._find_matching_paren(formula, paren_start)

            if paren_end <= paren_start:
                result.append(formula[i])
                i += 1
                continue

            content = formula[paren_start + 1 : paren_end].strip()
            expr, type_str = cls._parse_cast_content(content)

            if not (expr and type_str):
                result.append(formula[i])
                i += 1
                continue

            ibis_type = cls.TYPE_MAP.get(type_str.upper(), "float64")
            result.append(f"({expr}).cast('{ibis_type}')")
            i = paren_end + 1

        return "".join(result)


class GroupsAndAggregationTransformation(BaseTransformation):
    def __init__(self, parser: GroupsAndAggregationParser, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.groups_and_agg_parser: GroupsAndAggregationParser = parser

    def _parse_filter_columns(self) -> str:
        filter_parser = self.groups_and_agg_parser.filter
        filter_string = ""
        if not filter_parser:
            return filter_string
        conditions = filter_parser.conditions

        for count, condition in enumerate(conditions):
            source_pointer = "_"
            lhs_name = condition.lhs_column.column_name

            filter_string += f"( {source_pointer}['{lhs_name}']"
            lhs_type = condition.lhs_column.data_type
            rhs_value = condition.rhs_value
            rhs_value = rhs_value[0] if rhs_value.__len__() >= 1 else ""
            if lhs_type == "String":
                rhs_value = f"'{rhs_value}'"
            operation_type = Operators.get_operator_type(condition.operator, value=rhs_value)
            filter_string += operation_type

            if count < conditions.__len__() - 1:
                # Use the NEXT condition's type, as it specifies how it connects to current
                filter_string += f" ) {ConditionTypes.get_condition_type(conditions[count+1].condition_type)} "

        if filter_string:
            filter_string = f".filter({filter_string}))"

        return filter_string

    def _parse_having_columns(self) -> str:
        having_parser: HavingParser = self.groups_and_agg_parser.having
        having_string: str = ""
        if having_parser and having_parser.conditions:
            conditions = having_parser.conditions
            for count, condition in enumerate(conditions):
                source_pointer = "_"
                lhs_name = condition.lhs_column.column_name
                function_name = AGGREGATE_DICT.get(condition.lhs_column.function)

                # Convert * to appropriate Ibis expression for proper SQL generation in HAVING clause
                # Only COUNT supports *, other aggregates require actual columns
                if lhs_name == "*":
                    if condition.lhs_column.function == "COUNT":
                        # For COUNT(*), use _.count() which generates COUNT(*)
                        having_string += f"( {source_pointer}.count()"
                    else:
                        # Other aggregates don't support * - this shouldn't happen from UI
                        raise ValueError(
                            f"Aggregate function {condition.lhs_column.function} does not support * in HAVING clause"
                        )
                # Handle COUNT_DISTINCT in HAVING - uses nunique()
                elif condition.lhs_column.function == "COUNT_DISTINCT":
                    having_string += f"( {source_pointer}.{lhs_name}.nunique()"
                else:
                    having_string += f"( {source_pointer}.{lhs_name}.{function_name}()"

                lhs_type = condition.lhs_column.data_type
                rhs_value = (
                    condition.rhs_value[0]
                    if isinstance(condition.rhs_value, list) and len(condition.rhs_value) > 0
                    else condition.rhs_value or ""
                )
                operation_type = Operators.get_operator_type(condition.operator, value=rhs_value)
                having_string += operation_type

                if count < conditions.__len__() - 1:
                    having_string += f" ) {ConditionTypes.get_condition_type(conditions[count+1].condition_type)} "
            having_string = f".having({having_string}))"
        return having_string

    def _parse_aggregate_columns(self) -> str:
        computed_string: str = ".aggregate("
        if aggregation_cols := self.groups_and_agg_parser.aggregate_columns:
            aggregate_string = "["
            for count, agg_col_parser in enumerate(aggregation_cols):
                if count != 0:
                    aggregate_string += ", "

                # Check if this is a formula aggregate (has expression field)
                if agg_col_parser.is_formula_aggregate:
                    # Formula aggregate: use AggregateFormulaParser (not FormulaSQL)
                    # Expression comes without '=' prefix (stripped by frontend)
                    expression = agg_col_parser.expression
                    alias = agg_col_parser.alias

                    # Remove newlines and normalize
                    expression = "".join(expression.split("\n"))

                    # Parse expression and generate Ibis expression code
                    ibis_expr = AggregateFormulaParser.parse(expression, alias)
                    aggregate_string += ibis_expr
                else:
                    # Simple aggregate: function + column
                    aggregate_column = agg_col_parser.column
                    constructive_name = agg_col_parser.alias
                    function_name = AGGREGATE_DICT.get(agg_col_parser.function)

                    # Convert * to appropriate Ibis expression for proper SQL generation
                    # Only COUNT supports *, other aggregates require actual columns
                    if aggregate_column == "*":
                        if agg_col_parser.function == "COUNT":
                            # For COUNT(*), use _.count() which generates COUNT(*)
                            aggregate_string += f"_.count().name('{constructive_name}')"
                        else:
                            # Other aggregates don't support * - this shouldn't happen from UI
                            # but handle it gracefully by raising an error
                            raise ValueError(f"Aggregate function {agg_col_parser.function} does not support * column")
                    # Handle COUNT_DISTINCT - uses nunique() which requires a specific column
                    elif agg_col_parser.function == "COUNT_DISTINCT":
                        aggregate_string += f"_['{aggregate_column}'].nunique().name('{constructive_name}')"
                    else:
                        aggregate_string += f"_['{aggregate_column}'].{function_name}().name('{constructive_name}')"
            computed_string += f"{aggregate_string}]"
        return computed_string + ")"

    def _parse_groups(self) -> str:
        computed_string: str = ""
        if group_columns := self.groups_and_agg_parser.group_columns:
            columns = "["
            for count, column in enumerate(group_columns):
                if count != 0:
                    columns += ", "
                columns += f"source_table['{column}']"
            columns += "]"
            computed_string += f".group_by({columns})"
        return computed_string

    def construct_code(self) -> str:
        groups_and_aggr_statement = self._parse_groups()
        groups_and_aggr_statement += self._parse_having_columns()
        groups_and_aggr_statement += self._parse_aggregate_columns()
        groups_and_aggr_statement += self._parse_filter_columns()

        template_data = {
            "groups_and_aggr_statement": groups_and_aggr_statement,
            "transformation_id": self.groups_and_agg_parser.transform_id,
        }
        self._transformed_code: str = self.template_render(
            template_file_name=TemplateNames.GROUPS_AND_AGGREGATION, template_content=template_data
        )
        return self._transformed_code

    def transform(self) -> str:
        return self.construct_code()
