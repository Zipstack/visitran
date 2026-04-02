"""Shared filter building utilities for all transformations.

Handles VALUE and COLUMN comparisons, string operators, multi-value
operators.
"""

from backend.application.config_parser.transformation_parsers.condition_parser import ConditionParser
from backend.application.config_parser.transformation_parsers.filter_parser import FilterParser
from backend.application.interpreter.constants import Operators


class FilterBuilder:
    """Builds Ibis filter expressions from FilterParser objects."""

    @staticmethod
    def _format_value(raw_val) -> str:
        """Convert a single raw value to a Python literal string."""
        if raw_val is None:
            return "None"

        if isinstance(raw_val, bool):
            return "True" if raw_val else "False"

        raw_s = str(raw_val).strip()

        # Already quoted
        if (raw_s.startswith("'") and raw_s.endswith("'")) or (
            raw_s.startswith('"') and raw_s.endswith('"')
        ):
            return raw_s

        # Boolean strings
        if raw_s.lower() in ("true", "false"):
            return "True" if raw_s.lower() == "true" else "False"

        # Numbers
        try:
            int(raw_s)
            return raw_s
        except ValueError:
            try:
                float(raw_s)
                return raw_s
            except ValueError:
                pass

        # String - escape and quote
        safe = raw_s.replace("'", "\\'")
        return f"'{safe}'"

    @staticmethod
    def _like_pattern(rhs_value: str, prefix: str = "%", suffix: str = "%") -> str:
        """Prepare properly quoted LIKE pattern for
        CONTAINS/STARTSWITH/ENDSWITH."""
        # Strip any existing quotes from value
        clean_val = str(rhs_value).strip("'\"")
        return f"'{prefix}{clean_val}{suffix}'"

    @staticmethod
    def build_single_condition(class_obj: str, condition: ConditionParser) -> str:
        """Build a single filter condition from ConditionParser.

        Supports:
        - VALUE: column == 'value' or column == 123
        - COLUMN: column == other_column
        - String operators: CONTAINS, NOTCONTAINS, STARTSWITH, ENDSWITH
        - Multi-value: IN, NOTIN
        - NULL checks: NULL, NOTNULL, TRUE, FALSE
        """
        column = condition.lhs_column.column_name
        operator = condition.operator

        if not column or not operator:
            return ""

        lhs = f'{class_obj}["{column}"]'

        # For operators that don't need RHS (NULL, NOTNULL, TRUE, FALSE)
        if operator in Operators.NO_RHS_OPERATORS:
            ibis_op = Operators.get_operator_type(operator, value=None)
            return f'{lhs}{ibis_op}'

        # Handle RHS based on type
        if condition.rhs_type == "COLUMN" and condition.rhs_column:
            # Column-to-column comparison
            rhs_column = condition.rhs_column.column_name
            rhs = f'{class_obj}["{rhs_column}"]'

            # Map operator to Python comparison (only basic operators for column comparison)
            op_map = {
                "EQ": "==",
                "NEQ": "!=",
                "GT": ">",
                "GTE": ">=",
                "LT": "<",
                "LTE": "<=",
            }
            py_op = op_map.get(operator, "==")
            return f"{lhs} {py_op} {rhs}"

        # VALUE type handling below
        rhs_value = condition.rhs_value

        # LIKE-based operators (CONTAINS, NOTCONTAINS, STARTSWITH, ENDSWITH)
        if operator == "CONTAINS":
            pattern = FilterBuilder._like_pattern(rhs_value, "%", "%")
            return f"{lhs}.like({pattern})"
        elif operator == "NOTCONTAINS":
            pattern = FilterBuilder._like_pattern(rhs_value, "%", "%")
            return f"(~{lhs}.like({pattern}))"
        elif operator == "STARTSWITH":
            pattern = FilterBuilder._like_pattern(rhs_value, "", "%")
            return f"{lhs}.like({pattern})"
        elif operator == "ENDSWITH":
            pattern = FilterBuilder._like_pattern(rhs_value, "%", "")
            return f"{lhs}.like({pattern})"

        # Multi-value operators (IN, NOTIN)
        if operator in ("IN", "NOTIN"):
            # Handle comma-separated string or list
            if isinstance(rhs_value, list):
                elems = rhs_value
            elif isinstance(rhs_value, str):
                elems = [v.strip() for v in rhs_value.split(",") if v.strip()]
            else:
                elems = [rhs_value] if rhs_value is not None else []

            if not elems:
                return ""

            formatted_elems = ", ".join(FilterBuilder._format_value(e) for e in elems)
            if operator == "IN":
                return f"{lhs}.isin([{formatted_elems}])"
            else:  # NOTIN
                return f"(~{lhs}.isin([{formatted_elems}]))"

        # Standard comparison operators (EQ, NEQ, GT, GTE, LT, LTE)
        formatted_value = FilterBuilder._format_value(rhs_value)
        ibis_op = Operators.get_operator_type(operator, value=formatted_value)
        return f"{lhs}{ibis_op}"

    @staticmethod
    def build_filter_expression(class_obj: str, filter_parser: FilterParser) -> str:
        """Build an Ibis filter expression from FilterParser with multiple
        conditions. Combines conditions with AND/OR logic.

        Returns a string like: "class_obj = class_obj.filter((cond1) & (cond2) | (cond3))"
        """
        if not filter_parser or not filter_parser.conditions:
            return ""

        conditions = filter_parser.conditions
        filter_conditions = []
        logical_operators = []

        for condition in conditions:
            condition_str = FilterBuilder.build_single_condition(class_obj, condition)
            if condition_str:
                filter_conditions.append(condition_str)
                # Get the logical operator for combining with next filter
                logical_op = condition.condition_type or "AND"
                logical_operators.append(logical_op)

        if not filter_conditions:
            return ""

        # Build combined filter expression
        if len(filter_conditions) == 1:
            return f'{class_obj} = {class_obj}.filter({filter_conditions[0]})'

        # Combine multiple conditions with AND/OR
        # Pattern: condition[i]'s logical_operator specifies how it connects to condition[i-1]
        combined = f"({filter_conditions[0]})"
        for i, condition_str in enumerate(filter_conditions[1:], start=1):
            # Use the logical operator from the CURRENT condition (index i)
            # because condition[i].condition_type tells us how condition[i] connects to previous
            op_symbol = " & " if logical_operators[i] == "AND" else " | "
            combined += f"{op_symbol}({condition_str})"

        return f'{class_obj} = {class_obj}.filter({combined})'
