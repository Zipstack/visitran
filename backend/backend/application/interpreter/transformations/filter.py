from typing import List, Optional

from backend.application.config_parser.transformation_parsers.filter_parser import ConditionParser, FilterParser
from backend.application.interpreter.constants import ConditionTypes, Operators, TemplateNames
from backend.application.interpreter.transformations.base_transformation import BaseTransformation


class FiltersTransformation(BaseTransformation):
    # Functions that return string/text type
    TEXT_FUNCTIONS = {
        "MID", "LEFT", "RIGHT", "SUBSTRING", "SUBSTR",
        "CONCAT", "CONCATENATE", "UPPER", "LOWER",
        "TRIM", "LTRIM", "RTRIM", "REPLACE", "TEXT",
        "CHAR", "REPT", "PROPER", "CLEAN", "SUBSTITUTE",
    }

    # String/text data types that require quoted values
    STRING_TYPES = {
        "string", "varchar", "text", "char", "nvarchar", "nchar",
        "character varying", "character", "bpchar",
    }

    def __init__(self, parser: FilterParser, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.filter_parser: FilterParser = parser
        self._has_formula_expression = False

    @classmethod
    def _is_text_formula(cls, expression: str) -> bool:
        """Check if the formula expression returns a text/string type."""
        if not expression:
            return False
        # Get the function name (first word before '(')
        expr_upper = expression.strip().upper()
        for func in cls.TEXT_FUNCTIONS:
            if expr_upper.startswith(func + "("):
                return True
        return False

    @staticmethod
    def _format_value(lhs_type: str, operator: str, raw_val) -> str:
        """Convert a single raw value to a Python literal string."""
        if raw_val is None:
            return "None"

        if isinstance(raw_val, bool):
            return "True" if raw_val else "False"

        raw_s = str(raw_val).strip()
        if (raw_s.startswith("'") and raw_s.endswith("'")) or (raw_s.startswith('"') and raw_s.endswith('"')):
            return raw_s

        if raw_s.lower() in ("true", "false"):
            return "True" if raw_s.lower() == "true" else "False"

        try:
            int(raw_s)
            return raw_s
        except Exception:
            try:
                float(raw_s)
                return raw_s
            except Exception:
                pass

        safe = raw_s.replace("'", "\\'")
        return f"'{safe}'"

    @staticmethod
    def _like_pattern(rhs_value: str, prefix: str = "%", suffix: str = "%") -> str:
        """Prepare properly quoted LIKE pattern for
        CONTAINS/STARTSWITH/ENDSWITH."""
        clean_val = rhs_value.strip("'")
        return f"'{prefix}{clean_val}{suffix}'"

    def _build_formula_expression(self, expression: str, expr_id: str) -> str:
        """Build a FormulaSQL expression for use in filter conditions.

        Args:
            expression: The formula expression (e.g., "YEAR(order_date)", "col1 * col2")
            expr_id: A unique identifier for the expression (for naming)

        Returns:
            String representation of FormulaSQL call for code generation
        """
        # Escape single quotes in expression
        safe_expr = expression.replace("'", "\\'")
        return f"FormulaSQL(source_table, '{expr_id}', '={safe_expr}').ibis_column()"

    def _get_rhs_value(self, condition: ConditionParser, condition_idx: int = 0) -> Optional[str]:
        # Check if LHS is a text-returning formula
        if condition.lhs_type == "FORMULA" and self._is_text_formula(condition.lhs_expression):
            lhs_type = "TEXT_FORMULA"
        else:
            lhs_type = getattr(condition.lhs_column, "data_type", "") or ""
        op = condition.operator

        # Handle FORMULA type RHS
        if condition.rhs_type == "FORMULA":
            rhs_expr = condition.rhs_expression
            if rhs_expr:
                self._has_formula_expression = True
                return self._build_formula_expression(rhs_expr, f"_filter_rhs_{condition_idx}")
            return None

        if condition.rhs_type == "COLUMN":
            rhs_col_name = getattr(condition.rhs_column, "column_name", None)
            if rhs_col_name:
                return f"_['{rhs_col_name}']"
            # Handle string value
            if isinstance(condition.rhs_value, str):
                return f"_['{condition.rhs_value}']"
            # Handle list value - frontend sends ["columnName"] for non-join column RHS
            if isinstance(condition.rhs_value, list) and condition.rhs_value:
                return f"_['{condition.rhs_value[0]}']"
            return None

        raw_rhs = condition.rhs_value
        if isinstance(raw_rhs, list) and raw_rhs:
            raw_rhs = raw_rhs[0]

        if condition.rhs_value in (None, [""]):
            return "" if op in Operators.NO_RHS_OPERATORS else raw_rhs

        # Multi-value operators
        if op in ("IN", "NOTIN"):
            if isinstance(raw_rhs, list):
                elems = raw_rhs
            elif isinstance(raw_rhs, str):
                elems = [v.strip() for v in raw_rhs.split(",") if v.strip()]
            else:
                raise ValueError("Operator [IN, NOTIN] expects a list or comma-separated string RHS.")
            if not elems:
                return None
            # Quote values as strings if column is string/text type or text formula
            is_string_col = lhs_type == "TEXT_FORMULA" or lhs_type.lower() in self.STRING_TYPES
            if is_string_col:
                quoted = [f"'{str(e).strip().replace(chr(39), chr(92)+chr(39))}'" for e in elems]
                return f"[{', '.join(quoted)}]"
            # Otherwise keep values as-is (numbers stay numbers)
            return f"[{', '.join(self._format_value(lhs_type, op, e) for e in elems)}]"

        # Single-value flattening
        if isinstance(raw_rhs, list):
            raw_rhs = raw_rhs[0] if raw_rhs else None

        if raw_rhs in (None, ""):
            return None

        return self._format_value(lhs_type, op, raw_rhs)

    def _get_between_values(self, condition: ConditionParser, condition_idx: int = 0):
        """Return formatted (low, high) values for BETWEEN operator."""
        if condition.lhs_type == "FORMULA" and self._is_text_formula(condition.lhs_expression):
            lhs_type = "TEXT_FORMULA"
        else:
            lhs_type = getattr(condition.lhs_column, "data_type", "") or ""
        low_raw, high_raw = condition.rhs_between_values
        low = self._format_value(lhs_type, "BETWEEN", low_raw)
        high = self._format_value(lhs_type, "BETWEEN", high_raw)
        return low, high

    def _build_like_expression(self, lhs_expr: str, rhs_value: str, op: str) -> str:
        """Construct LIKE/NOTLIKE expressions."""
        if op == "CONTAINS":
            pattern = self._like_pattern(rhs_value, "%", "%")
            return f"{lhs_expr}.like({pattern})"
        elif op == "NOTCONTAINS":
            pattern = self._like_pattern(rhs_value, "%", "%")
            return f"(~{lhs_expr}.like({pattern}))"
        elif op == "STARTSWITH":
            pattern = self._like_pattern(rhs_value, "", "%")
            return f"{lhs_expr}.like({pattern})"
        elif op == "ENDSWITH":
            pattern = self._like_pattern(rhs_value, "%", "")
            return f"{lhs_expr}.like({pattern})"
        else:
            raise ValueError(f"Unsupported LIKE operator: {op}")

    def _get_lhs_expression(self, condition: ConditionParser, condition_idx: int) -> str:
        """Get the LHS expression for a filter condition.

        Returns either a column reference or a FormulaSQL expression.
        """
        if condition.lhs_type == "FORMULA":
            lhs_expr = condition.lhs_expression
            if lhs_expr:
                self._has_formula_expression = True
                return self._build_formula_expression(lhs_expr, f"_filter_lhs_{condition_idx}")
            # FORMULA type but no expression provided
            raise ValueError("FORMULA type filter condition requires an expression.")
        # Default: column reference
        lhs_name = condition.lhs_column.column_name
        if not lhs_name:
            raise ValueError("Filter condition requires either a column name or a formula expression.")
        return f"_['{lhs_name}']"

    def parse_filter(self) -> str:
        conditions: list[ConditionParser] = self.filter_parser.conditions
        if not conditions:
            return ""

        filter_parts = []

        for idx, cond in enumerate(conditions):
            lhs_expr = self._get_lhs_expression(cond, idx)
            lhs_name = cond.lhs_column.column_name if cond.lhs_type != "FORMULA" else None
            op = cond.operator
            rhs_value = self._get_rhs_value(cond, idx)
            cond_type = cond.condition_type or ("AND" if idx > 0 else "")

            # No-RHS operators
            if op in Operators.NO_RHS_OPERATORS:
                expr = f"{lhs_expr}{Operators.get_operator_type(op, value='')}"
            # Multi-value operators
            elif op in ("IN", "NOTIN"):
                if rhs_value is None:
                    raise ValueError(f"RHS not provided for {op} condition.")
                expr = f"{lhs_expr}.isin({rhs_value})" if op == "IN" else f"(~{lhs_expr}.isin({rhs_value}))"
            # BETWEEN operator
            elif op == "BETWEEN":
                low, high = self._get_between_values(cond, idx)
                expr = f"{lhs_expr}.between({low}, {high})"
            # LIKE-based operators (supports both column and formula LHS)
            elif op in ("CONTAINS", "NOTCONTAINS", "STARTSWITH", "ENDSWITH"):
                if rhs_value is None:
                    raise ValueError(f"RHS not provided for {op} condition.")
                expr = self._build_like_expression(lhs_expr, rhs_value, op)
            # Other operators
            else:
                if rhs_value is None:
                    raise ValueError("RHS value is not provided for the condition.")
                op_fragment = Operators.get_operator_type(op, value=rhs_value or "''")
                expr = f"(~{lhs_expr}{op_fragment})" if op in Operators.NEGATIVE_OPERATORS else f"({lhs_expr}{op_fragment})"
            # Combine with previous
            if filter_parts:
                filter_parts.append(f"{ConditionTypes.get_condition_type(cond_type)} ({expr})")
            else:
                filter_parts.append(f"({expr})")

        filter_string = " ".join(filter_parts)
        return f".filter([{filter_string}])" if filter_string else ""

    def construct_code(self):
        # Reset formula flag before parsing
        self._has_formula_expression = False
        filters_content = self.parse_filter()

        # Add FormulaSQL import if formula expressions are used
        if self._has_formula_expression:
            self.add_headers("from formulasql.formulasql import FormulaSQL")

        template_data = {
            "filters_content": filters_content,
            "transformation_id": self.filter_parser.transform_id
        }
        self._transformed_code: str = self.template_render(
            template_file_name=TemplateNames.FILTER, template_content=template_data
        )
        return self._transformed_code

    def transform(self) -> str:
        return self.construct_code()
