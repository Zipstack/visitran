from typing import Any, Optional

from backend.application.config_parser.base_parser import BaseParser
from backend.application.config_parser.transformation_parsers.column_parser import ColumnParser


class ConditionParser(BaseParser):
    def __init__(self, config_data: dict[str, Any]) -> None:
        self._condition_data = config_data.get("condition", {})
        self._condition_type = config_data.get("logical_operator")
        super().__init__(self._condition_data)
        self._lhs_data = self.get("lhs", {})
        self._rhs_data = self.get("rhs", {})

    @property
    def lhs_type(self):
        return self._lhs_data.get("type")

    @property
    def lhs_column(self) -> ColumnParser:
        # Use empty dict if column is None (e.g., for FORMULA type)
        return ColumnParser(self._lhs_data.get("column") or {})

    @property
    def operator(self) -> str:
        op = self.get("operator")
        if op is None:  # YAML "NULL" → Python None
            return "NULL"
        return str(op).upper()

    @property
    def rhs_value(self) -> str:
        return self._rhs_data.get("value")

    @property
    def condition_type(self) -> str:
        return self._condition_type

    @property
    def rhs_type(self):
        return self._rhs_data.get("type")

    @property
    def rhs_column(self) -> ColumnParser:
        return ColumnParser(self._rhs_data.get("column", {}))

    @property
    def lhs_expression(self) -> Optional[str]:
        """Get LHS formula expression if type is FORMULA."""
        if self.lhs_type == "FORMULA":
            return self._lhs_data.get("expression")
        return None

    @property
    def rhs_expression(self) -> Optional[str]:
        """Get RHS formula expression if type is FORMULA."""
        if self.rhs_type == "FORMULA":
            return self._rhs_data.get("expression")
        return None

    @property
    def rhs_between_values(self) -> tuple:
        """Return (low, high) values for BETWEEN operator. Falls back to (None, None)."""
        val = self._rhs_data.get("value", [])
        if isinstance(val, list) and len(val) >= 2:
            return (val[0], val[1])
        return (None, None)
