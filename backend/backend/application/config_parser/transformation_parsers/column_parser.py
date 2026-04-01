from typing import Any

from backend.application.config_parser.base_parser import BaseParser


class ColumnParser(BaseParser):
    @property
    def table_name(self) -> str:
        """Return table name, default empty string."""
        return self.get("table_name", "") or ""

    @property
    def schema_name(self) -> str:
        """Return schema name, default empty string."""
        return self.get("schema_name", "") or ""

    @property
    def column_name(self) -> str:
        """Return column name, default empty string."""
        return self.get("column_name", "") or ""

    @property
    def data_type(self) -> str:
        """Return data type, default empty string."""
        return self.get("data_type", "") or ""

    @property
    def type(self) -> str:
        """Return column type, default empty string."""
        return self.get("type", "") or ""

    @property
    def formula(self) -> Any:
        """Return formula from 'operation.formula'.

        Always return a string (default "") to avoid NoneType errors.
        """
        return (self.get("operation", {}) or {}).get("formula", "")

    @property
    def function(self) -> str:
        """Return function name, default empty string."""
        return self.get("function", "") or ""

    @property
    def window_function(self) -> str:
        """Return window function name from operation (for WINDOW type
        columns)."""
        return (self.get("operation", {}) or {}).get("function", "") or ""

    @property
    def partition_by(self) -> list[str]:
        """Return partition_by columns for WINDOW type columns."""
        return (self.get("operation", {}) or {}).get("partition_by", []) or []

    @property
    def order_by(self) -> list[dict[str, str]]:
        """Return order_by specification for WINDOW type columns.

        Each item is a dict with 'column' and 'direction' keys.
        Example: [{"column": "order_date", "direction": "DESC"}]
        """
        return (self.get("operation", {}) or {}).get("order_by", []) or []

    @property
    def agg_column(self) -> str:
        """Return aggregation column for window aggregate functions (SUM, AVG,
        etc.)."""
        return (self.get("operation", {}) or {}).get("agg_column", "") or ""

    @property
    def preceding(self) -> int | str | None:
        """Return preceding frame specification for WINDOW type columns.

        Can be:
        - An integer (0, 1, 2, etc.) for fixed rows
        - "unbounded" for UNBOUNDED PRECEDING
        - None if not specified (default unbounded behavior)
        """
        return (self.get("operation", {}) or {}).get("preceding")

    @property
    def following(self) -> int | str | None:
        """Return following frame specification for WINDOW type columns.

        Can be:
        - An integer (0 for CURRENT ROW, 1, 2, etc.)
        - "unbounded" for UNBOUNDED FOLLOWING
        - None if not specified (default unbounded behavior)
        """
        return (self.get("operation", {}) or {}).get("following")

    def has_frame_spec(self) -> bool:
        """Check if this column has a frame specification (preceding or
        following)."""
        return self.preceding is not None or self.following is not None

    def is_window_type(self) -> bool:
        """Check if this column is a WINDOW type column."""
        return self.type.upper() == "WINDOW"

    def has_formula(self) -> bool:
        """Check if this column actually has a non-empty formula."""
        if type(self.formula) in (int, float, bool):
            return True
        elif self.formula is None:
            return False
        return bool(self.formula.strip())

    def has_window_function(self) -> bool:
        """Check if this column has a valid window function."""
        return bool(self.window_function)
