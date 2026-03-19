from typing import Any

from backend.application.config_parser.base_parser import BaseParser
from backend.application.config_parser.transformation_parsers.filter_parser import FilterParser


class JoinParser(BaseParser):
    def __init__(self, config_data: dict[str, Any]):
        super().__init__(config_data)
        self._filter_parser = None

    @property
    def lhs_schema_name(self) -> str:
        return self.get("source", {}).get("schema_name")

    @property
    def lhs_table_name(self) -> str:
        return self.get("source", {}).get("table_name")

    @property
    def lhs_column_name(self) -> str:
        return self.get("source", {}).get("column_name")

    @property
    def operator(self) -> str:
        return self.get("operator", "")

    @property
    def rhs_schema_name(self) -> str:
        return self.get("joined_table", {}).get("schema_name")

    @property
    def rhs_table_name(self) -> str:
        return self.get("joined_table", {}).get("table_name")

    @property
    def rhs_column_name(self) -> str:
        return self.get("joined_table", {}).get("column_name")

    @property
    def alias_name(self) -> str | None:
        return self.get("joined_table", {}).get("alias_name")

    @property
    def join_type(self) -> str:
        return self.get("type")

    @property
    def join_filter(self) -> FilterParser:
        if not self._filter_parser:
            self._filter_parser = FilterParser({"criteria": self.get("criteria", {})})
        return self._filter_parser


class JoinParsers(BaseParser):
    def __init__(self, config_data: dict[str, Any]):
        super().__init__(config_data)
        self._join_parsers: list[JoinParser] = []
        self._join_columns: list[str] = []

    def get_joins(self) -> list[JoinParser]:
        if not self._join_parsers:
            for join_payload in self._config_data.get("tables", []):
                self._join_parsers.append(JoinParser(join_payload))
        return self._join_parsers

    @property
    def join_columns(self) -> list[str]:
        if not self._join_columns:
            for join_parser in self.get_joins():
                self._join_columns.append(join_parser.lhs_column_name)
        return self._join_columns
