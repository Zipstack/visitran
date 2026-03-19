from typing import Any

from backend.application.config_parser.base_parser import BaseParser
from backend.application.config_parser.transformation_parsers.column_parser import ColumnParser


class WindowParser(BaseParser):
    """Parser for window transform type configurations.

    Parses window transform configurations with structure:
    {
        "window": {
            "columns": [
                {
                    "column_name": "row_num",
                    "operation": {
                        "function": "ROW_NUMBER",
                        "partition_by": ["dept"],
                        "order_by": [{"column": "salary", "direction": "DESC"}]
                    }
                }
            ]
        }
    }
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._column_schemas: list[dict[str, Any]] = self.get("columns", [])
        self._column_parsers: list[ColumnParser] = []
        self._column_names: list[str] = []

    @property
    def columns(self) -> list[ColumnParser]:
        if not self._column_parsers:
            for column_data in self._column_schemas:
                self._column_parsers.append(ColumnParser(column_data))
        return self._column_parsers

    @property
    def column_names(self) -> list[str]:
        if not self._column_names:
            for column_parser in self.columns:
                self._column_names.append(column_parser.column_name)
        return self._column_names
