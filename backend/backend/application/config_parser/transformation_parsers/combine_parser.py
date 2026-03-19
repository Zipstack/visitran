from typing import Any

from backend.application.config_parser.base_parser import BaseParser


class CombineValues(BaseParser):
    def __init__(self, config_data: dict[str, Any]):
        super().__init__(config_data)

    @property
    def type(self):
        return self.get("type")

    @property
    def value(self):
        return self.get("value")

    @property
    def column(self) -> str | None:
        if self.type == "column":
            return self.get("value")


class CombineColumns(BaseParser):
    def __init__(self, config_data: dict[str, Any]):
        super().__init__(config_data)
        self._values = []

    @property
    def column_name(self) -> str:
        return self.get("columnName")

    @property
    def values(self) -> list[CombineValues]:
        if not self._values:
            value_datas: list = self.get("values")
            for value_data in value_datas:
                self._values.append(CombineValues(config_data=value_data))
        return self._values


class CombineColumnParser(BaseParser):
    def __init__(self, config_data: dict[str, Any]):
        super().__init__(config_data)
        self._combine_columns = self.get("columns", [])
        self._column_parser = []
        self._column_names = []

    @property
    def columns(self) -> list[CombineColumns]:
        if not self._column_parser:
            for col_data in self._combine_columns:
                self._column_parser.append(CombineColumns(col_data))
        return self._column_parser

    @property
    def column_names(self) -> list[str]:
        if not self._column_names:
            for column in self.columns:
                for combine_value in column.values:
                    if column_data := combine_value.column:
                        self._column_names.append(column_data)
        return self._column_names
