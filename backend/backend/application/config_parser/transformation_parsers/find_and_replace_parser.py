from typing import Any

from backend.application.config_parser.base_parser import BaseParser


class EditOperations(BaseParser):
    def __init__(self, config_data: dict[str, Any]):
        super().__init__(config_data)

    @property
    def match_type(self):
        return self.get("match_type")

    @property
    def find_value(self):
        return self.get("find")

    @property
    def replace_value(self):
        return self.get("replace")


class FindAndReplaceColumns(BaseParser):
    def __init__(self, config_data: dict[str, Any]):
        super().__init__(config_data)
        self._operations = []

    @property
    def columns(self) -> list[str]:
        return self.get("column_list")

    @property
    def operations(self):
        if not self._operations:
            operation_datas: list = self.get("operation")
            for operation_data in operation_datas:
                self._operations.append(EditOperations(config_data=operation_data))
        return self._operations


class FindAndReplaceParser(BaseParser):
    def __init__(self, config_data: dict[str, Any]):
        super().__init__(config_data)
        self._edit_columns = self.get("replacements", [])
        self._colum_parser = []
        self._column_names = []

    @property
    def columns(self) -> list[FindAndReplaceColumns]:
        if not self._colum_parser:
            for col_data in self._edit_columns:
                self._colum_parser.append(FindAndReplaceColumns(col_data))
        return self._colum_parser

    @property
    def column_names(self) -> list[str]:
        if not self._column_names:
            for column in self.columns:
                self._column_names.extend(column.columns)
        return self._column_names
