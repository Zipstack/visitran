from typing import Any

from backend.application.config_parser.base_parser import BaseParser


class RenameParser(BaseParser):
    @property
    def old_name(self) -> str:
        return self.get("old_name", "")

    @property
    def new_name(self) -> str:
        return self.get("new_name", "")

class RenameParsers(BaseParser):
    def __init__(self, config_data: dict[str, Any]):
        super().__init__(config_data)
        self._rename_parsers: list[RenameParser] = []
        self._rename_columns: list[str] = []

    def get_rename_parsers(self) -> list[RenameParser]:
        if not self._rename_parsers:
            for rename_data in self.get("mappings", []):
                self._rename_parsers.append(RenameParser(config_data=rename_data))
        return self._rename_parsers

    @property
    def column_names(self) -> list[str]:
        if not self._rename_columns:
            for rename_parser in self.get_rename_parsers():
                self._rename_columns.append(rename_parser.old_name)
        return self._rename_columns

    @property
    def new_column_names(self) -> list[str]:
        return [rp.new_name for rp in self.get_rename_parsers()]