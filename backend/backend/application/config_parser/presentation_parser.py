from backend.application.config_parser.base_parser import BaseParser


class PresentationParser(BaseParser):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._sort_columns = []

    @property
    def hidden_columns(self):
        columns = self.get("columns", None)
        if columns == ["*"]:
            return None
        return self.get("columns", None)

    @property
    def sort(self) -> list[dict[str, str]]:
        return self.get("sort", [])

    @property
    def column_order(self) -> list[str]:
        return self.get("column_order", [])

    @property
    def sort_columns(self) -> list[str]:
        if not self._sort_columns:
            for sort_field in self.sort:
                self._sort_columns.append(sort_field.get("column"))
        return self._sort_columns
