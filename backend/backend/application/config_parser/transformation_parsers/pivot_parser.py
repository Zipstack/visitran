from typing import Any

from backend.application.interpreter.constants import Aggregations
from backend.application.config_parser.base_parser import BaseParser


class PivotParser(BaseParser):
    def __init__(self, config_data: dict[str, Any]):
        super().__init__(config_data)
        self._columns = None
        self._rows = None
        self._values_by = None
        self._aggregation = None

    @property
    def to_column_names(self) -> str:
        if not self._columns:
            self._columns = self.get("column")
        return self._columns

    @property
    def to_rows(self) -> str:
        if not self._rows:
            self._rows = self.get("row")
        return self._rows

    @property
    def values_from(self):
        if not self._values_by:
            self._values_by = self.get("summerize_by", {}).get("summerize_column")
        return self._values_by

    @property
    def aggregator(self):
        if not self._aggregation:
            aggregator = self.get("summerize_by", {}).get("aggregator")
            self._aggregation = Aggregations.mapper.get(aggregator)
        return self._aggregation

    @property
    def fill_null(self):
        return self.get("fill_null")
