from typing import Any

from backend.application.config_parser.base_parser import BaseParser
from backend.application.config_parser.transformation_parsers.condition_parser import ConditionParser


class FilterParser(BaseParser):
    def __init__(self, config_data: dict[str, Any]):
        super().__init__(config_data)
        self._condition_objs: list[ConditionParser] = []
        self._column_names = []

    @property
    def conditions(self) -> list[ConditionParser]:
        if not self._condition_objs:
            condition_list = self.get("criteria", {})
            for condition in condition_list:
                self._condition_objs.append(ConditionParser(condition))
        return self._condition_objs

    @property
    def column_names(self) -> list[str]:
        if not self._column_names:
            for condition in self.conditions:
                if lhs_column := condition.lhs_column:
                    self._column_names.append(lhs_column.column_name)
                if rhs_column := condition.rhs_column:
                    self._column_names.append(rhs_column.column_name)
        return self._column_names

    # Inside BaseParser or FilterParser
    def has_column(self, column_name: str) -> bool:
        return column_name in self.column_names

