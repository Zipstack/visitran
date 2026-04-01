from typing import Any

from backend.application.config_parser.base_parser import BaseParser
from backend.application.config_parser.transformation_parsers.filter_parser import FilterParser


class HavingParser(FilterParser):
    def __init__(self, config_data: dict[str, Any]):
        super().__init__(config_data)


class AggregationColumnsParser(BaseParser):
    @property
    def function(self) -> str:
        return self.get("function", "")

    @property
    def column(self) -> str:
        return self.get("column", "")

    @property
    def alias(self) -> str:
        return self.get("alias", "")

    @property
    def expression(self) -> str:
        """Expression field for formula-based aggregates (e.g.,
        'SUM(revenue)/COUNT(*)')."""
        return self.get("expression", "")

    @property
    def is_formula_aggregate(self) -> bool:
        """Check if this is a formula-based aggregate (has expression
        field)."""
        return bool(self.expression)

    def validate(self) -> list[str]:
        """Validate aggregate column configuration."""
        errors = []

        if self.is_formula_aggregate:
            # Formula aggregate validation (has expression field)
            if not self.alias:
                errors.append("Formula aggregate requires an alias")
            if not self.expression:
                errors.append("Formula aggregate requires an expression")
        else:
            # Simple aggregate validation
            if not self.function:
                errors.append("Simple aggregate requires a function")
            if not self.column:
                errors.append("Simple aggregate requires a column")

        return errors



class GroupsAndAggregationParser(BaseParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._having_parser = None
        self._filter_parser = None
        self._aggregation_column_parser = []
        self._aggregation_column_names = []

    @property
    def group_columns(self) -> list[str]:
        return self.get("group", [])

    @property
    def aggregate_columns(self) -> list[AggregationColumnsParser]:
        if not self._aggregation_column_parser:
            if aggregate_datas := self.get("aggregate_columns", {}):
                for aggregate_data in aggregate_datas:
                    self._aggregation_column_parser.append(AggregationColumnsParser(config_data=aggregate_data))
        return self._aggregation_column_parser

    @property
    def column_names(self) -> list[str]:
        if not self._aggregation_column_names:
            for aggregation_column in self.aggregate_columns:
                self._aggregation_column_names.append(aggregation_column.column)
        return self._aggregation_column_names

    @property
    def having(self) -> HavingParser:
        if having_data := self.get("having", {}):
            self._having_parser: HavingParser = HavingParser(config_data=having_data)
        return self._having_parser

    @property
    def filter(self) -> FilterParser:
        if filter_data := self.get("filter", {}):
            self._filter_parser: FilterParser = FilterParser(config_data=filter_data)
        return self._filter_parser
