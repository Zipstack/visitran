from copy import deepcopy

from backend.application.config_parser.transformation_parsers.groups_and_aggregation_parser import \
    GroupsAndAggregationParser
from backend.application.model_validator.transformations.base_validator import Validator


class GroupAndAggregationValidator(Validator):

    @staticmethod
    def _get_group_columns(groups: GroupsAndAggregationParser) -> list[str]:
        return deepcopy(groups.group_columns if groups else [])

    @staticmethod
    def _get_aggregate_columns(aggregate: GroupsAndAggregationParser) -> list[str]:
        return [col.alias for col in aggregate.aggregate_columns]

    def validate_new_transform(self) -> list[str]:
        group_columns = self._get_group_columns(self.current_parser)
        group_columns.extend(self._get_aggregate_columns(self.current_parser))
        return group_columns

    def validate_updated_transform(self) -> list[str]:
        new_group_columns = self._get_aggregate_columns(self.current_parser)
        old_group_columns = self._get_aggregate_columns(self.old_parser)
        return [col for col in old_group_columns if col not in new_group_columns]

    def validate_deleted_transform(self) -> list[str]:
        return self._get_aggregate_columns(self.old_parser)

    def check_column_usage(self, columns: list[str]) -> list[str]:
        # Collect all columns used in this filter's criteria
        used_columns = set(self.current_parser.column_names)

        # Intersection with columns that should have been removed
        return list(used_columns.intersection(columns))
