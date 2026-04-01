from backend.application.model_validator.transformations.base_validator import Validator


class FilterValidator(Validator):

    def check_column_usage(self, columns: set[str]) -> list[str]:
        # Collect all columns used in this filter's criteria
        used_columns = set(self.current_parser.column_names)

        # Intersection with columns that should have been removed
        return list(used_columns.intersection(columns))

