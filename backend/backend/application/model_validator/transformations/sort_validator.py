from backend.application.model_validator.transformations.base_validator import Validator


class SortValidator(Validator):

    def check_column_usage(self, columns: list[str]) -> list[str]:
        return [column for column in columns if column in self.current_parser.sort_columns]

