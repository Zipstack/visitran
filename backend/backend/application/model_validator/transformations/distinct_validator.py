from backend.application.model_validator.transformations.base_validator import Validator


class DistinctValidator(Validator):

    def check_column_usage(self, columns: list[str]) -> list[str]:
        return [col for col in columns if col in self.current_parser.columns]
