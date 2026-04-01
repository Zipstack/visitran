from backend.application.config_parser.transformation_parsers.rename_parser import RenameParsers
from backend.application.model_validator.transformations.base_validator import Validator


class RenameValidator(Validator):

    @staticmethod
    def _get_renamed_columns(parser: RenameParsers) -> list[str]:
        """
        Extracts the original (old) column names from rename mappings.
        """
        return parser.column_names

    def validate_new_transform(self) -> list[str]:
        return self._get_renamed_columns(self.current_parser)

    def validate_updated_transform(self) -> list[str]:
        new_renamed_columns = self._get_renamed_columns(self.current_parser)
        old_renamed_columns = self._get_renamed_columns(self._old_parser)
        missing_columns = list(set(new_renamed_columns) - set(old_renamed_columns))
        return missing_columns

    def validate_deleted_transform(self) -> list[str]:
        return self._old_parser.new_column_names

    def check_column_usage(self, columns: list[str]) -> list[str]:
        return [column for column in columns if column in self.current_parser.column_names]
