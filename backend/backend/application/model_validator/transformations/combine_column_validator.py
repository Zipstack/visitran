from backend.application.model_validator.transformations.base_validator import Validator


class CombineColumnValidator(Validator):

    def validate_updated_transform(self):
        old_cols = {
            col.column_name
            for col in (self._old_parser.columns if self._old_parser else [])
            if hasattr(col, "column_name")
        }
        new_cols = {
            col.column_name
            for col in (self._current_parser.columns if self._current_parser else [])
            if hasattr(col, "column_name")
        }

        removed = old_cols - new_cols
        return list(removed)

    def validate_deleted_transform(self):
        old_cols = {
            col.column_name
            for col in (self._old_parser.columns if self._old_parser else [])
            if hasattr(col, "column_name")
        }
        return list(old_cols)

    def check_column_usage(self, columns: list[str]) -> list[str]:
        return [col for col in columns if col in self._current_parser.column_names]
