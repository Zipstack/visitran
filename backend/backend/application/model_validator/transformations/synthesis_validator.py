from typing import Set

from backend.application.model_validator.transformations.base_validator import Validator


class SynthesisValidator(Validator):

    def validate_updated_transform(self):
        old_column_names: list[str] = self.old_parser.column_names
        new_column_names: list[str] = self.current_parser.column_names
        return [col for col in old_column_names if col not in new_column_names]

    def validate_deleted_transform(self):
        return self.old_parser.column_names

    def check_column_usage(self, columns: list[str]) -> list[str]:
        still_used: Set[str] = set()
        for expr in self.current_parser.referred_column_names:
            for col in columns:
                if col == expr or col in expr:
                    still_used.add(col)
        return list(still_used)
