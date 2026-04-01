from backend.application.model_validator.transformations.base_validator import Validator


class PivotValidator(Validator):

    def validate_new_transform(self) -> list[str]:
        used_columns = [self.current_parser.to_rows]
        return used_columns

    def validate_updated_transform(self) -> list[str]:
        missing_columns = []

        # Check if any of the pivot axes or values changed
        if self.current_parser.to_rows != self.old_parser.to_rows:
            missing_columns.append(self.old_parser.to_rows)

        if self.current_parser.to_column_names != self.old_parser.to_column_names:
            old_pivoted_columns = self.session.get_model_dependency_data(
                model_name=self.model_name,
                transformation_id=f"{self.current_parser.transform_id}_transform",
                default={},
            )
            missing_columns.extend(
                [col for col in old_pivoted_columns if col not in [self.current_parser.to_column_names]]
            )

        if self.current_parser.values_from != self.old_parser.values_from:
            missing_columns.append(self.old_parser.values_from)

        return missing_columns

    def validate_deleted_transform(self) -> list[str]:
        old_columns_details = self.session.get_model_dependency_data(
            model_name=self.model_name, transformation_id=f"{self.old_parser.transform_id}", default={}
        )
        old_columns = old_columns_details.get("column_names") or []
        new_columns_details = self.session.get_model_dependency_data(
            model_name=self.model_name, transformation_id=f"{self.old_parser.transform_id}_transformed", default={}
        )
        new_columns = new_columns_details.get("column_names") or []
        return [column for column in new_columns if column not in old_columns]

    def check_column_usage(self, columns: list[str]) -> list[str]:
        """Checks if any columns are used in the pivot transformation."""
        affected_columns = [
            column
            for column in columns
            if column
            in {self.current_parser.to_rows, self.current_parser.to_column_names, self.current_parser.values_from}
        ]

        return affected_columns
