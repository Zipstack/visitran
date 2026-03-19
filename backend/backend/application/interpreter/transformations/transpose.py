from backend.application.interpreter.transformations.base_transformation import BaseTransformation


class TransposeTableTransformation(BaseTransformation):

    def transform(self) -> list[str]:
        # Table name passed to the transformation (assuming the config_parser has this)
        transpose_data = self.config_parser.transform_parser.get("transpose", {})

        if transpose_data.get("state"):
            column_list = transpose_data.get("columnList", [])
            # Ensure column list is a proper list of strings
            return [str(col) for col in column_list]

        return []
