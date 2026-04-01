from backend.application.config_parser.transformation_parsers.combine_parser import CombineColumnParser, CombineColumns
from backend.application.interpreter.constants import TemplateNames
from backend.application.interpreter.transformations.base_transformation import BaseTransformation


class CombineColumnTransformation(BaseTransformation):
    def __init__(self, parser: CombineColumnParser, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.combine_column_parser: CombineColumnParser = parser

    def _process_combine_columns(self, combine_columns: list[CombineColumns]) -> list[dict]:
        formula_statements = []
        for combine_column in combine_columns:
            formula_statement = self._create_formula_statement(combine_column)
            if formula_statement:
                formula_statements.append(formula_statement)
        return formula_statements

    def _create_formula_statement(self, combine_column: CombineColumns):
        target_column = combine_column.get("columnName")
        values = combine_column.get("values", [])

        if not target_column or not values:
            return None

        formula_parts = self._get_formula_parts(values)
        if not formula_parts:
            print(f"Warning: No valid formula parts for target column: {target_column}")
            return None

        return {"target_column": target_column, "formula": " + ".join(formula_parts)}

    def _get_formula_parts(self, values: list) -> list[str]:
        formula_parts = []
        for value in values:
            part = self._process_value(value)
            if part:
                formula_parts.append(part)
        return formula_parts

    @staticmethod
    def _process_value(value: dict):
        try:
            value_type = value.get("type", "")
            value_content = value.get("value", "")

            if value_type == "value":
                return f"'{value_content}'"
            elif value_type == "column":
                return f"source_table.{value_content}.cast('string')"
            return None
        except Exception as e:
            print(f"Error processing value {value}: {e}")
            return None

    @staticmethod
    def _deduplicate_formulas(formula_statements: list[dict]) -> list[dict]:
        return list({f"{fs['target_column']}:{fs['formula']}": fs for fs in formula_statements}.values())

    def construct_code(self) -> str:
        combine_columns: list[CombineColumns] = self.combine_column_parser.columns
        formula_statements = self._process_combine_columns(combine_columns)
        combine_column_statements = self._deduplicate_formulas(formula_statements)

        template_data = {
            "combine_column_statements": combine_column_statements,
            "transformation_id": self.combine_column_parser.transform_id,
        }
        self._transformed_code: str = self.template_render(
            template_file_name=TemplateNames.COMBINE_COLUMN, template_content=template_data
        )
        return self._transformed_code

    def transform(self) -> str:
        return self.construct_code()
