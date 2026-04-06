
from backend.application.config_parser.transformation_parsers.pivot_parser import PivotParser
from backend.application.interpreter.constants import TemplateNames
from backend.application.interpreter.transformations.base_transformation import BaseTransformation


class PivotTransformation(BaseTransformation):
    def __init__(self, parser: PivotParser, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.pivot_parser: PivotParser = parser

    def get_values_fill(self) -> str:
        if fill_null := self.pivot_parser.fill_null:
            if self.is_number_type(
                column_name=self.pivot_parser.values_from,
                transformation_id=self.pivot_parser.transform_id,
            ):
                try:
                    return f"values_fill={int(fill_null)})"
                except (ValueError, TypeError):
                    """
                    suppress the exception since the column type doesn't support string types
                    """
                    pass
            else:
                return f"values_fill='{fill_null}')"
        return "values_fill=None)"

    def _parse_pivot(self):
        pivot_statement = ""
        if self.pivot_parser.to_rows:
            pivot_statement = (f".pivot_wider("
                               f"id_cols='{self.pivot_parser.to_rows}', "
                               f"names_from='{self.pivot_parser.to_column_names}', "
                               f"values_from='{self.pivot_parser.values_from}', ")
            if self.pivot_parser.aggregator:
                pivot_statement += f"values_agg='{self.pivot_parser.aggregator}', "
            elif self.visitran_context.database_type == "postgres":
                pivot_statement += "values_agg='min', "
            pivot_statement += self.get_values_fill()
        return pivot_statement

    def compute_code(self):
        pivot_statements = self._parse_pivot()

        template_data = {
            "pivot_statements": pivot_statements,
            "transformation_id": self.pivot_parser.transform_id,
        }
        self._transformed_code: str = self.template_render(
            template_file_name=TemplateNames.PIVOT, template_content=template_data
        )
        return self._transformed_code


    def transform(self) -> str:
        return self.compute_code()
