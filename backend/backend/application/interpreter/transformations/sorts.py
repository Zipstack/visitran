from backend.application.interpreter.constants import SortOperators, TemplateNames
from backend.application.interpreter.transformations.base_transformation import BaseTransformation


class SortsTransformation(BaseTransformation):
    def parse_sort(self):
        sorted_values: str = ""
        sort_fields = self.config_parser.presentation_parser.sort
        for count, sort_field in enumerate(sort_fields):
            if count != 0:
                sorted_values += ", "

            order_by = sort_field.get("order_by")
            column = sort_field.get("column")

            sorted_values += SortOperators.SORT_MAPPERS.get(order_by).format(value=column)

        return sorted_values

    def construct_code(self) -> str:
        if sorted_values := self.parse_sort():
            template_data = {
                "sort_statements": f".order_by([{sorted_values}])",
            }
            self._transformed_code: str = self.template_render(
                template_file_name=TemplateNames.SORT, template_content=template_data
            )
            return self._transformed_code

        return ""

    def transform(self) -> str:
        return self.construct_code()
