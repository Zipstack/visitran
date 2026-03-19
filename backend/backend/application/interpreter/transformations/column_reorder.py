from backend.application.interpreter.constants import TemplateNames
from backend.application.interpreter.transformations.base_transformation import BaseTransformation


class ColumnReorderTransformation(BaseTransformation):
    def construct_code(self) -> str:
        column_order = self.config_parser.presentation_parser.column_order
        if column_order:
            template_data = {
                "column_order": column_order,
            }
            self._transformed_code: str = self.template_render(
                template_file_name=TemplateNames.COLUMN_REORDER,
                template_content=template_data,
            )
            return self._transformed_code

        return ""

    def transform(self) -> str:
        return self.construct_code()
