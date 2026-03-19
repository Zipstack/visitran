from backend.application.interpreter.constants import TemplateConstants, TemplateNames
from backend.application.interpreter.transformations.base_transformation import BaseTransformation


class SourceModelTransformation(BaseTransformation):
    def _parse_source_table(self, parent_class: str) -> str:
        """This method generates the first class in the python file."""
        # When parent_class is a reference model (not VisitranModel), call its select() method
        # to get the data instead of reading from source_table_obj (which reads from DB)
        if parent_class == "VisitranModel":
            declaration = "source_table = self.source_table_obj"
        else:
            declaration = f"source_table = {parent_class}().select()"
        template_data = {
            TemplateConstants.CLASS_NAME: self.source_class_name,
            TemplateConstants.PARENT_CLASS: parent_class,
            TemplateConstants.SOURCE_SCHEMA_NAME: self.config_parser.source_schema_name,
            TemplateConstants.SOURCE_TABLE_NAME: self.config_parser.source_table_name,
            TemplateConstants.DESTINATION_SCHEMA_NAME: self.config_parser.destination_schema_name,
            TemplateConstants.DESTINATION_TABLE_NAME: self.config_parser.destination_table_name,
            TemplateConstants.DATABASE_NAME: self.default_database,
            TemplateConstants.PARENT_DECLARATION: declaration,
        }

        self._transformed_code: str = self.template_render(
            template_file_name=TemplateNames.EPHEMERAL_TABLE, template_content=template_data
        )
        return self._transformed_code

    def transform(self, parent_class: str) -> str:
        return self._parse_source_table(parent_class=parent_class)
