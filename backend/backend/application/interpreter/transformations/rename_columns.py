from backend.application.config_parser.transformation_parsers.rename_parser import RenameParser, RenameParsers
from backend.application.interpreter.constants import TemplateNames
from backend.application.interpreter.transformations.base_transformation import BaseTransformation


class RenameColumnTransformation(BaseTransformation):
    def __init__(self, parser: RenameParsers, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.rename_parsers: RenameParsers = parser

    def _parse_rename_transformations(self) -> list[str]:
        rename_parsers: list[RenameParser] = self.rename_parsers.get_rename_parsers()
        rename_statements = []

        for rename_parser in rename_parsers:
            # Generate the mutate/rename statement for renaming columns
            rename_statement = f".rename({{'{rename_parser.new_name}': '{rename_parser.old_name}'}})"
            rename_statements.append(rename_statement)

        return rename_statements

    def construct_code(self) -> str:
        rename_transformations: list[str] = self._parse_rename_transformations()
        template_data = {
            "rename_transformations": rename_transformations,
            "transformation_id": self.rename_parsers.transform_id,
        }
        self._transformed_code: str = self.template_render(
            template_file_name=TemplateNames.RENAME_COLUMN, template_content=template_data
        )
        return self._transformed_code

    def transform(self) -> str:
        return self.construct_code()
