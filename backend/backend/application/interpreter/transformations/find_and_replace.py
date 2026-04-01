from backend.application.config_parser.transformation_parsers.find_and_replace_parser import (
    FindAndReplaceColumns,
    FindAndReplaceParser,
)
from backend.application.interpreter.constants import FindAndReplaceConstants, TemplateNames
from backend.application.interpreter.transformations.base_transformation import BaseTransformation


class FindAndReplaceTransformation(BaseTransformation):
    def __init__(self, parser: FindAndReplaceParser, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.find_and_replace_parser: FindAndReplaceParser = parser

    def get_find_value_by_operator(self, match_value: str, find_value: str) -> str:
        if self.visitran_context.database_type == "snowflake" and match_value == "TEXT":
            # Handling explicitly for snowflake due to snowflake REGEX limitations
            snowflake_match_value = "(\\W|^){value}(\\W|$)"
            return snowflake_match_value.format(**{"value": find_value})
        if match_value in FindAndReplaceConstants.FIND_VALUE:
            return FindAndReplaceConstants.FIND_VALUE.get(match_value).format(**{"value": find_value})
        return find_value

    @staticmethod
    def is_regex(match_type: str) -> bool:
        return match_type in FindAndReplaceConstants.REGEX_PATTERN_OPERATORS

    def generate_formula(self, column_name, operator) -> str:
        match_type = operator.match_type
        find_value = self.get_find_value_by_operator(match_value=match_type, find_value=operator.find_value)
        replace_value = operator.replace_value
        if self.is_number_type(column_name, transformation_id=self.find_and_replace_parser.transform_id):
            # Cannot replace for integer column
            # Need to raise a proper exception
            raise ValueError("Integer value cannot be replaced")
        if match_type == FindAndReplaceConstants.FILL_NULL:
            fill_null_pattern = FindAndReplaceConstants.FIND_VALUE[FindAndReplaceConstants.FILL_NULL]
            return (
                f'source_table["{column_name}"]'
                f'.fillna("{replace_value}")'
                f'.re_replace("{fill_null_pattern}", "{replace_value}")'
            )
        if match_type == FindAndReplaceConstants.EMPTY:
            # fillna("") converts NULLs to empty strings so the regex can match them.
            # Regex ^\s*$ matches empty strings and whitespace-only strings.
            return f'source_table["{column_name}"].fillna("").re_replace(r"^\\s*$", "{replace_value}")'
        if self.is_regex(match_type):
            return f'source_table["{column_name}"].re_replace("{find_value}", "{replace_value}")'
        return f'source_table["{column_name}"].replace("{find_value}", "{replace_value}")'

    def _parse_find_and_replace_transformations(self) -> list[str]:
        find_replace_statements = []
        edit_columns: list[FindAndReplaceColumns] = self.find_and_replace_parser.columns
        for edit_column in edit_columns:
            columns = edit_column.columns
            operations = edit_column.operations
            for column in columns:
                for operator in operations:
                    formula = self.generate_formula(column, operator)
                    substitute = f".mutate({column}={formula})"
                    find_replace_statements.append(substitute)
        return find_replace_statements

    def construct_code(self) -> str:
        find_and_replace_transformations = self._parse_find_and_replace_transformations()

        template_data = {
            "find_and_replace_statements": find_and_replace_transformations,
            "transformation_id": self.find_and_replace_parser.transform_id,
        }
        self._transformed_code: str = self.template_render(
            template_file_name=TemplateNames.FIND_AND_REPLACE, template_content=template_data
        )
        return self._transformed_code

    def transform(self) -> list[str]:
        return self.construct_code()
