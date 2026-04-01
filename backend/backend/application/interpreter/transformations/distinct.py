from backend.application.config_parser.transformation_parsers.distinct_parser import DistinctParser
from backend.application.interpreter.constants import TemplateNames
from backend.application.interpreter.transformations.base_transformation import BaseTransformation


class DistinctTransformation(BaseTransformation):
    def __init__(self, parser: DistinctParser, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.distinct_parser: DistinctParser = parser

    @staticmethod
    def _get_ordered_statements(format_contents: dict) -> str:
        statement = """
    # Define a window partitioned by last_name without ordering
    window = ibis.window(
        group_by=source_table['{column_name}'],
        order_by=[source_table['{column_name}'].{order_by}()]
    )
    # Assign row numbers arbitrarily within each group
    ranked = source_table.mutate(row_num=ibis.row_number().over(window))
    # Keep only the first row for each last_name (arbitrary choice)
    source_table = ranked.filter(ranked['row_num'] == 0).drop(['row_num'])\n
        """
        return statement.format(**format_contents)

    def _parse_postgres_distinct(self, distinct_parser: DistinctParser) -> str:
        statements = ""
        group_columns = ""

        for column in distinct_parser.columns:
            format_contents = {"column_name": column, "order_by": "desc"}
            statements += self._get_ordered_statements(format_contents)
        return statements

    def construct_code(self):
        # distinct_content: str = ""
        groups_content = []
        for columns in self.distinct_parser.columns:
            groups_content.append(f"source_table['{columns}']")
        # if self.visitran_context.database_type == "postgres":
        #     pass
        # else:
        #     columns = ", ".join(self.distinct_parser.columns)
        #     distinct_content += f"""
        #     source_table = source_table.distinct(on=['{columns}'], keep='first')\n
        #     """

        template_data = {
            "group_columns": f'[{", ".join(groups_content)}]',
            "order_by": self.distinct_parser.columns[0],
            "transformation_id": self.distinct_parser.transform_id,
        }
        self._transformed_code: str = self.template_render(
            template_file_name=TemplateNames.DISTINCT, template_content=template_data
        )
        return self._transformed_code

    def transform(self) -> str | list[str]:
        return self.construct_code()
