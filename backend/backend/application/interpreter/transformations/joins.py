from typing import Any
import uuid
from backend.application.config_parser.transformation_parsers.filter_parser import FilterParser
from backend.application.config_parser.transformation_parsers.join_parser import JoinParser, JoinParsers
from backend.application.interpreter.constants import JoinTypes, OperatorsToIbis, TemplateConstants, TemplateNames
from backend.application.interpreter.transformations.base_transformation import BaseTransformation
from backend.application.utils import get_class_name


class JoinTransformation(BaseTransformation):
    def __init__(self, parser: JoinParsers, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.join_parsers: JoinParsers = parser
        self.join_statements: list[str] = []
        self.join_classes: list[str] = []
        self.join_headers: list[str] = []
        self._duplicated_classes: list[str] = []
        self._schema_table_mapper: dict[str, Any] = {}

    def _get_join_declaration(self, class_name: str, alias_name: str | None = None) -> str:
        if alias_name:
            join_alias = alias_name + f": Table = {self.get_class_var_str(class_name=class_name)}.view()"
            return join_alias
        return self.get_class_var_str(class_name=class_name) + f": Table = {class_name}().select()"

    def __parse_left_joins(self, join_parser: JoinParser) -> tuple[bool, str]:
        class_name = get_class_name(join_parser.rhs_table_name)
        if class_name in self.join_classes:
            self.join_statements.append(self._get_join_declaration(class_name=class_name))
            return True, class_name
        declaration = "source_table = self.source_table_obj"
        for reference_name, reference_model in self.config_parser.all_reference.items():
            if (
                reference_model.get("destination_table") == join_parser.rhs_table_name
                and reference_model.get("destination_schema") == join_parser.rhs_schema_name
            ):
                class_name = get_class_name(reference_name)
                importer = f"from {self.visitran_context.project_py_name}.models.{reference_name} import {class_name}"
                if importer not in self.join_headers:
                    self.join_headers.append(importer)
                self.join_classes.append(class_name)
                if join_parser.alias_name:
                    self.join_statements.append(self._get_join_declaration(class_name=class_name))
                return True, class_name
        self.join_classes.append(class_name)

        template_data = {
            TemplateConstants.CLASS_NAME: class_name,
            TemplateConstants.PARENT_CLASS: "VisitranModel",
            TemplateConstants.SOURCE_SCHEMA_NAME: join_parser.rhs_schema_name,
            TemplateConstants.SOURCE_TABLE_NAME: join_parser.rhs_table_name,
            TemplateConstants.DESTINATION_SCHEMA_NAME: self.config_parser.destination_schema_name,
            TemplateConstants.DESTINATION_TABLE_NAME: join_parser.rhs_table_name,
            TemplateConstants.DATABASE_NAME: self.default_database,
            TemplateConstants.PARENT_DECLARATION: declaration,
        }

        content: str = self.template_render(
            template_file_name=TemplateNames.EPHEMERAL_TABLE,
            template_content=template_data,
        )

        if join_parser.alias_name:
            self.join_statements.append(self._get_join_declaration(class_name=class_name))

        self.add_content(content)
        return False, class_name

    @staticmethod
    def _get_join_type(join_type: str) -> str:
        return JoinTypes.get_join_type(join_type)

    def _parse_join_filters(
        self,
        right_table: str,
        join_parser: JoinParser,
        filter_parser: FilterParser,
    ) -> str:
        join_type = self._get_join_type(join_parser.join_type)
        if join_type == "cross_join":
            return f".{join_type}({right_table}, rname='{right_table}_{{name}}')"
        filter_string = f".{join_type}({right_table}, [("
        conditions = filter_parser.conditions

        for count, condition in enumerate(conditions):
            if count != 0:
                filter_string += "), ("
            lhs_column_name: str = condition.lhs_column.column_name
            operator = condition.operator
            ibis_operator = OperatorsToIbis.JOIN_MAPPER.get(operator)
            rhs_column_name: str = condition.rhs_column.column_name
            # filter_string += f"source_table['{lhs_column_name}'] {ibis_operator} {right_table}['{right_table}_{rhs_column_name}']"
            filter_string += f"source_table['{lhs_column_name}'] {ibis_operator} {right_table}['{rhs_column_name}']"

        # Generate a stable unique suffix per join to avoid collisions
        add_right_table_column_name_prefix = (
                filter_string
                + f")], rname='{right_table}_{{name}}')"
        )

        return add_right_table_column_name_prefix

    def parse_joins(self, join_list: list[JoinParser]):
        # Determine the class name for the joined table
        joined_class_name = f"{get_class_name(self.config_parser.model_name)}JoinedTable"

        parent_classes_declarations: list[str] = []
        join_filters: list[str] = []

        for join_parser in join_list:
            # Parse the left join
            join_successful, class_name = self.__parse_left_joins(join_parser=join_parser)

            # If join parsing failed, store parent class
            if not join_successful:
                self.parent_classes.append(class_name)

            # Generate and store the join declaration
            parent_classes_declarations.append(self._get_join_declaration(class_name, join_parser.alias_name))

            # Renaming the joined columns to support self join support
            # alias_name = join_parser.alias_name or self.get_class_var_str(class_name)
            # for_compr = "{f'" + f"{alias_name}" +"_{col}': col " + f"for col in {alias_name}.columns" + "}"
            # rename_mapper = f"{alias_name}.rename({for_compr})"
            # join_alias = alias_name + f": Table = {rename_mapper}"
            # parent_classes_declarations.append(join_alias)

            # Prepare the join filter
            join_filter: FilterParser = join_parser.join_filter
            rhs_class_name: str = join_parser.alias_name or self.get_class_var_str(class_name)
            joined_values = f"source_table{self._parse_join_filters(rhs_class_name, join_parser, join_filter)}"

            # Append the filtered assignment
            join_filters.append(f"source_table = {joined_values}")

        # Extend the join statements in order
        self.join_statements.extend(parent_classes_declarations)
        self.join_statements.extend(join_filters)

        return joined_class_name


    def construct_code(self):
        join_list: list[JoinParser] = self.join_parsers.get_joins()
        self.parse_joins(join_list=join_list)

        template_data = {
            "join_statements": self.join_statements,
            "transformation_id": self.join_parsers.transform_id,
        }
        self._transformed_code: str = self.template_render(
            template_file_name=TemplateNames.JOIN, template_content=template_data
        )
        return self._transformed_code

    def transform(self) -> str:
        return self.construct_code()
