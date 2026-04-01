from backend.application.config_parser.transformation_parsers.filter_parser import FilterParser
from backend.application.config_parser.transformation_parsers.union_parser import UnionBranchParser, UnionParsers
from backend.application.interpreter.constants import TemplateConstants, TemplateNames
from backend.application.interpreter.transformations.base_transformation import BaseTransformation
from backend.application.interpreter.utils.filter_builder import FilterBuilder
from backend.application.utils import get_class_name


class UnionTransformation(BaseTransformation):
    # Mapping from user-friendly types to Ibis types
    IBIS_TYPE_MAP = {
        "VARCHAR": "string",
        "STRING": "string",
        "INTEGER": "int64",
        "INT": "int64",
        "BIGINT": "int64",
        "FLOAT": "float64",
        "DOUBLE": "float64",
        "DECIMAL": "decimal",
        "BOOLEAN": "boolean",
        "BOOL": "boolean",
        "DATE": "date",
        "TIMESTAMP": "timestamp",
    }

    def __init__(self, parser: UnionParsers, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.union_parsers: UnionParsers = parser
        self.union_headers: list[str] = []
        self.union_statements: list[str] = []
        self.union_classes: list[str] = []
        self.parent_classes: list[str] = []

    def _parse_table_unions(self, table: str, schema: str = None) -> tuple[bool, str]:
        class_name = get_class_name(table)
        declaration = "source_table = self.source_table_obj"

        # Use provided schema or fall back to default source schema
        table_schema = schema or self.config_parser.source_schema_name

        for reference_name, reference_model in self.config_parser.all_reference.items():
            if (
                reference_model.get("destination_table") == table
                and reference_model.get("destination_schema") == self.config_parser.destination_schema_name
            ):
                class_name = get_class_name(reference_name)
                importer = f"from {self.visitran_context.project_py_name}.models.{reference_name} import {class_name}"
                if importer not in self.union_headers:
                    self.union_headers.append(importer)
                self.union_headers.append(class_name)
                return True, class_name
        self.union_headers.append(class_name)

        template_data = {
            TemplateConstants.CLASS_NAME: class_name,
            TemplateConstants.PARENT_CLASS: "VisitranModel",
            TemplateConstants.SOURCE_SCHEMA_NAME: table_schema,
            TemplateConstants.SOURCE_TABLE_NAME: table,
            TemplateConstants.DESTINATION_SCHEMA_NAME: self.config_parser.destination_schema_name,
            TemplateConstants.DESTINATION_TABLE_NAME: table,
            TemplateConstants.DATABASE_NAME: self.default_database,
            TemplateConstants.PARENT_DECLARATION: declaration,
        }

        content = self.template_render(
            template_file_name=TemplateNames.EPHEMERAL_TABLE,
            template_content=template_data,
        )

        self.add_content(content)
        return False, class_name

    def _get_union_declaration(
        self, class_name: str, mapping: list[dict], filter_parser: FilterParser = None
    ) -> list[str]:
        _class_obj = self.get_class_var_str(class_name=class_name)
        table = _class_obj + f": Table = {class_name}().select() "

        declarations = [
            table,
        ]

        # Add filters if specified using FilterBuilder
        if filter_parser:
            filter_expr = FilterBuilder.build_filter_expression(_class_obj, filter_parser)
            if filter_expr:
                declarations.append(filter_expr)

        declarations.append(
            f"{_class_obj} = self.prepare_child_table(child_obj={_class_obj}, parent_obj=source_table, mappings={mapping})"
        )

        return declarations

    def _build_branch_select(self, branch: UnionBranchParser, branch_index: int) -> tuple[list[str], str]:
        """Build SELECT statement for a branch (new branch-based format).

        Returns:
            tuple: (declarations, table_var_name)
        """
        table_name = branch.table
        table_schema = branch.schema
        table_var = f"branch_{branch_index}_table"

        declarations = []

        # Branch 0 is the source table - select from already-filtered source_table
        if branch_index == 0:
            # Use the source_table which may have source-level filters already applied
            declarations.append(f"{table_var}: Table = source_table")
        else:
            # For other branches, select from the table class
            # Check if this table needs to be imported or created as ephemeral
            flag, class_name = self._parse_table_unions(table=table_name, schema=table_schema)
            if not flag:
                self.parent_classes.append(class_name)

            # Start with base table selection
            declarations.append(f"{table_var}: Table = {class_name}().select()")

        # Apply filters if present
        if branch.filters:
            filter_expr = FilterBuilder.build_filter_expression(table_var, branch.filters)
            if filter_expr:
                declarations.append(filter_expr)

        # Build SELECT expressions for each column
        select_exprs = []
        for col_expr in branch.get_column_expressions():
            output_col = col_expr.output_column
            expr_type = col_expr.expression_type
            cast_type = col_expr.cast_type

            # Build base expression based on type
            if expr_type == "COLUMN":
                # Column reference: table["column_name"]
                col_name = col_expr.column_name
                base_expr = f'{table_var}["{col_name}"]'

            elif expr_type == "LITERAL":
                # Literal value: ibis.literal(value)
                literal_val = col_expr.literal_value
                literal_type = col_expr.literal_type

                # Quote literal based on type
                if literal_type in ["String", "Date"]:
                    quoted_val = f"'{literal_val}'"
                else:
                    quoted_val = literal_val

                base_expr = f"ibis.literal({quoted_val})"

            elif expr_type == "FORMULA":
                # Formula (future enhancement)
                formula = col_expr.formula
                base_expr = f"({formula})"

            else:
                # Default fallback
                base_expr = f'{table_var}["{output_col}"]'

            # Apply CAST if cast_type is specified
            if cast_type:
                ibis_type = self.IBIS_TYPE_MAP.get(cast_type.upper(), "string")
                base_expr = f'{base_expr}.cast("{ibis_type}")'

            # Add output column name
            select_exprs.append(f'{base_expr}.name("{output_col}")')

        # Build final SELECT statement
        select_list = ", ".join(select_exprs)
        declarations.append(f"{table_var} = {table_var}.select({select_list})")

        return declarations, table_var

    def _parse_union(self, union_list, join_classes=None):
        if join_classes is None:
            join_classes = []

        parent_classes_declarations: list[str] = []

        _unions = []
        _union_vals = ""

        # map child tables column with parent and collect filters
        mapping = {}
        table_filters = {}
        for union in union_list:
            if union.merge_table:
                child_table = union.merge_table
                column_map = {union.source_column: union.merge_column}
                if child_table in mapping:
                    mapping[child_table].update(column_map)
                else:
                    mapping[child_table] = column_map

                # Store FilterParser for this table (last one wins if multiple rows for same table)
                if union.filters:
                    table_filters[child_table] = union.filters

        for key in mapping:
            _table_name = key
            class_name = get_class_name(_table_name)
            if class_name not in join_classes:
                flag, class_name = self._parse_table_unions(table=_table_name)
                if not flag:
                    self.parent_classes.append(class_name)
            parent_classes_declarations += self._get_union_declaration(
                class_name, mapping=mapping.get(_table_name), filter_parser=table_filters.get(_table_name)
            )

            rhs_class_name: str = self.get_class_var_str(class_name)
            _union_vals += f"{rhs_class_name}, "

        if _union_vals:
            _union_vals = "source_table" + ".union" + "(" + _union_vals
            _union_vals += f"distinct={self.union_parsers.unions_duplicate})"
            _unions.append(f"source_table = {_union_vals}")

        self.union_statements.extend(parent_classes_declarations)
        self.union_statements.extend(_unions)

    def _parse_branch_based_union(self, join_classes=None):
        """Parse new branch-based union format."""
        if join_classes is None:
            join_classes = []

        branches = self.union_parsers.get_branch_parsers()

        if not branches:
            return

        # Validate: need at least 2 branches for UNION (branch 0 is source, branch 1 is first user branch)
        # Note: Frontend now sends source table as branch 0 automatically
        if len(branches) < 2:
            raise ValueError(
                f"UNION requires at least 2 branches (source + 1 user branch), but only {len(branches)} found"
            )

        # Apply source-level filters if present
        source_filters = self.union_parsers.source_filters
        if source_filters:
            filter_expr = FilterBuilder.build_filter_expression("source_table", source_filters)
            if filter_expr:
                self.union_statements.append(filter_expr)

        # Build each branch SELECT
        branch_vars = []
        for idx, branch in enumerate(branches):
            declarations, table_var = self._build_branch_select(branch, idx)
            self.union_statements.extend(declarations)
            branch_vars.append(table_var)

        # Build UNION statement
        if branch_vars and len(branch_vars) >= 2:
            distinct_flag = self.union_parsers.unions_duplicate
            union_statement = (
                f"source_table = {branch_vars[0]}.union({', '.join(branch_vars[1:])}, distinct={distinct_flag})"
            )
            self.union_statements.append(union_statement)

    def _parse_union_transformations(self, join_classes=None):
        if join_classes is None:
            join_classes = []

        # Check if this is branch-based or table-based format
        if self.union_parsers.is_branch_based():
            self._parse_branch_based_union(join_classes=join_classes)
        else:
            # Legacy table-based format
            union_list = self.union_parsers.get_union_parsers()
            if union_list:
                self._parse_union(union_list=union_list, join_classes=join_classes)

    def construct_code(self, join_classes=None):
        self._parse_union_transformations(join_classes=join_classes)
        template_data = {
            "union_statements": self.union_statements,
            "transformation_id": self.union_parsers.transform_id,
        }
        self._transformed_code: str = self.template_render(
            template_file_name=TemplateNames.UNION, template_content=template_data
        )
        return self._transformed_code

    def transform(self, join_classes=None):
        return self.construct_code(join_classes=join_classes)
