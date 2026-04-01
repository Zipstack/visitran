from pathlib import Path
import re
from re import sub
from typing import Any

import ibis
from ibis.common import exceptions as ib_exceptions
from jinja2 import Environment, FileSystemLoader
from visitran.errors import VisitranBaseExceptions

from backend.application.config_parser.config_parser import ConfigParser
from backend.application.utils import get_class_name
from backend.application.visitran_backend_context import VisitranBackendContext
from backend.application.utils import replace_notin, replace_in

TEMPLATES_PATH = (f"{Path(__file__).parent.parent.parent.parent}"
                  f"/application/interpreter/python_templates/transformations_template/")


class BaseTransformation:
    def __init__(
            self,
            config_parser: ConfigParser,
            context: VisitranBackendContext
    ):
        self.config_parser = config_parser
        self.visitran_context = context
        self.source_file_name = f"{self.config_parser.destination_schema_name}_{self.config_parser.destination_table_name}"
        self.source_class_name = self.build_source_class_name()
        self._headers: list[str] = []
        self._content: list[str] = []
        self._transformed_code: str = ""
        self.parent_classes: list = []

    def build_source_class_name(self) -> str:
        source_class_name = get_class_name(self.source_file_name)
        if source_class_name == get_class_name(self.config_parser.model_name):
            return f"{source_class_name}Source"
        return source_class_name

    @property
    def headers(self) -> list[str]:
        return self._headers

    def add_headers(self, header: str) -> None:
        self._headers.append(header)

    @staticmethod
    def get_class_var_str(class_name: str) -> str:
        return "_".join(
            sub(
                "([A-Z][a-z]+)",
                r" \1",
                sub("([A-Z]+)", r" \1", class_name.replace("-", " ")),
            ).split()
        ).lower()

    @staticmethod
    def template_render(template_file_name: str, template_content: dict[str, Any]):
        """This method creates a file using jinja template render."""
        environment = Environment(loader=FileSystemLoader(TEMPLATES_PATH))
        template = environment.get_template(f"{template_file_name}.jinja")
        content = template.render(template_content)
        return content

    def add_content(self, content: str) -> None:
        self._content.append(content)

    @property
    def default_database(self) -> str:
        if self.visitran_context.db_adapter.db_connection.dbtype == 'bigquery':
            return self.visitran_context.db_adapter.db_connection.dataset_id
        return self.visitran_context.db_adapter.db_connection.dbname

    @property
    def content(self) -> list[str]:
        return self._content

    def transform(self, *args):
        """This method has to be implemented in child classes."""
        raise NotImplementedError

    def get_table_columns_with_type(self, schema_name: str, table_name: str) -> list[dict[str, Any]]:
        try:
            return self.visitran_context.get_table_columns_with_type(
                schema_name=self.config_parser.destination_schema_name,
                table_name=self.config_parser.destination_table_name
            )
        except (VisitranBaseExceptions, ib_exceptions.IbisError):
            return []

    def get_column_db_type(self, column_name: str, transformation_id: str) -> str:
        all_columns = []
        if transformation_id:
            transformation_columns: dict[str, Any] = self.visitran_context.session.get_model_dependency_data(
                model_name=self.config_parser.model_name,
                transformation_id=transformation_id,
                default=dict()
            )
            all_columns = [values for _, values in transformation_columns.get("column_description", {}).items()]
        if not all_columns:
            all_columns = self.get_table_columns_with_type(
                schema_name=self.config_parser.destination_schema_name,
                table_name=self.config_parser.destination_table_name
            )
        column_details = {}
        for column in all_columns:
            column_details[column['column_name']] = column['column_dbtype'].lower()

        col_db_type = column_details.get(column_name) or "string"
        if col_db_type.startswith("int") or col_db_type.startswith("number"):
            return "Number"
        return col_db_type

    def is_number_type(self, column_name: str, transformation_id: str) -> bool:
        col_db_type = self.get_column_db_type(column_name=column_name, transformation_id=transformation_id)
        return col_db_type == "Number"

    @property
    def transformed_code(self) -> str:
        return self._transformed_code

    @staticmethod
    def synthesis_formula_checks(formula: str) -> str:
        """
        Normalize SQL-style operators in formulas into Excel-style syntax
        compatible with the `formulas` parser (FormulaSQL safe).

        Transformations performed:
            - Replace "!=" with "<>"
            - Replace "x = TRUE" with "x"
            - Replace "x = FALSE" with "NOT(x)"
            - Replace comparisons with "" (empty string) into ISBLANK() / NOT(ISBLANK())
            - Convert FIXED() to YEAR()/MONTH()/DAY() where applicable
            - Convert generic FIXED(x,0) into ROUND(x,0)
            - Ensure TODAY / NOW have parentheses
        Args:
            formula (str): The raw formula string in SQL-like syntax.

        Returns:
            str: The normalized formula string in Excel-like syntax.
        """

        formula = re.sub(
            r"NOTIN\s*\(\s*([^)]+)\)",
            lambda m: replace_notin(m),
            formula,
            flags=re.IGNORECASE,
        )

        formula = re.sub(
            r"\bIN\s*\(\s*([^)]+)\)",
            lambda m: replace_in(m),
            formula,
            flags=re.IGNORECASE,
        )

        # Replace "!=" with "<>"
        formula = re.sub(r"\s*!=\s*", "<>", formula)

        # Replace "= TRUE" with just the variable
        formula = re.sub(
            r"(\w+)\s*=\s*TRUE",
            r"\1",
            formula,
            flags=re.IGNORECASE,
        )

        # Replace "= FALSE" with NOT(variable)
        formula = re.sub(
            r"(\w+)\s*=\s*FALSE",
            r"NOT(\1)",
            formula,
            flags=re.IGNORECASE,
        )

        #  Replace <> ""  → NOT(ISBLANK(x))
        formula = re.sub(
            r"(\b[A-Za-z_][A-Za-z0-9_]*\b)\s*<>\s*\"\"",
            r"NOT(ISBLANK(\1))",
            formula,
        )

        # Replace = ""  → ISBLANK(x)
        formula = re.sub(
            r"(\b[A-Za-z_][A-Za-z0-9_]*\b)\s*=\s*\"\"",
            r"ISBLANK(\1)",
            formula,
        )

        # Replace FIXED(YEAR(x), 0) → YEAR(x)
        formula = re.sub(r"FIXED\(\s*(YEAR\([^)]*\))\s*,\s*0\s*\)", r"\1", formula, flags=re.IGNORECASE)
        formula = re.sub(r"FIXED\(\s*(MONTH\([^)]*\))\s*,\s*0\s*\)", r"\1", formula, flags=re.IGNORECASE)
        formula = re.sub(r"FIXED\(\s*(DAY\([^)]*\))\s*,\s*0\s*\)", r"\1", formula, flags=re.IGNORECASE)

        #Fallback: generic FIXED(x, 0) → ROUND(x, 0)
        # This safely handles expressions like FIXED(FLOOR(YEAR(date)/10)*10, 0)
        formula = re.sub(r"\bFIXED\s*\(\s*([^,]+)\s*,\s*0\s*\)", r"ROUND(\1,0)", formula, flags=re.IGNORECASE)

        # Ensure TODAY / NOW have parentheses
        formula = re.sub(r"\bTODAY\b(?!\s*\()", "TODAY()", formula)
        formula = re.sub(r"\bNOW\b(?!\s*\()", "NOW()", formula)

        formula = re.sub(
            r"\bFIND\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*1\s*\)",
            r"FIND(\1, \2)",
            formula,
            flags=re.IGNORECASE
        )

        return formula
