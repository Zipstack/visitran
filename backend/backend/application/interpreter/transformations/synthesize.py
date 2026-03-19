from backend.application.config_parser.transformation_parsers.column_parser import ColumnParser
from backend.application.config_parser.transformation_parsers.synthesize_parser import SynthesizeParser
from backend.application.interpreter.constants import TemplateNames
from backend.application.interpreter.transformations.base_transformation import BaseTransformation


class SynthesizeTransformation(BaseTransformation):
    """Transformation for formula-based column synthesis.

    Handles FORMULA type columns that use FormulaSQL expressions to
    create new columns. Window functions are handled separately by
    WindowTransformation.
    """

    def __init__(self, parser: SynthesizeParser, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.synthesis_parser: SynthesizeParser = parser

    def add_synthesis_headers(self):
        self.add_headers("from formulasql.formulasql import FormulaSQL")
        self.add_headers("from visitran.errors import SynthesisColumnNotExist")

    def _parse_synthesis_transformations(self) -> list[str]:
        """Parse synthesis transformations from column parsers and return a
        list of Ibis mutate statements using FormulaSQL.

        Raises:
            ValueError: If a column does not have a formula.
        """
        self.add_synthesis_headers()
        synthesis_statements = []
        column_parsers: list[ColumnParser] = self.synthesis_parser.columns

        for column_parser in column_parsers:
            col_name = column_parser.column_name

            # Handle FORMULA type columns
            if not column_parser.has_formula():
                raise ValueError(f"No synthesis formula provided for column '{col_name}'")

            formula = column_parser.formula

            # Normalize formula string
            formula = str(formula)  # Ensure it is string
            formula = "".join(formula.split("\n"))  # Remove newlines
            formula = self.synthesis_formula_checks(formula)  # Custom checks

            # Escape single quotes for Python string embedding
            # SQL uses '' for escaped quotes (e.g., 'Nook''s Cranny')
            # Convert to \' for Python string literals
            formula_escaped = formula.replace("'", "\\'")

            # Construct the mutate statement
            statement = (
                f".mutate(FormulaSQL(source_table, '{col_name}', '={formula_escaped}').ibis_column())"
            )
            synthesis_statements.append(statement)

        return synthesis_statements

    def construct_code(self) -> str:
        synthesis_statements = self._parse_synthesis_transformations()
        template_data = {
            "synthesis_statements": synthesis_statements,
            "transformation_id": self.synthesis_parser.transform_id,
        }
        self._transformed_code: str = self.template_render(
            template_file_name=TemplateNames.SYNTHESIZE, template_content=template_data
        )
        return self._transformed_code

    def transform(self) -> str:
        return self.construct_code()
