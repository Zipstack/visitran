from typing import Type

from backend.application.config_parser.config_parser import ConfigParser
from backend.application.file_explorer.file_explorer import FileExplorer
from backend.application.interpreter.base_interpreter import BaseInterpreter
from backend.application.interpreter.constants import TemplateNames
from backend.application.interpreter.transformations.base_transformation import BaseTransformation
from backend.application.interpreter.transformations.combine_column import CombineColumnTransformation
from backend.application.interpreter.transformations.distinct import DistinctTransformation
from backend.application.interpreter.transformations.filter import FiltersTransformation
from backend.application.interpreter.transformations.find_and_replace import FindAndReplaceTransformation
from backend.application.interpreter.transformations.groups_and_aggregation import GroupsAndAggregationTransformation
from backend.application.interpreter.transformations.joins import JoinTransformation
from backend.application.interpreter.transformations.pivot import PivotTransformation
from backend.application.interpreter.transformations.reference import ReferenceTransformation
from backend.application.interpreter.transformations.rename_columns import RenameColumnTransformation
from backend.application.interpreter.transformations.column_reorder import ColumnReorderTransformation
from backend.application.interpreter.transformations.sorts import SortsTransformation
from backend.application.interpreter.transformations.source_model_transformation import SourceModelTransformation
from backend.application.interpreter.transformations.synthesize import SynthesizeTransformation
from backend.application.interpreter.transformations.union_transformation import UnionTransformation
from backend.application.interpreter.transformations.window import WindowTransformation
from backend.application.utils import get_class_name
from backend.application.visitran_backend_context import VisitranBackendContext


class Interpreter(BaseInterpreter):
    def __init__(
        self,
        config_parser: ConfigParser,
        file_explorer: FileExplorer,
        visitran_context: VisitranBackendContext,
    ) -> None:
        super().__init__(config_parser, file_explorer, visitran_context)
        self._headers: list[str] = []
        self._content: list[str] = []
        self._parent_classes: list[str] = []
        self._transformation_statements: list[str] = []
        self._python_file_content: str = ""

    def add_headers(self, header: str) -> None:
        self._headers.append(header)

    def add_content(self, content: str) -> None:
        self._content.append(content)

    def add_parent_classes(self, parent_class: str) -> None:
        self._parent_classes.append(parent_class)

    @property
    def headers(self) -> list[str]:
        return self._headers

    @property
    def contents(self) -> list[str]:
        return self._content

    @property
    def parent_classes(self) -> list[str]:
        return self._parent_classes

    @property
    def python_file_content(self) -> str:
        return self._python_file_content

    @staticmethod
    def _transformation_mapper(transformation_type: str) -> Type[BaseTransformation]:
        """This method will map the transformation type to the transformation class and return it."""
        _transformation_mapper = {
            "join": JoinTransformation,
            "union": UnionTransformation,
            "pivot": PivotTransformation,
            "combine_columns": CombineColumnTransformation,
            "synthesize": SynthesizeTransformation,
            "filter": FiltersTransformation,
            "groups_and_aggregation": GroupsAndAggregationTransformation,
            "find_and_replace": FindAndReplaceTransformation,
            "distinct": DistinctTransformation,
            "rename_column": RenameColumnTransformation,
            "window": WindowTransformation,
        }
        return _transformation_mapper[transformation_type]

    def _parse_file_headers(self) -> None:
        """This method will add all the import modules needed for the python
        file."""

        self.add_headers("import re")
        self.add_headers("import ibis")
        self.add_headers("from ibis import _")
        self.add_headers("from ibis.expr.types.relations import Table")
        self.add_headers("from ibis.common.exceptions import IbisTypeError")
        self.add_headers("from visitran.templates.model import VisitranModel")
        self.add_headers("from visitran.errors import ColumnNotExist")
        self.add_headers("from visitran.errors import TransformationFailed")
        self.add_headers("from visitran.materialization import Materialization")

    def _parse_model_config(self) -> None:
        class_name = self.source_class_name
        syntax_operator = self.get_class_var_str(class_name=class_name)
        parent_declaration = syntax_operator + f": Table = {class_name}().select()"
        self._transformation_statements.append(parent_declaration)

        # Parsing for model reference
        reference_transformation = ReferenceTransformation(
            config_parser=self.parser, context=self.context
        )
        reference_transformation.transform()
        self._headers.extend(reference_transformation.headers)
        source_model_transformation = SourceModelTransformation(
            config_parser=self.parser, context=self.context
        )
        transformed_code = source_model_transformation.transform(reference_transformation.parent_class)
        self.add_parent_classes(self.source_class_name)
        self.add_content(transformed_code)
        self._transformation_statements.append(f"source_table = {syntax_operator}")

    def _parse_transformations(self) -> None:
        params = {"config_parser": self.parser, "context": self.context}
        for transformation_parser in self.parser.transform_parser.get_transforms():
            transformation_class: Type[BaseTransformation] = self._transformation_mapper(
                transformation_parser.transform_type
            )
            params["parser"] = transformation_parser
            transformation_instance: BaseTransformation = transformation_class(**params)
            if hasattr(transformation_instance, "join_headers"):
                transformation_instance.join_headers = self.headers
            self._transformation_statements.append(transformation_instance.transform())
            self._content.extend(transformation_instance.content)
            self._headers.extend(transformation_instance.headers)
            if transformation_instance.parent_classes:
                self._parent_classes.extend(transformation_instance.parent_classes)

    def _parse_presentations(self):
        sort = SortsTransformation(
            config_parser=self.parser, context=self.context
        )
        self._transformation_statements.append(sort.transform())

        column_reorder = ColumnReorderTransformation(
            config_parser=self.parser, context=self.context
        )
        self._transformation_statements.append(column_reorder.transform())

    def _resolve_parent_classes(self) -> str:
        """
        Resolve parent classes to avoid MRO (Method Resolution Order) conflicts.

        When multiple parent classes are collected (from JOINs, UNIONs, etc.),
        they may have conflicting inheritance chains that Python cannot linearize.

        Strategy:
        - If only one parent class exists, use it directly
        - If multiple parent classes exist, use only the first one (the source model)
          to avoid diamond inheritance MRO conflicts

        This ensures proper dependency tracking while avoiding TypeError:
        "Cannot create a consistent method resolution order (MRO)"
        """
        unique_parents = list(dict.fromkeys(self.parent_classes))  # Preserve order, remove duplicates

        if len(unique_parents) <= 1:
            # Single parent or empty - no MRO conflict possible
            return ", ".join(unique_parents) if unique_parents else "VisitranModel"

        # Multiple parents - use only the first (source model) to avoid MRO conflicts
        # The first parent is always the source model added in _parse_model_config
        return unique_parents[0]

    def _parse_destination_class(self):
        template_data = {
            "previous_class": self._resolve_parent_classes(),
            "class_name": get_class_name(self.parser.model_name),
            "destination_schema_name": self.parser.destination_schema_name,
            "destination_table_name": self.parser.destination_table_name,
            "database_name": self.default_database,
            "transformation_statements": self._transformation_statements,
            "materialization_type": self.parser.materialization,
            "unique_keys": self.parser.unique_keys,
            "delta_strategy": self.parser.delta_strategy
        }
        content = self.template_render(
            template_file_name=TemplateNames.DESTINATION_TABLE, template_content=template_data
        )
        self.add_content(content)

    def generate_python_file(self) -> str:
        self._python_file_content: str = self.template_render(
            template_file_name=TemplateNames.FILE_GENERATOR,
            template_content={
                "imports": self.headers,
                "class_contents": self.contents,
                "model_name": self.parser.model_name,
            },
        )
        return self._python_file_content

    def parse_to_py(self) -> str:
        self._parse_file_headers()
        self._parse_model_config()
        self._parse_transformations()
        self._parse_presentations()
        self._parse_destination_class()
        self.generate_python_file()
        return self._python_file_content
