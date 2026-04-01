
from pathlib import Path
from re import sub
from typing import Any

from jinja2 import Environment, FileSystemLoader

from backend.application.config_parser.config_parser import ConfigParser
from backend.application.file_explorer.file_explorer import FileExplorer
from backend.application.utils import get_class_name
from backend.application.visitran_backend_context import VisitranBackendContext

TEMPLATES_PATH = f"{Path(__file__).parent.parent.parent}/application/interpreter/python_templates/"


class BaseInterpreter:
    def __init__(
        self,
        config_parser: ConfigParser,
        file_explorer: FileExplorer,
        visitran_context: VisitranBackendContext,
    ) -> None:
        self._config_parser: ConfigParser = config_parser
        self._file_explorer = file_explorer
        self._visitran_context = visitran_context
        self._files: list[dict[str, str]] = []
        self.source_file_name = f"{self.parser.destination_schema_name}_{self.parser.destination_table_name}"
        self.source_class_name = self.build_source_class_name()

    def build_source_class_name(self) -> str:
        source_class_name = get_class_name(self.source_file_name)
        if source_class_name == get_class_name(self.parser.model_name):
            return f"{source_class_name}Source"
        return source_class_name

    @staticmethod
    def get_class_var_str(class_name: str) -> str:
        return "_".join(
            sub(
                "([A-Z][a-z]+)",
                r" \1",
                sub("([A-Z]+)", r" \1", class_name.replace("-", " ")),
            ).split()
        ).lower()

    @property
    def parser(self) -> ConfigParser:
        """Returns config parser object."""
        return self._config_parser

    @property
    def explorer(self) -> FileExplorer:
        """Returns file explorer object."""
        return self._file_explorer

    @property
    def context(self) -> VisitranBackendContext:
        return self._visitran_context

    @property
    def files(self) -> list[dict[str, str]]:
        return self._files

    @property
    def default_database(self) -> str:
        return self.context.db_adapter.db_connection.database_name

    @property
    def full_code_base_path(self):
        return f"{self.context.project_name}.models"

    def add_files(self, file_name: str, file_content: str) -> None:
        self._files.append({f"{file_name}.py": file_content})

    @staticmethod
    def template_render(template_file_name: str, template_content: dict[str, Any]):
        """This method creates a file using jinja template render."""
        environment = Environment(loader=FileSystemLoader(TEMPLATES_PATH))
        template = environment.get_template(f"{template_file_name}.jinja")
        content = template.render(template_content)
        return content
