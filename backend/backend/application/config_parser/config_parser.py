from typing import Any

from backend.application.config_parser.base_parser import BaseParser
from backend.application.config_parser.presentation_parser import PresentationParser
from backend.application.config_parser.transformation_parser import TransformationParser
from backend.errors import InvalidSourceTable, InvalidDestinationTable, InvalidMaterialization


class ConfigParser(BaseParser):
    _instances: dict[str, "ConfigParser"] = {}

    def __new__(cls, model_data: dict[str, Any], file_name: str, *args, **kwargs) -> "ConfigParser":
        """Overrides the __new__ method to implement a singleton pattern based
        on the file_name parameter.

        Args:
            model_data (dict[str, Any]): Configuration data for the model.
            file_name (str): The name of the configuration file.

        Returns:
            ConfigParser: A singleton instance of the class for the given file name.
        """
        if file_name in cls._instances:
            return cls._instances[file_name]
        instance = super().__new__(cls)
        cls._instances[file_name] = instance
        return instance

    def __init__(self, model_data: dict[str, Any], file_name: str) -> None:
        self._model_name: str = file_name
        self._no_code_conf: dict[str, Any] = model_data
        self._presentation_parser: Any = None
        self._transformation_parser: Any = None
        self._reference: list[str] = []
        self._all_reference: dict[str, Any] = {}
        super().__init__(self._no_code_conf)

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def source_schema_name(self) -> str:
        if self.get("source", {}).get("schema_name", "") == "~":
            # Special case where "~" indicates no schema.
            return ""
        return self.get("source", {}).get("schema_name", "default")  # Return the schema name or "default".

    @property
    def source_table_name(self) -> str:
        source_table_name = self.get("source", {}).get("table_name", "")
        if not source_table_name:
            raise InvalidSourceTable(table_name=source_table_name)
        return source_table_name

    @property
    def destination_schema_name(self) -> str:
        if self.get("model", {}).get("schema_name", "") == "~":
            return ""
        return self.get("model", {}).get("schema_name", "default")

    @property
    def destination_table_name(self) -> str:
        destination_table_name = self.get("model", {}).get("table_name", "")
        if not destination_table_name:
            raise InvalidDestinationTable(table_name=destination_table_name)
        return destination_table_name

    @property
    def materialization(self) -> str:
        materialization = self.get("source", {}).get("materialization", "TABLE")
        if materialization not in ["TABLE", "VIEW", "EPHEMERAL", "INCREMENTAL"]:
            # Raise an exception if the materialization type is unsupported.
            raise InvalidMaterialization(
                materialization=materialization,
                supported_materializations=["TABLE", "VIEW", "EPHEMERAL", "INCREMENTAL"],
            )
        return self.get("source", {}).get("materialization", "TABLE")

    @property
    def incremental_config(self) -> dict[str, Any]:
        if self.materialization == "TABLE":
            return {}
        return self.get("source", {}).get("incremental_config", {})

    @property
    def unique_keys(self) -> list[str]:
        return self.incremental_config.get('primary_key', [])

    @property
    def delta_strategy(self) -> dict[str, Any]:
        return self.incremental_config.get("delta_strategy", {})

    @property
    def reference(self) -> list[str]:
        if not self._reference:
            self._reference = self.get("reference") or []
        return self._reference

    @property
    def source_model(self) -> str | None:
        """Returns the model name that produces this model's source table, if
        any.

        This is set by validate_table_usage_references() when the source table
        matches another model's destination. It explicitly tracks which model
        should be the parent class (for inheritance and data flow).

        Returns:
            str: Model name if source table is produced by another model
            None: If source table is a raw database table
        """
        return self.get("source_model")

    @property
    def all_reference(self) -> dict[str, Any]:
        return self._all_reference

    @all_reference.setter
    def all_reference(self, value: dict[str, Any]) -> None:
        if not isinstance(value, dict):
            raise TypeError("all_reference must be a dictionary, Passed a list instead.")
        self._all_reference = value

    @property
    def presentation_parser(self) -> PresentationParser:
        if not self._presentation_parser:
            self._presentation_parser: PresentationParser = PresentationParser(config_data=self.get("presentation", {}))
        return self._presentation_parser

    @property
    def transform_parser(self) -> TransformationParser:
        if not self._transformation_parser:
            self._transformation_parser: TransformationParser = TransformationParser(
                config_data={
                    "transform": self.get("transform", []),
                    "transform_order": self.get("transform_order", [])
                }
            )
        return self._transformation_parser
