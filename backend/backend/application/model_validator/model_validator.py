from collections import defaultdict
from typing import Any, Callable

from backend.application.config_parser.base_parser import BaseParser
from backend.application.config_parser.config_parser import ConfigParser
from backend.application.config_parser.transformation_parser import TransformationParser
from backend.application.model_validator.model_config_validator import ModelConfigValidator
from backend.application.model_validator.transformations.base_validator import Validator
from backend.application.model_validator.transformations.combine_column_validator import CombineColumnValidator
from backend.application.model_validator.transformations.distinct_validator import DistinctValidator
from backend.application.model_validator.transformations.filter_validator import FilterValidator
from backend.application.model_validator.transformations.find_and_replace_validator import FindAndReplaceValidator
from backend.application.model_validator.transformations.group_and_aggregation_validator import (
    GroupAndAggregationValidator)
from backend.application.model_validator.transformations.join_validator import JoinValidator
from backend.application.model_validator.transformations.merge_validator import MergeValidator
from backend.application.model_validator.transformations.pivot_validator import PivotValidator
from backend.application.model_validator.transformations.rename_validator import RenameValidator
from backend.application.model_validator.transformations.synthesis_validator import SynthesisValidator
from backend.application.session.session import Session
from backend.application.visitran_backend_context import VisitranBackendContext
from backend.errors import ModelNotExists, \
    ColumnDependency
from backend.errors.dependency_exceptions import ModelTableDependency


class ModelValidator:
    # Validators for transform types that add/remove columns.
    # Used by update, delete, and clear-all validation paths.
    _COLUMN_AFFECTING_VALIDATORS: dict[str, type[Validator]] = {
        "synthesize": SynthesisValidator,
        "join": JoinValidator,
        "combine_columns": CombineColumnValidator,
        "pivot": PivotValidator,
        "groups_and_aggregation": GroupAndAggregationValidator,
        "rename_column": RenameValidator
    }

    def __init__(
        self,
        updated_model_data: dict[str, Any],
        model_name: str,
        session: Session,
        visitran_context: VisitranBackendContext,
    ):
        """Initializes a new instance of the class.

        This constructor sets up the initial state of the object by
        initializing its attributes based on the provided parameters. It
        prepares parsers for configuration management, establishes a
        session for handling backend operations, and integrates the
        visitran context for the application runtime.

        :param updated_model_data: A dictionary containing the updated
            model data to be used for configuration.
        :type updated_model_data: dict[str, Any]
        :param model_name: Name of the model associated with the
            configurations.
        :type model_name: str
        :param session: A session object to manage interaction with the
            backend system.
        :type session: Session
        :param visitran_context: Backend context object for supporting
            visitran runtime configuration.
        :type visitran_context: VisitranBackendContext
        """
        self._updated_model = updated_model_data
        self._model_name = model_name
        self._session = session
        self._visitran_context = visitran_context
        self._current_config_parser = ConfigParser(
            model_data=updated_model_data,
            file_name=self._model_name,
        )
        self._old_config_parser: ConfigParser | None = None
        self._all_parsers: list[ConfigParser] = []
        self._table_columns = []
        self.old_table_details = {}
        self.dependency_columns = defaultdict(dict)
        self._fetch_old_model_if_exists()

    @property
    def current_config_parser(self) -> ConfigParser:
        return self._current_config_parser

    @property
    def old_config_parser(self) -> ConfigParser | None:
        return self._old_config_parser

    @property
    def table_columns(self) -> list[str]:
        if not self._table_columns:
            schema_name = self.current_config_parser.destination_schema_name
            table_name = self.current_config_parser.destination_table_name
            table_columns = self._visitran_context.get_table_columns_with_type(
                schema_name=schema_name, table_name=table_name
            )
            for column in table_columns:
                self._table_columns.append(column['column_name'])
        return self._table_columns

    def _fetch_old_model_if_exists(self) -> ConfigParser | None:
        try:
            # Trying to fetch the old config model if exist
            old_model = self._session.fetch_model(self._model_name)
            if old_model:
                self._old_config_parser = ConfigParser(
                    model_data=old_model.model_data,
                    file_name=f"{self._model_name}_old",
                )
        except ModelNotExists:
            # If the old model doesn't exist, surplussing the exception
            pass

    def _validate_source_config(self, **kwargs) -> None:
        """This method validates the newly configured model with source,
        destination and reference models :return:"""
        model_config_validator = ModelConfigValidator(
            session=self._session,
            visitran_context=self._visitran_context,
            current_parser=self.current_config_parser,
            old_parser=self.old_config_parser,
            model_name=self._model_name
        )
        response = model_config_validator.validate_source_config()
        if response:
            schema_name, table_name = response
            self.old_table_details = {
                "schema_name": schema_name,
                "table_name": table_name
            }
        model_config_validator.validate_destination_config()
        model_config_validator.validate_reference_config()

    def _validate_model_config(self, **kwargs) -> None:
        """This method validates the newly configured model with source,
        destination and reference models :return:"""
        model_config_validator = ModelConfigValidator(
            session=self._session,
            visitran_context=self._visitran_context,
            current_parser=self.current_config_parser,
            old_parser=self.old_config_parser
        )
        response = model_config_validator.validate_destination_config()
        if response:
            schema_name, table_name = response
            self.old_table_details = {
                "schema_name": schema_name,
                "table_name": table_name
            }

    def _validate_reference_config(self, **kwargs):
        model_config_validator = ModelConfigValidator(
            session=self._session,
            visitran_context=self._visitran_context,
            current_parser=self.current_config_parser,
            old_parser=self.old_config_parser
        )
        model_config_validator.validate_reference_config()

    def _validate_new_transformation(self, transformation_type: str, transformation_id: str):
        _transformation_mapper: dict[str, type[Validator]] = {
            "pivot": PivotValidator,
            "groups_and_aggregation": GroupAndAggregationValidator,
            "rename_column": RenameValidator
        }
        if transformation_type in _transformation_mapper:
            transformation_cls: type[Validator] = _transformation_mapper[transformation_type]
            transform_parser: BaseParser = self.current_config_parser.transform_parser.get_transformation_parser(
                transformation_id=transformation_id
            )
            transformation_validator = transformation_cls(
                session=self._session,
                visitran_context=self._visitran_context,
                model_name=self._model_name,
                current_parser=transform_parser
            )
            affected_columns = transformation_validator.validate_new_transform()
            if transformation_type in ["pivot", "groups_and_aggregation"]:
                affected_columns = [col for col in self.table_columns if col not in affected_columns]
            sort_columns = self.current_config_parser.presentation_parser.sort_columns
            dependent_columns: list[str] = [col for col in affected_columns if col in sort_columns]
            if dependent_columns:
                raise ColumnDependency(
                    model_name=self._model_name,
                    transformation_name=transformation_type,
                    affected_columns=dependent_columns,
                    affected_transformation="sort"
                )
            return affected_columns
        return None

    def _validate_updated_transformation(self, transformation_type: str, transformation_id: str):
        if transformation_type in self._COLUMN_AFFECTING_VALIDATORS:
            transformation_cls: type[Validator] = self._COLUMN_AFFECTING_VALIDATORS[transformation_type]
            transform_parser: BaseParser = self.current_config_parser.transform_parser.get_transformation_parser(
                transformation_id=transformation_id
            )
            old_transform_parser: BaseParser = self.old_config_parser.transform_parser.get_transformation_parser(
                transformation_id=transformation_id
            )
            transformation_validator = transformation_cls(
                session=self._session,
                visitran_context=self._visitran_context,
                model_name=self._model_name,
                current_parser=transform_parser,
                old_parser=old_transform_parser
            )
            affected_columns = transformation_validator.validate_updated_transform()
            if affected_columns:
                self._validate_column_usage(
                    config_parser=self.current_config_parser,
                    affected_columns=affected_columns,
                    transformation_id=transformation_id
                )
            return affected_columns
        return None

    def _validate_deleted_transformation(self, transformation_type: str, transformation_id: str):
        if transformation_type in self._COLUMN_AFFECTING_VALIDATORS:
            transformation_cls: type[Validator] = self._COLUMN_AFFECTING_VALIDATORS[transformation_type]
            old_transform_parser: BaseParser = self.old_config_parser.transform_parser.get_transformation_parser(
                transformation_id=transformation_id
            )
            transformation_validator = transformation_cls(
                session=self._session,
                visitran_context=self._visitran_context,
                model_name=self._model_name,
                old_parser=old_transform_parser
            )
            affected_columns = transformation_validator.validate_deleted_transform()
            if affected_columns:
                self._validate_column_usage(
                    config_parser=self.current_config_parser,
                    affected_columns=affected_columns,
                    transformation_id=transformation_id
                )
            return affected_columns
        return None

    def _validate_all_transformations(self, **kwargs) -> list[str] | None:
        """Validate all transformations being cleared at once.

        Iterates every transform in the old model, collects the columns
        each one would remove, and returns the combined list so that the
        caller (validate_model) can check child-model dependencies.
        """
        if not self.old_config_parser:
            return None

        all_affected: list[str] = []
        old_transform_parser = self.old_config_parser.transform_parser

        for transformation_id in old_transform_parser.transform_orders:
            parser = old_transform_parser.get_transformation_parser(
                transformation_id=transformation_id
            )
            if parser is None:
                continue

            transformation_cls = self._COLUMN_AFFECTING_VALIDATORS.get(parser.transform_type)
            if transformation_cls is None:
                # Transform type doesn't add/remove columns (filter, distinct, etc.)
                continue

            validator = transformation_cls(
                session=self._session,
                visitran_context=self._visitran_context,
                model_name=self._model_name,
                old_parser=parser,
            )
            affected = validator.validate_deleted_transform()
            if affected:
                all_affected.extend(affected)

        # Deduplicate while preserving order
        unique_affected = list(dict.fromkeys(all_affected))
        return unique_affected or None

    @staticmethod
    def _load_validator(transform_type: str) -> type(Validator):
        _transformation_mapper = {
            "join": JoinValidator,
            "union": MergeValidator,
            "pivot": PivotValidator,
            "combine_columns": CombineColumnValidator,
            "synthesize": SynthesisValidator,
            "filter": FilterValidator,
            "groups_and_aggregation": GroupAndAggregationValidator,
            "distinct": DistinctValidator,
            "rename_column": RenameValidator,
            "find_and_replace": FindAndReplaceValidator
        }
        return _transformation_mapper[transform_type]

    def _validate_column_usage(
            self,
            config_parser: ConfigParser,
            affected_columns: list[str],
            transformation_id: str
    ) -> None:
        transform_parser: TransformationParser = config_parser.transform_parser
        transform_index = -1
        if transformation_id in transform_parser.transform_orders:
            transform_index = transform_parser.transform_orders.index(transformation_id)
        for parser in transform_parser.get_transforms()[transform_index + 1:]:
            dependent_columns = self._validate_column_in_transformation(
                parser=parser,
                affected_columns=affected_columns
            )
            if dependent_columns:
                self.dependency_columns[config_parser.model_name][parser.transform_type] = dependent_columns
        sort_columns = config_parser.presentation_parser.sort_columns
        dependent_columns: list[str] = [col for col in affected_columns if col in sort_columns]
        if dependent_columns:
            self.dependency_columns[config_parser.model_name]["sort"] = dependent_columns

    def _validate_column_in_transformation(self, parser: BaseParser, affected_columns: list[str]):
        try:
            validator_cls = self._load_validator(parser.transform_type)
        except KeyError:
            # Transform type has no validator (e.g. window) — skip gracefully
            return []
        validator = validator_cls(
            session=self._session,
            visitran_context=self._visitran_context,
            model_name=self._model_name,
            current_parser=parser
        )
        return validator.check_column_usage(columns=affected_columns)

    def validate(
            self,
            config_type: str = "all",
            transformation_type: str = None,
            transformation_id: str = None
    ) -> list[str] | None:
        _validators: dict[str, Callable[..., None]] = {
            "source": self._validate_source_config,
            "model": self._validate_model_config,
            "reference": self._validate_reference_config,
            "create_transformation": self._validate_new_transformation,
            "update_transformation": self._validate_updated_transformation,
            "delete_transformation": self._validate_deleted_transformation,
            "clear_all": self._validate_all_transformations
        }
        validator_function: Callable[..., None] = _validators.get(config_type, self._validate_all_transformations)
        kwargs = {
            "transformation_type": transformation_type,
            "transformation_id": transformation_id
        }
        affected_columns: list[str] | None = validator_function(**kwargs)
        return affected_columns

    def validate_child_models(self, child_model_names: list[str], affected_columns: list[str]) -> None:
        for model_name in child_model_names:
            model_data: dict[str, Any] = self._session.fetch_model_data(model_name)
            config_parser = ConfigParser(model_data=model_data, file_name=model_name)
            self._validate_column_usage(
                config_parser=config_parser,
                affected_columns=affected_columns,
                transformation_id="1"
            )

    def validate_child_table_usage(self, child_model_names: list[str], affected_table: dict[str, str]) -> None:
        schema_name = affected_table.get("schema_name")
        table_name = affected_table.get("table_name")
        affected_child_models = []
        for model_name in child_model_names:
            model_data: dict[str, Any] = self._session.fetch_model_data(model_name)
            config_parser = ConfigParser(model_data=model_data, file_name=model_name)
            if config_parser.source_table_name == table_name and config_parser.source_schema_name == schema_name:
                affected_child_models.append(model_name)
        if affected_child_models:
            raise ModelTableDependency(
                child_models=affected_child_models,
                table_name=table_name,
                model_name=self._model_name,
            )

    def refactor_model_name_in_child_model(self, child_model_names: list[str], rename_name: str) -> dict[str, Any]:
        refactored_model_names = {}
        for model_name in child_model_names:
            model_data: dict[str, Any] = self._session.fetch_model_data(model_name)
            config_parser = ConfigParser(model_data=model_data, file_name=model_name)
            references = config_parser.reference
            if self._model_name not in references:
                continue
            references.remove(self._model_name)
            references.append(rename_name)
            model_data["reference"] = references
            self._session.update_model(model_data=model_data, model_name=model_name)
            refactored_model_names[model_name] = model_data
        return refactored_model_names
