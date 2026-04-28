import logging
from typing import Any, Union, AnyStr, Dict

import yaml
from sqlparse import parse
from visitran.errors import VisitranPostgresMissingError, VisitranBaseExceptions
from visitran.visitran import Visitran

from backend.application.config_parser.config_parser import ConfigParser
from backend.application.config_parser.dag_builder import DAGBuilder
from backend.application.config_parser.dag_executor import DAGExecutor
from backend.application.config_parser.feature_flags import ExecutionRouter, FeatureFlags
from backend.application.session.env_session import EnvironmentSession
from backend.application.context.model_graph import ModelGraph
from backend.application.interpreter.interpreter import Interpreter
from backend.application.model_validator import ModelValidator
from backend.application.utils import set_transformation_sequence
from backend.application.validate_references import ValidateReferences
from backend.core.models.config_models import ConfigModels
from backend.core.models.csv_models import CSVModels
from backend.errors import ModelDependency, ModelNotExists, InvalidSQLQuery, ProhibitedSqlQuery, CsvDownloadFailed, SchemaNotFoundError, \
    MultipleColumnDependency
from backend.errors.visitran_backend_base_exceptions import VisitranBackendBaseException
from backend.utils.cache_service.cache_loader import CacheService
from backend.utils.utils import convert_db_type_to_no_code_type

logger = logging.getLogger(__name__)


class ApplicationContext(ModelGraph):
    """This application context will connect with visitran and code generator
    and file explorer."""

    def __init__(self, project_id: str, environment_id: str = "") -> None:
        super().__init__(project_id, environment_id)
        self._parser = None

    def get_all_schemas(self) -> list[Any]:
        """This method will return all the list of schemas in the project."""
        schema_list: list[Any] = self.visitran_context.list_all_schemas()
        return schema_list

    def create_schema(self):
        """This method will create schema for current connection"""
        self.visitran_context.create_schema()

    def get_all_tables(self, schema_name: str) -> list[Any]:
        """This method will return all the list of tables in the project."""
        table_list: list[Any] = self.visitran_context.list_all_tables(schema_name=schema_name)
        return table_list

    def get_destination_table_name(self, model_name: str) -> str:
        no_code_model: dict = self.session.fetch_model_data(model_name=model_name)
        destination_table_name: str = no_code_model.get("model", {}).get("table_name", "")
        return destination_table_name

    def get_project_config_details(self, model_name: str) -> dict[str, Any]:
        """This method is used to get the project config details.

        It will return the config details for the given model name and
        return the reference models for the given model name and return
        the output list of models that are referenced by the given model
        name.
        """
        config_models: list[ConfigModels] = self.session.fetch_all_models(fetch_all=True)
        model_list = [model.model_name for model in config_models]
        model_references = {model.model_name: model.model_data.get("reference", []) for model in config_models}
        output_list = []
        visited = set()

        # gather references back&forth
        def collect_references(model):
            if model in visited:
                return
            visited.add(model)

            # Get the references for the current model
            references = model_references.get(model, [])
            output_list.extend([ref for ref in references if ref not in output_list])

            # Recursively process each reference(in reverse)
            for ref in references:
                collect_references(ref)

        # Collect all references recursively
        collect_references(model_name)

        # Check if any model references the gathered models (upfront)
        for key, values in model_references.items():
            if any(ref in output_list for ref in values) and key not in output_list:
                output_list.append(key)

        filtered_list = [item for item in model_list if item not in output_list]

        config_data = {
            model.model_name: {
                "source": {
                    "schema_name": model.model_data.get("source", {}).get("schema_name"),
                    "table_name": model.model_data.get("source", {}).get("table_name"),
                },
                "destination": {
                    "schema_name": model.model_data.get("destination", {}).get("schema_name"),
                    "table_name": model.model_data.get("destination", {}).get("table_name"),
                },
            }
            for model in config_models
            if model.model_name in filtered_list
        }
        return config_data

    @staticmethod
    def get_config_parser(model_data: dict[str, Any], file_name: str) -> ConfigParser:
        return ConfigParser(model_data=model_data, file_name=file_name)

    def get_table_columns(self, schema_name: str, table_name: str, prefix="") -> list[dict[str, Any]]:
        """This method will return the columns with the table."""
        column_names: list[dict[str, Any]] = self.visitran_context.get_table_columns_with_type(
            schema_name=schema_name, table_name=table_name
        )
        updated_column = []
        for column_data in column_names:
            ui_dbtype = convert_db_type_to_no_code_type(
                db_type=column_data["column_dbtype"]
            )
            column_data["data_type"] = ui_dbtype
            if prefix:
                column_data["column_name"] = f'{prefix}_{column_data["column_name"]}'
            updated_column.append(column_data)
        return column_names

    def get_connection_details(self) -> dict[str, Union[str, int]]:
        connection_details: dict[str, Any] = self.connection.masked_connection_details
        return connection_details

    def is_table_exists_in_db(self) -> bool:
        table_list = self.visitran_context.list_all_tables()
        return bool(table_list)

    def update_connection_details(self, connection_details: dict[str, Any]) -> dict[str, Any]:
        connection_details = self.session.update_project_connection(connection_details)
        self._reload_context()
        return connection_details

    def test_connection_details(self) -> bool:
        return self.visitran_context.test_connection()

    def test_connection_details_with_data(self, connection_details: dict[str, Any], datasource: str) -> bool:
        return self.visitran_context.test_connection_data(connection_data=connection_details, db_type=datasource)

    def load_testing_models(self):
        from backend.utils.load_models.load_models import load_models
        models = load_models()
        try:
            for model in models:
                try:
                    self.session.create_model(model.get("model_name"), is_generate_ai_request=False)
                except Exception as e:
                    pass
                model_name = model.get("model_name")
                self.save_model_file(no_code_data={"file": model}, model_name=model_name, is_chat_response=True)
            self.execute_run()
        except Exception as e:
            print(e)
            raise e

    def get_project_explorer(self):
        # self.load_testing_models()
        self.sync_seed_with_table()
        file_structure: dict[str, Any] = self.file_explorer.get_project_file_structure(self.session)
        return file_structure

    def sync_seed_with_table(self):
        _csv_models: list[CSVModels] = self.session.fetch_all_csv_files()
        if _csv_models:
            _schema_name = _csv_models[0].table_schema
            tables = set(self.get_all_tables(_schema_name))
            for csv_model in _csv_models:
                status = csv_model.table_name in tables
                if csv_model.table_exists != status:
                    csv_model.table_exists = status
                    csv_model.save()

    def create_a_model(self, model_name: str, is_generate_ai_request: bool=False) -> str:
        """This method will create a new no_code_model with help of file
        explorer."""
        new_model_name = self.session.create_model(model_name=model_name, is_generate_ai_request=is_generate_ai_request)
        self.add_node_to_model_graph(new_model_name)  # add node to the graph and persist them in db
        return new_model_name

    def add_node_to_model_graph(self, model_name: str) -> None:
        model = self.session.fetch_model(model_name)
        self.session.model_graph.add_node(model.model_name, name=str(model.model_id))
        self.session.project_instance.project_model_graph = self.session.model_graph.serialize()
        self.session.project_instance.save()

    def delete_node_from_model_graph(self, model_name: str) -> None:
        try:
            self.session.model_graph.remove_node(model_name)
            self.session.project_instance.project_model_graph = self.session.model_graph.serialize()
            self.session.project_instance.save()
        except ValueError:
            pass

    def delete_a_file_or_folder(self, file_path: str, table_delete_enabled=False, deleting_models: set[str] | None = None):
        """This method is used to delete a file or folder either in no_code."""

        if file_path.startswith("models"):
            file_name = file_path.split("models/no_code/")[-1]
            file_name = file_name.replace(" ", "_").strip()
            # Validating the model for child model usages before deleting
            model_dict = self.get_model_references()
            if file_name in model_dict:
                reference_validator = ValidateReferences(model_dict=model_dict, model_name=file_name)
                child_models = reference_validator.get_child_references()
                # Exclude models that are also being deleted in this batch
                if deleting_models:
                    child_models = child_models - deleting_models
                if child_models:
                    raise ModelDependency(child_models=list(child_models), model_name=file_name)
            if table_delete_enabled:
                self.delete_destination_table(model_name=file_name)
            try:
                self.session.delete_model(model_name=file_name)
            except ModelNotExists:
                logger.debug("Model '%s' not found in DB during delete; skipping.", file_name)
            self.delete_node_from_model_graph(file_name)
        elif file_path.startswith("seeds/"):
            file_name = file_path.split("seeds/")[-1]
            self.session.delete_csv_model(file_name=file_name)

    def delete_destination_table(self, model_name: str, force=False) -> None:
        try:
            all_models: list[ConfigModels] = self.session.fetch_all_models(fetch_all=True)
            current_model: ConfigModels = self.session.fetch_model(model_name=model_name)
            destination_schema = current_model.model_data.get("model", {}).get("schema_name")
            destination_table = current_model.model_data.get("model", {}).get("table_name")
            if not force:
                for model in all_models:
                    if model.model_name != model_name:
                        source_schema = model.model_data.get("source", {}).get("schema_name")
                        source_table = model.model_data.get("source", {}).get("table_name")
                        if f"{destination_schema}.{destination_table}" == f"{source_schema}.{source_table}":
                            logging.info(
                                f"Skipping table delete({destination_table}) on deleting model {model_name} due to referenced in {model.model_name} model"
                            )
                            return None
                logging.info(
                    f"No Usage of found for table: {destination_table} under schema: {destination_schema}, hence deleting..."
                )
            self.visitran_context.drop_table_if_exist(
                schema_name=destination_schema, table_name=destination_table
            )

        except Exception as e:
            logging.error(f"Exception while deleting destination table for model {model_name}, {e}")
            return None
        finally:
            self.visitran_context.clear_database_cache()

    def rename_a_file_or_folder(self, file_path: str, rename: str) -> list[str]:
        """This method is used to rename a file or folder."""
        refactored_models = []
        rename = rename.split("/")[-1]
        rename = rename.replace(" ", "_").strip()
        if file_path.startswith("seeds"):
            file_name = file_path.split("seeds/")[-1]
            self.session.rename_a_csv_file(csv_name=file_name, rename=rename)
        elif file_path.startswith("models"):
            file_name = file_path.split("models/no_code/")[-1]
            """This method is used to validate the model."""
            # Check the validation for child models
            model_data: dict[str, Any] = self.session.fetch_model_data(model_name=file_name)
            updated_child_models: dict[str, Any] = {}
            model_dict = self.get_model_references()
            if file_name in model_dict:
                model_validator = self.get_model_validator(model_data=model_data, model_name=file_name)
                reference_validator = ValidateReferences(model_dict=model_dict, model_name=file_name)
                child_models = reference_validator.get_child_references()
                updated_child_models: dict[str, Any] = model_validator.refactor_model_name_in_child_model(
                    child_model_names=list(child_models), rename_name=rename
                )
            # Rename all the python models and its reference using interpreter.
            self.session.rename_a_model(model_name=file_name, rename=rename)
            if model_data:
                self.convert_to_python(model_data, rename)
            if updated_child_models:
                for model_name, model_data in updated_child_models.items():
                    self.convert_to_python(model_data, model_name)
            if visitran_models := self.session.redis_client.get(self.redis_model_key):
                models = yaml.safe_load(visitran_models)
                if isinstance(models, dict):
                    self.session.redis_client.delete(self.redis_model_key)
                else:
                    for model_data in models:
                        if model_data["model_name"] == file_name:
                            model_data["model_name"] = rename
                            break
                    yaml_models = yaml.dump(models, default_flow_style=False, sort_keys=False)
                    self.session.redis_client.set(self.redis_model_key, yaml_models)
            refactored_models = list(updated_child_models.keys())
        return refactored_models

    def get_file_content(self, file_path: str):
        if file_path.startswith("seeds"):
            file_name = file_path.split("seeds/")[-1]
            return self.session.fetch_csv_file_fields(csv_name=file_name)
        elif file_path.startswith("models"):
            file_name = file_path.split("models/")[-1]
            return self.session.fetch_model_data(model_name=file_name)
        return None

    def get_file_abs_path(self, file_path: str) -> str:
        csv_file_name = file_path.split("/")[-1]
        return self.session.fetch_csv_file_fields(csv_file_name)

    def upload_a_file(self, file_name: str, file_content) -> str:
        """This method is used to upload a given file in seed path in project
        path."""
        file_name = file_name.split("/")[-1]
        file_name = file_name.split("-1")[0]
        return self.session.upload_csv_file(file_name=file_name, file_content=file_content)

    def reload_model(self, file_name: str) -> dict[str, Any]:
        """This method is to fetch the previous yaml configuration, which is
        needed to reload the project."""
        model_data = self.session.fetch_model_data(model_name=file_name)
        return model_data

    def write_database_file(self, database_file) -> None:
        """This method is used to upload a database file in database folder."""
        _file_path = self.file_explorer.write_database_file(database_file=database_file)
        self.visitran_context.import_database(db_path=self.file_explorer.get_db_path(), import_path=_file_path)

    def convert_to_python(self, model_data: dict[str, Any], model_name: str) -> ConfigParser:
        """This method is used to convert the no code model to python file."""
        models = self.get_all_model_details()
        models[model_name] = {
            "reference": model_data.get("reference", []),
            "source_schema": model_data.get("source", {}).get("schema_name"),
            "source_table": model_data.get("source", {}).get("table_name"),
            "destination_schema": model_data.get("model", {}).get("schema_name"),
            "destination_table": model_data.get("model", {}).get("table_name"),
        }
        model_dict = self.get_model_references()
        model_dict[model_name] = set(model_data.get("reference", []))
        current_model_reference = self.get_model_reference_details(model_name=model_name, models=models, model_dict=model_dict)
        model_name = model_name.replace(" ", "_").strip()
        parser, executor = self.compile_yaml_data(
            model_data=model_data,
            file_name=model_name,
            current_model_reference=current_model_reference,
        )
        file_content = executor.python_file_content
        self.session.update_model_content(model_name=model_name, model_content=file_content)
        return parser

    def update_sample_project_model_graph(self, no_code_data: dict[str, Any], model_name: str):
        new_model_reference = no_code_data.get("reference", [])
        for reference in new_model_reference:
            self.session.model_graph.add_edge(reference, model_name)

        self.session.project_instance.project_model_graph = self.session.model_graph.serialize()
        self.session.project_instance.save()

    def get_table_schema(self):
        try:
            table_names = self.get_all_tables("default")
            table_schema = []
            for (table,) in table_names:
                column_details = self.visitran_context.get_table_columns_with_type("default", table)
                table_schema.append({"table_name": table, "column_schema": column_details})
            return table_schema
        except Exception as exc:
            logging.error("Exception while fetching table, column %s", exc)

    def get_model_reference_details(
            self,
            model_name: str,
            models: dict[str, Any] = None,
            model_dict: dict[str,Any] = None
    ) -> dict[str, Any]:
        """
        This will return all reference models for the current model
        :param model_name:
        :param models: Optional
        :param model_dict: Optional
        :return:
        """
        models = models or self.get_all_model_details()
        referenced_models: dict[str, Any] = {}
        model_dict = model_dict or self.get_model_references()
        ref_validator = ValidateReferences(model_dict=model_dict, model_name=model_name)
        child_models = ref_validator.get_parent_references()
        for reference in child_models:
            referenced_models[reference] = {
                "source_schema": models.get(reference, {}).get("source_schema", ""),
                "source_table": models.get(reference, {}).get("source_table", ""),
                "destination_schema": models.get(reference, {}).get("destination_schema", ""),
                "destination_table": models.get(reference, {}).get("destination_table", ""),
            }
        return referenced_models

    def get_all_model_details(self):
        """
        This will return all reference models
        """
        config_models: list[ConfigModels] = self.session.fetch_all_models(fetch_all=True)
        models = {}
        for config_model in config_models:
            models[config_model.model_name] = {
                "reference": config_model.model_data.get("reference", []),
                "source_schema": config_model.model_data.get("source", {}).get("schema_name"),
                "source_table": config_model.model_data.get("source", {}).get("table_name"),
                "destination_schema": config_model.model_data.get("model", {}).get("schema_name"),
                "destination_table": config_model.model_data.get("model", {}).get("table_name"),
            }
        return models

    def get_model_references(self) -> dict[str, set[str]]:
        """This method is used to fetch the reference model data."""
        models = self.session.fetch_all_models(fetch_all=True)
        model_dict = {}
        for model in models:
            model_dict[model.model_name] = set(model.model_data.get("reference", []))
        return model_dict

    def _get_source_dependent_models(
        self, model_name: str, dest_schema: str, dest_table: str
    ) -> set[str]:
        """Find models whose source table matches the given destination table.

        This catches table-based dependencies that may not be captured in the
        reference graph — e.g. when Model B sources from Model A's destination
        table but Model A was never added to Model B's reference list.
        """
        if not dest_table:
            return set()
        # Normalise schema: treat None, empty string, and "~" as equivalent
        normalised_dest_schema = (dest_schema or "").replace("~", "")
        all_details = self.get_all_model_details()
        children = set()
        for name, details in all_details.items():
            if name == model_name:
                continue
            source_schema = (details.get("source_schema") or "").replace("~", "")
            if (
                source_schema == normalised_dest_schema
                and details.get("source_table") == dest_table
            ):
                children.add(name)
        return children

    def _update_model(self, model_name: str, model_data: dict[str, Any]) -> None:
        """This method is used to update the model in a database and redis :param
        model_name:

        :param model_name:
        :param model_data:
        :return:
        """
        self.session.update_model(model_name=model_name, model_data=model_data)
        # Updating the model spec in cache
        if visitran_models := self.session.redis_client.get(self.redis_model_key):
            models = yaml.safe_load(visitran_models)
            if isinstance(models, dict):
                self.session.redis_client.delete(self.redis_model_key)
            else:
                for model_data in models:
                    if model_data["model_name"] == model_name:
                        model_data["model"] = model_data
                        break
                yaml_models = yaml.dump(models, default_flow_style=False, sort_keys=False)
                self.session.redis_client.set(self.redis_model_key, yaml_models)

    def update_model(self, model_name: str, model_data: dict[str, Any]):
        # Converting the current model to python.
        self.update_model_graph(model_data, model_name)
        parser: ConfigParser = self.convert_to_python(model_data, model_name)
        self._update_model(model_name=model_name, model_data=model_data)
        sequence_orders, sequence_lineage = set_transformation_sequence(parser)

        model_data_yaml: Any | str = yaml.dump(model_data, indent=4, default_flow_style=False)
        return {
            "sequence_orders": sequence_orders,
            "sequence_lineage": sequence_lineage,
            "model_data": model_data,
            "model_data_yaml": model_data_yaml
        }

    def get_model_validator(self, model_data: dict[str, Any], model_name: str) -> ModelValidator:
        return ModelValidator(
            updated_model_data=model_data,
            model_name=model_name,
            session=self.session,
            visitran_context=self.visitran_context,
        )

    def validate_model(
        self,
        new_model_data: dict[str, Any],
        model_name: str,
        config_type: str = "all",
        transformation_type: str = None,
        transformation_id: str = None,
    ) -> None:
        """
        Validates a model's configuration and handles dependency resolution.

        Args:
            new_model_data (dict[str, Any]): The dict of the metadata of the model
            model_name (str): Name of the model
            transformation_type (str, optional): Type of the transformation
            transformation_id (str, optional):
            config_type: Explains about Create or update and delete operations
        Returns:
            None: This method performs validation and updates the new_model_data dict
                in-place. Specifically, it may modify the "reference" key in new_model_data
                to resolve any Method Resolution Order (MRO) issues in the dependency graph.

        Raises:
            ValidationError: If model validation fails or invalid configurations are detected.
            ReferenceError: If circular dependencies or unresolvable MRO issues are found.
            KeyError: If required! Keys are missing from new_model_data.
        """
        model_validator = self.get_model_validator(model_data=new_model_data, model_name=model_name)
        affected_columns = model_validator.validate(
            config_type=config_type,
            transformation_type=transformation_type,
            transformation_id=transformation_id
        )

        logger.info(
            "[DependencyCheck] model=%s config_type=%s transform_type=%s affected_columns=%s",
            model_name, config_type, transformation_type, affected_columns,
        )

        # Check the validation for child models
        model_dict = self.get_model_references()
        reference_validator = ValidateReferences(model_dict=model_dict, model_name=model_name)

        # Gather children from reference graph
        ref_child_models = set()
        if model_name in model_dict:
            ref_child_models = reference_validator.get_child_references()

        # Also discover children by table-level dependency: models whose source
        # table matches this model's destination table. This catches cases where
        # a model uses another model's output as source without an explicit reference.
        dest_schema = new_model_data.get("model", {}).get("schema_name")
        dest_table = new_model_data.get("model", {}).get("table_name")
        table_child_models = self._get_source_dependent_models(
            model_name, dest_schema, dest_table
        )
        all_child_models = ref_child_models | table_child_models

        logger.info(
            "[DependencyCheck] model=%s dest=%s.%s ref_children=%s table_children=%s",
            model_name, dest_schema, dest_table, ref_child_models, table_child_models,
        )

        if all_child_models:
            if config_type in ("source", "model") and model_validator.old_table_details:
                model_validator.validate_child_table_usage(
                    child_model_names=list(all_child_models),
                    affected_table=model_validator.old_table_details,
                )

            if affected_columns:
                model_validator.validate_child_models(
                    child_model_names=list(all_child_models),
                    affected_columns=affected_columns,
                )

        logger.info(
            "[DependencyCheck] model=%s dependency_columns=%s",
            model_name, dict(model_validator.dependency_columns),
        )

        if model_validator.dependency_columns:
            raise MultipleColumnDependency(
                model_name=model_name,
                transformation_name=transformation_type,
                affected_columns=affected_columns,
                dependency_details=model_validator.dependency_columns
            )
        # Preserve original reference order - the first reference determines parent class for DAG execution
        original_references = new_model_data.get("reference", [])
        reference_validator.model_dict[model_name] = set(original_references)
        reference_validator.validate_table_usage_references(new_model_data=new_model_data, session=self.session)
        updated_model_dict = reference_validator.detect_and_fix_mro_issues()
        # Restore order: keep original order for items that remain, append any new items at the end
        final_refs = updated_model_dict[model_name]
        ordered_refs = [ref for ref in original_references if ref in final_refs]
        new_refs = [ref for ref in final_refs if ref not in original_references]
        new_model_data["reference"] = ordered_refs + new_refs

    def save_model_file(self, no_code_data: dict[str, Any], model_name: str, is_chat_response: bool, is_update: bool = False):
        if is_chat_response:
            model_data = no_code_data["file"]
        else:
            # Converting the YAML configuration to python JSON
            model_data: dict[str, Any] = yaml.safe_load(no_code_data["file"])

        # Preserve 'model' attribute (destination) when updating via AI chat response
        if is_chat_response and is_update:
            existing_model_data = self.session.fetch_model_data(model_name=model_name)
            model_data["model"] = existing_model_data.get("model", model_data.get("model", {}))

        if "presentation" not in model_data:
            model_data["presentation"] = {"sort": [], "hidden_columns": ["*"]}

        # This is to validate the Models from visitran AI
        for criteria in model_data.get("transform", {}).get("filter", {}).get("criteria", []):
            rhs_data = criteria.get("condition", {}).get("rhs", {})
            if "value" not in rhs_data and "column" in rhs_data:
                column_data = rhs_data.pop("column", {})
                criteria["condition"]["rhs"]["value"] = [column_data.get("column_name", "")]

        # Validating the model data before persisting
        self.validate_model(
            new_model_data=model_data, model_name=model_name
        )

        # Converting the current model to python.
        self.update_model_graph(model_data, model_name)
        self._update_model(model_name=model_name, model_data=model_data)

    def validate_model_file(self, no_code_data: dict[str, Any], model_name: str):
        # Converting the YAML configuration to python JSON
        model_data: dict[str, Any] = yaml.safe_load(no_code_data["file"])

        current_transformation: str = no_code_data.get("current_transformation", "")

        if "presentation" not in model_data:
            model_data["presentation"] = {"sort": [], "hidden_columns": ["*"]}

        # Validating the model data before persisting
        self.validate_model(
            new_model_data=model_data,
            model_name=model_name,
            transformation_type=current_transformation,
        )

    def backup_current_no_code_model(self) -> None:
        """Creates a backup of the current model.

        This method ensures that all current models are backed up using the
        `backup_all_model` functionality provided by the session object. It does not
        return any value and performs the backup operation without additional
        parameters.

        :return: None
        """
        self.session.backup_all_model()

    def execute_visitran_seed_command(self, seed_details, environment_id=None) -> list:
        """Executes the Visitran seed command by passing seed details to a
        Visitran instance and returns the results of the operation. The method
        creates a `Visitran` object using the provided context, runs the seeds
        based on the input details, and retrieves the output as a list.

        :param environment_id: Optional environment id to override the
            connection settings
        :param seed_details: Dictionary containing seed-related
            information to be processed by the Visitran instance.
        :return: list containing the results from running the seeds with
            Visitran.
        :rtype: list
        """
        if environment_id:
            env_model = EnvironmentSession.get_environment_model(environment_id)
            env_payload = env_model.decrypted_connection_data
            self.visitran_context.override_connections_data(env_data=env_payload)
        visitran_obj = Visitran(context=self.visitran_context)
        if self.project_instance.project_schema != seed_details.get("schema_name"):
            logging.info(
                f"Changing connection schema to {seed_details.get('schema_name')} for project {self.project_name}"
            )
            self.project_instance.project_schema = seed_details.get("schema_name")
            self.project_instance.save()
        if not self.project_instance.project_schema:
            raise SchemaNotFoundError(project_name=self.session.project_instance.project_name)
        seed_result = visitran_obj.run_seeds(seed_details)
        self.visitran_context.clear_database_cache()
        return seed_result

    def get_supported_reference_models(self, current_model: str, current_references: list[str]) -> list[str]:
        """Added unique code for each DAG to build a validation in frontend."""
        model_dict = self.get_model_references()
        model_dict[current_model] = set(current_references)
        ref_validator = ValidateReferences(model_dict=model_dict, model_name=current_model)
        valid_reference_model = ref_validator.get_valid_references()
        return valid_reference_model

    def compile_yaml_data(
        self,
        model_data: dict[str, Any],
        file_name: str,
        current_model_reference: dict[str, Any] = None,
    ):
        self._parser = self.get_config_parser(model_data, file_name)
        self._parser.all_reference = current_model_reference
        executor = Interpreter(
            config_parser=self._parser,
            file_explorer=self.file_explorer,
            visitran_context=self.visitran_context,
        )
        executor.parse_to_py()
        return self._parser, executor

    def execute_run(self, environment_id=None, model_name: str = None, model_names: list = None):
        """
        Execute the visitran run command.

        Routes between legacy (Python) and direct (SQL) execution paths
        based on the VISITRAN_EXECUTION_MODE feature flag.

        Args:
            environment_id: Optional environment ID for connection details
            model_name: Single model name for selective execution (legacy support)
            model_names: List of model names for multi-model selective execution (AI Apply)
        """
        if environment_id:
            env_model = EnvironmentSession.get_environment_model(environment_id)
            env_payload = env_model.decrypted_connection_data
            self._reload_context(env_data=env_payload)

        # Route based on feature flag
        if ExecutionRouter.should_execute_direct():
            logger.info("[execute_run] Using DIRECT execution path (YAML → SQL)")
            return self._execute_direct()

        # Legacy path (default)
        logger.info("[execute_run] Using LEGACY execution path (YAML → Python → Ibis → SQL)")
        return self._execute_legacy(model_name=model_name, model_names=model_names)

    def _execute_legacy(self, model_name: str = None, model_names: list = None):
        """Legacy execution path: YAML → Python code → Ibis → SQL → Database."""
        visitran_obj = Visitran(context=self.visitran_context)
        try:
            self.session.sync_file_models()
            for model in self.session.fetch_all_models(fetch_all=True):
                if model_data := model.model_data:
                    logging.info(f"[Model Update] Converting YAML to Python: {model.model_name}")
                    self.update_model(model_name=model.model_name, model_data=model_data)
            self.session.add_sys_path()
            visitran_obj.search_n_run_models(model_name=model_name, model_names=model_names)
            return visitran_obj
        except ModuleNotFoundError as err:
            if "psycopg2" in str(err):
                raise VisitranPostgresMissingError()
            raise err
        finally:
            self.visitran_context.clear_database_cache()
            self.session.remove_sys_path()

    def _execute_direct(self):
        """Direct execution path: YAML → SQL Builder → SQL → Database.

        Bypasses Python code generation entirely. Uses DAGBuilder to construct
        the dependency graph from ConfigParser instances, then DAGExecutor to
        execute models in topological order via Ibis/SQL.
        """
        from backend.application.config_parser.model_registry import ModelRegistry

        try:
            self.session.sync_file_models()

            # Register all models in the ModelRegistry
            registry = ModelRegistry()
            registry.clear()
            configs = []
            for model in self.session.fetch_all_models(fetch_all=True):
                if model_data := model.model_data:
                    schema = self.visitran_context.get_profile_schema()
                    config = ConfigParser(model_data, model.model_name)
                    config._dialect = self.visitran_context.database_type
                    registry.register(schema, model.model_name, config)
                    configs.append(config)

            # Build DAG and execute
            dag_builder = DAGBuilder(registry=registry, configs=configs)
            dag = dag_builder.build()

            executor = DAGExecutor(
                dag=dag,
                registry=registry,
            )
            result = executor.execute()

            if not result.success:
                failed = [m for m in result.model_results if m.status.value == "failed"]
                if failed:
                    raise VisitranBackendBaseException(
                        error_message=f"Model execution failed: {failed[0].error_message}"
                    )

            logger.info(
                f"[_execute_direct] Completed: {result.models_executed} models, "
                f"{result.models_failed} failed"
            )
            return result
        finally:
            self.visitran_context.clear_database_cache()

    def execute_visitran_run_command(self, current_model: str = "", current_models: list = None, environment_id=None) -> None:
        """
        Execute the visitran run command with selective model execution.

        Args:
            current_model: Single model name for selective execution (right-click Run)
            current_models: List of model names for multi-model execution (AI Apply)
            environment_id: Optional environment ID for connection details

        Execution modes:
            - current_models provided: Execute only these models + their downstream children
            - current_model provided: Execute only this model + its downstream children
            - Neither provided: Execute ALL models
        """
        logging.info(f"[execute_visitran_run_command] Called with current_model={current_model}, current_models={current_models}, environment_id={environment_id}")
        try:
            CacheService.clear_cache(f"model_content_{self.project_instance.project_id}_*")

            if current_models:
                # Multi-model execution (AI Apply)
                logging.info(f"[execute_visitran_run_command] Multi-model execution for: {current_models}")
                self.execute_run(environment_id=environment_id, model_names=current_models)
                self.session.redis_client.delete(self.redis_db_metadata_key)
            elif current_model:
                # Single model execution (right-click Run)
                logging.info(f"[execute_visitran_run_command] Single model execution for: {current_model}")
                self.execute_run(environment_id=environment_id, model_name=current_model)
                self.session.redis_client.delete(self.redis_db_metadata_key)
            else:
                # Run all models
                logging.info(f"[execute_visitran_run_command] Running ALL models")
                self.execute_run(environment_id=environment_id)

            logging.info(f"[execute_visitran_run_command] Completed successfully")
        except (VisitranBackendBaseException, VisitranBaseExceptions) as visitran_err:
            logging.error(f"[execute_visitran_run_command] Error: {visitran_err}")
            rollback_model = current_model or (current_models[0] if current_models else "")
            visitran_err.error_args().update(
                {"is_rollback": self.session.is_rollback_exist(rollback_model)}
            )
            raise visitran_err

    @staticmethod
    def validate_sql(query: str) -> None:
        # Parse the SQL query to check syntax

        parsed = parse(query)
        if not parsed:
            raise InvalidSQLQuery(sql_query=query)

        # Check for potentially destructive SQL keywords
        prohibited_keywords = ["DELETE", "TRUNCATE"]
        for statement in parsed:
            tokens_list = statement.tokens
            for token in tokens_list:
                if token.value.upper() in prohibited_keywords:
                    raise ProhibitedSqlQuery(
                        prohibited_action=token.value.upper(),
                        prohibited_actions=prohibited_keywords,
                    )

    def execute_sql_command(self, sql_command: str, limit: int = 100) -> list[dict[str, Any]]:
        # Validate the SQL command before execution
        self.validate_sql(sql_command)

        # Safe execution of the SQL command
        content = self.visitran_context.db_adapter.db_connection.execute_llm_sql_query(sql_query=sql_command, limit=limit)
        return content

    def get_lineage_model_details(self, model_name: str, content_type: str = "sql") -> dict[str, Any]:
        """This method is used to fetch the fields internal dependencies.

        Args:
            model_name: The name of the model
            content_type: The type of content to return ('sql' or 'python')

        Returns:
            A dictionary containing model details including source, destination, joins,
            sequence lineage, and either SQL or Python content based on the content_type parameter
        """
        no_code_model: dict = self.session.fetch_model_data(model_name=model_name)

        # Get SQL query data
        sql_query: dict[str, Any] = self.session.get_model_dependency_data(
            model_name=model_name, transformation_id="sql"
        )
        source_schema_name: str = no_code_model.get("source", {}).get("schema_name", "")
        source_table_name: str = no_code_model.get("source", {}).get("table_name", "")
        destination_schema_name: str = no_code_model.get("model", {}).get("schema_name", "")
        destination_table_name: str = no_code_model.get("model", {}).get("table_name", "")
        joined_tables = []
        for key, value in no_code_model.get("transform", {}).items():
            if value.get("type") == "join":
                for join_item in value.get("join", {}).get("tables", []):
                    joined_table = join_item.get("joined_table", {})
                    schema = joined_table.get("schema_name", "")
                    table = joined_table.get("table_name", "")
                    if schema and table:
                        joined_tables.append(f"{schema}.{table}")

        # Get sequence lineage data for the model
        try:
            config_parser = self.get_config_parser(model_data=no_code_model, file_name=model_name)
            sequence_orders, sequence_lineage = set_transformation_sequence(config_parser)
        except Exception as e:
            logger.error(f"Error generating sequence lineage: {str(e)}")
            sequence_orders = {}
            sequence_lineage = {"data": {"nodes": [], "edges": []}}

        # Prepare the base table details
        table_details = {
            "source_table_name": source_schema_name + "." + source_table_name,
            "destination_table_name": destination_schema_name + "." + destination_table_name,
            "joined_table": joined_tables,
            "sql": sql_query,
            "sequence_orders": sequence_orders,
            "sequence_lineage": sequence_lineage,
        }

        # If Python content is requested, fetch the model's Python content
        if content_type == "python":
            try:
                # Get the model instance
                model = self.session.fetch_model(model_name=model_name)

                # Check if the model has Python content
                if model and model.model_py_content:
                    # Read the Python content
                    python_content = model.model_py_content.read().decode("utf-8")
                    table_details["python_content"] = python_content
                else:
                    table_details["python_content"] = "# No Python content available for this model"
            except Exception as e:
                print(f"Error fetching Python content: {str(e)}")
                table_details["python_content"] = f"# Error fetching Python content: {str(e)}"
        return table_details

    def get_model_table_details(
        self,
        model_name: str,
        transformation_id: str = "",
        transformation_type: str = "current",
    ) -> dict[str, Any]:
        """This method is used to fetch the fields internal dependencies."""
        no_code_model: dict = self.session.fetch_model_data(model_name=model_name)
        config_parser = self.get_config_parser(
            model_data=no_code_model, file_name=model_name
        )

        table_details: dict[str, Any] = {
            "source_schema_name": config_parser.source_schema_name,
            "source_table_name": config_parser.source_table_name,
            "destination_schema_name": config_parser.destination_schema_name,
            "destination_table_name": config_parser.destination_table_name,
        }

        if transformation_id:
            column_dependency_key = transformation_id
            if transformation_type not in ["pivot", "groups_and_aggregation"]:
                column_dependency_key = f"{transformation_id}_transformed"
            transformation_columns: dict[str, Any] = (
                self.session.get_model_dependency_data(
                    model_name=model_name,
                    transformation_id=column_dependency_key,
                    default={},
                )
            )
            if transformation_columns:
                transformation_columns["column_names"] = {
                    transformation_type: transformation_columns["column_names"],
                    "joined_tables": {},
                    "visible": transformation_columns["column_names"],
                }
                table_details.update(transformation_columns)

            if transformation_type == "rename_column":
                # fetching the source table columns which can be mapped in joins
                column_details: list[Any] = self.get_table_columns(
                    config_parser.destination_schema_name,
                    config_parser.destination_table_name,
                )

                column_names = []
                column_descriptions = {}
                for column in column_details:
                    column_names.append(column["column_name"])
                    column_descriptions[column["column_name"]] = column
                transformation_columns["column_names"]["visible"] = column_names
            return table_details

        # fetching the source table columns which can be mapped in joins
        source_table_columns: list[str] = []
        source_column_details: list[Any] = self.get_table_columns(
            config_parser.source_schema_name,
            config_parser.source_table_name
        )

        for column in source_column_details:
            source_table_columns.append(column["column_name"])

        # fetching the destination table columns which can be mapped in joins
        column_details: list[Any] = self.get_table_columns(
            config_parser.destination_schema_name,
            config_parser.destination_table_name
        )

        column_names = []
        column_descriptions = {}
        for column in column_details:
            column_names.append(column["column_name"])
            column_descriptions[column["column_name"]] = column

        joined_tables: dict[str, int] = {}
        colour_code = 0
        for key, value in no_code_model.get("transform", {}).items():
            if value["type"] == "join":
                for join_list in value.get("join", {}).get("tables", {}):
                    if destination_table := join_list.get("joined_table", {}):
                        _schema: str = destination_table.get("schema_name")
                        _alias: str = destination_table.get("alias_name")
                        table_name: str = destination_table.get("table_name")
                        _alias_name: str = table_name # or _alias is removed for temporary support
                        column_details = self.get_table_columns(schema_name=_schema, table_name=table_name)
                        for _column in column_details:
                            _column_name = _column["column_name"]
                            table_col = f"{_alias_name}_{_column_name}"
                            if table_col in column_names:
                                joined_tables[f"{_alias_name}_{_column_name}"] = colour_code
                            elif _column_name in column_names and _column_name not in source_table_columns:
                                joined_tables[_column_name] = colour_code
                        colour_code += 1

        table_details["column_names"]: dict[str, Any] = {
            transformation_type: column_names,
            "visible": column_names,
        }
        table_details["joined_tables"] = joined_tables
        table_details["column_description"] = column_descriptions
        return table_details

    def get_model_content(
        self, model_name: str, page: int = 1, limit: int = 100
    ) -> dict[str, Any]:
        """Get model content with pagination and column details.

        Args:
            model_name: Name of the model to fetch content for
            page: Page number for pagination (1-based)
            limit: Number of records per page

        Returns:
            Dictionary containing model content, metadata and column information
        """
        # Get table details once to avoid multiple lookups
        table_details = self.get_model_table_details(model_name=model_name)

        # Get source and destination details
        dest_schema = table_details["destination_schema_name"]
        dest_table = table_details["destination_table_name"]

        # Get model spec from fetch_model_data
        no_code_model = self.session.fetch_model_data(model_name=model_name)
        model_info = no_code_model.get("model", {})
        model_schema_name = model_info.get("schema_name")
        model_table_name = model_info.get("table_name")

        all_columns = self.visitran_context.get_table_columns(schema_name=model_schema_name, table_name=model_table_name)

        hidden_columns = no_code_model.get("presentation", {}).get("hidden_columns", [])
        # Filter: exclude columns present in hidden_columns
        selective_columns = [col for col in all_columns if col not in hidden_columns]


        # Get table content and count in parallel
        table_content: list[Any] = self.visitran_context.get_table_records(
            schema_name=dest_schema,
            table_name=dest_table,
            selective_columns=selective_columns,
            limit=limit,
            page=page,
        )

        total_records = self.visitran_context.get_table_record_count(
            schema_name=dest_schema,
            table_name=dest_table,
        )

        # Get sequence information
        _parser = self.get_config_parser(no_code_model, model_name)
        sequence_orders, sequence_lineage = set_transformation_sequence(_parser)

        column_names = table_details["column_names"]["current"]

        # Filter column_description for only selective_columns
        all_column_description = table_details.get("column_description", {})
        visible_column_description = {
            col: all_column_description[col]
            for col in selective_columns
            if col in all_column_description
        }
        # Build response with sorted lists and optimized structure
        return {
            "content": table_content,
            "total": total_records,
            "schema_name": dest_schema,
            "sequence_orders": sequence_orders,
            "sequence_lineage": sequence_lineage,
            "column_names": {
                "current": column_names,
                "joined_tables": table_details["joined_tables"],
                "visible": column_names,
            },
            "all_column_description": table_details["column_description"],
            "column_description": visible_column_description,
        }

    def get_full_model_content_for_export(self, model_name: str) -> dict[str, Any]:
        """This method fetches all model content for CSV export without
        pagination limits."""
        try:
            # Get table details first (same pattern as get_model_content)
            table_details = self.get_model_table_details(model_name=model_name)
            dest_schema = table_details.get("destination_schema_name", "")
            dest_table = table_details.get("destination_table_name", "")

            if not dest_schema or not dest_table:
                raise CsvDownloadFailed(
                    table_name=model_name,
                    reason="Unable to determine table schema or name",
                )

            # Get model spec from fetch_model_data (same as get_model_content)
            no_code_model = self.session.fetch_model_data(model_name=model_name)
            model_info = no_code_model.get("model", {})
            model_schema_name = model_info.get("schema_name")
            model_table_name = model_info.get("table_name")

            # Get all columns (selective_columns is required by get_table_records)
            all_columns = self.visitran_context.get_table_columns(
                schema_name=model_schema_name, table_name=model_table_name
            )

            # Get hidden columns from presentation
            hidden_columns = no_code_model.get("presentation", {}).get("hidden_columns", [])

            # Filter: exclude columns present in hidden_columns
            selective_columns = [col for col in all_columns if col not in hidden_columns]

            # Get total record count
            total_records = self.visitran_context.get_table_record_count(
                schema_name=dest_schema, table_name=dest_table
            )

            # Use a large limit to fetch all records in one go
            limit = max(total_records, 1_000_000) if total_records > 0 else 1_000_000

            # Fetch all records with only visible columns
            records = self.visitran_context.get_table_records(
                schema_name=dest_schema,
                table_name=dest_table,
                selective_columns=selective_columns,
                limit=limit,
                page=1,
            )

            return {
                "content": records,
                "hidden_columns": hidden_columns,
                "total_records": total_records,
                "schema_name": dest_schema,
                "table_name": dest_table,
                "visible_columns": selective_columns,
            }

        except Exception as e:
            raise CsvDownloadFailed(table_name=model_name, reason=str(e))

    def rollback_model_content(self, model_name: str) -> AnyStr:
        model_data = self.session.rollback_model(model_name=model_name)
        response_data = self.update_model(model_name=model_name, model_data=model_data)
        return response_data["model_data_yaml"]

    def get_database_explorer(self, reload_db=False):
        if reload_db:
            self.visitran_context.clear_database_cache()
        table_schema_structure: dict[str, Any] = self.visitran_context.get_database_explorer()
        return table_schema_structure

    def cleanup_no_code_model(self, table_delete_enabled: bool):
        try:
            config_models: list[ConfigModels] = self.session.fetch_all_models(fetch_all=True)
            for current_model in config_models:
                model_name = current_model.model_name
                logging.info(f"clean up initiated for model {model_name}....")
                if table_delete_enabled:
                    self.delete_destination_table(model_name=current_model.model_name, force=True)
                self.session.delete_model(model_name=model_name)
                logging.info(f"model: {model_name} is deleted")
                logging.info("model cache is cleared")
                self.delete_node_from_model_graph(model_name)
                logging.info(f"Node deleted from graph\nAll cleanup done for model {model_name}")

        except Exception as e:
            logging.critical(f"Failed to cleanup no code model due to {e}")

    def _get_transformation_details(
        self,
        no_code_model: dict[str, Any],
        sequence_orders: dict[str, int]
    ) -> dict[str, Any]:
        """
        Extract detailed information about each transformation in the model.

        Args:
            no_code_model: The complete no-code model configuration
            sequence_orders: Dictionary mapping transform IDs to their execution order

        Returns:
            Dictionary mapping transform IDs to their detailed configuration
        """
        details = {}

        # Get transform configuration and presentation
        transform_config = no_code_model.get("transform", {})
        presentation = no_code_model.get("presentation", {})

        logging.info(f"=== Transformation Details Extraction ===")
        logging.info(f"sequence_orders keys: {list(sequence_orders.keys())}")
        logging.info(f"transform_config keys: {list(transform_config.keys())}")
        logging.info(f"presentation keys: {list(presentation.keys())}")

        # Build mapping: sequence_orders has plural keys ("filters"), transform_config has UUID keys ("filter_uuid")
        # We need to match them by transform type
        transform_type_to_config = {}
        for config_id, config_data in transform_config.items():
            if isinstance(config_data, dict):
                transform_type = config_data.get("type")
                if transform_type:
                    # Store the config_id and its nested config
                    if transform_type not in transform_type_to_config:
                        transform_type_to_config[transform_type] = []

                    # Get the actual nested config (e.g., config_data["filter"] for filter transforms)
                    nested_config = config_data.get(transform_type, {})
                    transform_type_to_config[transform_type].append({
                        "id": config_id,
                        "config": nested_config
                    })

        logging.info(f"Transform type to config mapping: {list(transform_type_to_config.keys())}")

        # Process each transformation in the sequence
        for transform_key, order in sequence_orders.items():
            if order is None or order <= 0:
                continue

            logging.info(f"Processing transform_key: {transform_key}, order: {order}")

            # Check if this is a presentation item (sort or hidden_columns)
            if transform_key == "sort" or transform_key == "sort_fields":
                sort_columns = presentation.get("sort", [])
                if sort_columns:
                    details[transform_key] = {
                        "type": "sort",
                        "count": len(sort_columns),
                        "columns": sort_columns
                    }
                    logging.info(f"  ✓ Processed presentation sort: {len(sort_columns)} columns")
                continue

            if transform_key == "hidden_columns":
                hidden_cols = presentation.get("hidden_columns", [])
                if hidden_cols and hidden_cols != ["*"]:
                    details[transform_key] = {
                        "type": "hidden_columns",
                        "count": len(hidden_cols),
                        "columns": hidden_cols
                    }
                    logging.info(f"  ✓ Processed presentation hidden_columns: {len(hidden_cols)} columns")
                continue

            # Map sequence_orders keys to transform types
            # "filters" -> "filter", "joins" -> "join", etc.
            type_map = {
                "filters": "filter",
                "joins": "join",
                "synthesize": "synthesize",
                "distinct": "distinct",
                "find_and_replace": "find_and_replace",
                "rename": "rename_column",
                "groups": "groups_and_aggregation",
                "groups_and_aggregation": "groups_and_aggregation",  # Add direct mapping
                "aggregate": "aggregate",
                "unions": "union",
                "pivot": "pivot",
                "unpivot": "unpivot",
                "combine_columns": "combine_columns"
            }

            transform_type = type_map.get(transform_key)
            if not transform_type:
                logging.warning(f"  ✗ Unknown transform key: {transform_key}")
                continue

            # Get the configs for this transform type
            configs = transform_type_to_config.get(transform_type, [])
            if not configs:
                logging.warning(f"  ✗ No configs found for type: {transform_type}")
                continue

            # Process the first config of this type (usually there's only one)
            config_item = configs[0]
            actual_config = config_item["config"]
            config_id = config_item["id"]

            logging.info(f"  ✓ Found config: {config_id} for type {transform_type}")

            # Extract details based on transformation type
            if transform_type in ["synthesize", "synthesize_column"]:
                # Synthesize has different structure: columns array with column_name and operation.formula
                columns_data = actual_config.get("columns", [])
                details[transform_key] = {
                    "type": transform_type,
                    "count": len(columns_data),
                    "columns": [
                        {
                            "name": col.get("column_name"),
                            "formula": col.get("operation", {}).get("formula")
                        }
                        for col in columns_data
                    ]
                }

            elif transform_type == "filter":
                criteria = actual_config.get("criteria", [])
                parsed_conditions = []

                for criterion in criteria:
                    condition = criterion.get("condition", {})
                    lhs = condition.get("lhs", {})
                    operator = condition.get("operator", "")
                    rhs = condition.get("rhs", {})

                    lhs_column = lhs.get("column", {})
                    column_name = lhs_column.get("column_name", "")

                    # Handle different operators
                    if operator == "NOTNULL":
                        parsed_conditions.append({
                            "column": column_name,
                            "operator": "IS NOT NULL",
                            "value": ""
                        })
                    else:
                        rhs_value = rhs.get("value", [""])[0] if rhs.get("value") else ""
                        operator_map = {"EQ": "=", "NE": "!=", "GT": ">", "LT": "<", "GTE": ">=", "LTE": "<=", "IN": "IN", "LIKE": "LIKE"}
                        op_display = operator_map.get(operator, operator)

                        parsed_conditions.append({
                            "column": column_name,
                            "operator": op_display,
                            "value": str(rhs_value)
                        })

                details[transform_key] = {
                    "type": transform_type,
                    "count": len(parsed_conditions),
                    "conditions": parsed_conditions
                }

            elif transform_type == "rename_column":
                mappings = actual_config.get("mappings", [])
                details[transform_key] = {
                    "type": transform_type,
                    "count": len(mappings),
                    "mappings": [
                        {
                            "old_name": m.get("old_name"),
                            "new_name": m.get("new_name")
                        }
                        for m in mappings
                    ]
                }

            elif transform_type in ["groups", "aggregate", "groups_and_aggregation"]:
                # Handle both "group_by" and "group" field names
                group_by = actual_config.get("group_by") or actual_config.get("group", [])
                aggregate_columns = actual_config.get("aggregate_columns", [])

                logging.info(f"  Aggregation - group_by: {group_by}, aggregations: {len(aggregate_columns)}")

                details[transform_key] = {
                    "type": transform_type,
                    "group_by": group_by,
                    "aggregations": [
                        {
                            "function": agg.get("function"),
                            "column": agg.get("column"),
                            "alias": agg.get("alias")
                        }
                        for agg in aggregate_columns
                    ]
                }

                logging.info(f"  ✓ Processed aggregation details for {transform_key}")

            elif transform_type == "join":
                tables = actual_config.get("tables", [])
                if tables:
                    table = tables[0]
                    join_type = table.get("type", "Inner")
                    criteria = table.get("criteria", [])
                    joined_table = table.get("joined_table", {})

                    # Extract condition details from nested structure
                    left_table = ""
                    right_table = joined_table.get("table_name", "")
                    on_condition = ""

                    if criteria:
                        condition = criteria[0].get("condition", {})
                        lhs = condition.get("lhs", {}).get("column", {})
                        rhs = condition.get("rhs", {}).get("column", {})

                        lhs_table = lhs.get("table_name", "")
                        lhs_col = lhs.get("column_name", "")
                        rhs_table = rhs.get("table_name", "")
                        rhs_col = rhs.get("column_name", "")

                        operator_map = {"EQ": "=", "NE": "!=", "GT": ">", "LT": "<", "GTE": ">=", "LTE": "<="}
                        op = operator_map.get(condition.get("operator", "EQ"), "=")

                        left_table = lhs_table
                        on_condition = f"{lhs_table}.{lhs_col} {op} {rhs_table}.{rhs_col}"

                    details[transform_key] = {
                        "type": transform_type,
                        "join_type": join_type,
                        "left_table": left_table,
                        "right_table": right_table,
                        "on": on_condition
                    }

            elif transform_type == "distinct":
                columns = actual_config.get("columns", [])
                details[transform_key] = {
                    "type": transform_type,
                    "description": f"Remove duplicates based on: {', '.join(columns)}" if columns else "Remove duplicate rows from the dataset",
                    "columns": columns
                }

            elif transform_type == "find_and_replace":
                replacements = actual_config.get("replacements", [])
                parsed_replacements = []

                for replacement in replacements:
                    column_list = replacement.get("column_list", [])
                    operations = replacement.get("operation", [])

                    for column in column_list:
                        for op in operations:
                            parsed_replacements.append({
                                "column": column,
                                "find": op.get("find"),
                                "replace": op.get("replace")
                            })

                details[transform_key] = {
                    "type": transform_type,
                    "count": len(parsed_replacements),
                    "replacements": parsed_replacements
                }

        logging.info(f"=== Transformation Details Extracted ===")
        logging.info(f"Total details extracted: {len(details)}")
        logging.info(f"Details keys: {list(details.keys())}")

        return details

