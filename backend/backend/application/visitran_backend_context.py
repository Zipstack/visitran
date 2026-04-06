import json
import logging
import os
from collections import defaultdict
from typing import Dict, Any, List, Set, Union

import yaml
from ibis.common.exceptions import IbisTypeError
from ibis.expr.types.relations import Table
from sqlalchemy.exc import ProgrammingError
from visitran.adapters.connection import BaseConnection
from visitran.errors import ConnectionFailedError
from visitran.singleton import Singleton
from visitran.utils import get_adapter_connection_cls
from visitran.visitran_context import VisitranContext

from backend.application.database_explorer import DatabaseExplorerTree
from backend.application.session.session import Session
from backend.core.models.csv_models import CSVModels
from backend.errors import TableContentIssue
from backend.utils.constants import FileConstants as Fc
from backend.utils.decryption_utils import decrypt_sensitive_fields
from backend.utils.utils import download_from_gcs

CACHE_TTL_SECONDS = 1800  # 30 minutes idle timeout


def fetch_abs_path(csv_files, csv_model):
    try:
        csv_path = csv_model.csv_field.path
    except NotImplementedError:
        csv_path = csv_model.csv_field.url
    csv_files.append(
        {
            "file_name": csv_model.csv_name,
            "file_path": csv_path,
            "schema_name": csv_model.table_schema,
        }
    )


class VisitranBackendContext(VisitranContext):
    def __init__(
        self,
        project_config: Dict[str, Any],
        session: Session,
        is_api_call: bool = False,
        env_data: Dict[str, Any] = None,
    ):
        self.session = session
        self._select_models: list = []
        self._exclude_models: list = []
        self._model_configs: Dict[str, Any] = {}  # Per-model deployment configuration
        super().__init__(project_config, is_api_call, env_data)

    # --------------- CACHE: Key helpers using BaseSession's tenant/project ---------------
    def _cache_key(self, *parts: str) -> str:
        safe_parts = [str(p).replace(" ", "_") for p in parts]
        return f"org:{self.session.tenant_id}:proj:{self.session.project_id}:" + ":".join(safe_parts)

    def _cache_get(self, key: str):
        try:
            raw = self.session.redis_client.get(key)
            if raw is None:
                return None
            # sliding TTL refresh
            self.session.redis_client.expire(key, CACHE_TTL_SECONDS)
            return json.loads(raw)
        except Exception:
            logging.exception("VisitranBackendContext cache get failed for key=%s", key)
            return None

    def _cache_set(self, key: str, value):
        try:
            self.session.redis_client.set(key, json.dumps(value), ex=CACHE_TTL_SECONDS)
        except Exception:
            logging.exception("VisitranBackendContext cache set failed for key=%s", key)

    def _cache_delete(self, *keys: str):
        try:
            if not keys:
                return
            self.session.redis_client.delete(*keys)
        except Exception as e:
            logging.error(f"VisitranBackendContext cache delete failed keys={keys}")
            logging.exception(e)

    # -------------------- Basic properties passthrough --------------------
    @property
    def project_name(self) -> str:
        return self.session.project_instance.project_name

    @property
    def project_py_name(self) -> str:
        return self.session.project_instance.project_py_name

    @property
    def get_includes(self):
        return self._select_models

    @property
    def get_excludes(self):
        return self._exclude_models

    @property
    def model_configs(self) -> Dict[str, Any]:
        """Per-model deployment configuration for materialization overrides."""
        return self._model_configs

    @model_configs.setter
    def model_configs(self, value: Dict[str, Any]):
        self._model_configs = value or {}

    @property
    def database_type(self):
        return self.session.project_instance.database_type

    @property
    def db_connection_details(self) -> dict[str, Union[str, int]]:
        return self.env_data if self.env_data else self.session.project_instance.connection_model.decrypted_connection_details

    def get_profile_schema(self) -> str:
        if self.database_type == "bigquery":
            return str(self._conn_details.get("dataset_id", ""))
        if self.database_type == "duckdb":
            return "default"
        return str(self._conn_details.get("schema", ""))

    # -------------------- Seeds & files --------------------
    def get_seed_file(self, csv_file_name: str):
        csv_model = self.session.fetch_csv_model(csv_file_name)
        csv_files = []
        fetch_abs_path(csv_files, csv_model)
        return csv_files

    def get_seed_files(self) -> List[Dict[str, Any]]:
        csv_models = self.session.fetch_all_csv_files()
        csv_files = []
        for csv_model in csv_models:
            fetch_abs_path(csv_files, csv_model)
        return csv_files

    def update_seed_run_status(self, **kwargs) -> None:
        status = kwargs.get("status")
        file_name = kwargs.get("file_name")
        error_msg = kwargs.get("error_message", "")
        csv_model: CSVModels = self.session.fetch_csv_model(file_name)
        csv_model.status = status
        if str(csv_model.status).lower() == "success":
            csv_model.table_name = kwargs.get("destination_table", "")
            csv_model.table_schema = kwargs.get("table_schema", "")
        csv_model.error_message = error_msg
        csv_model.save()

    # -------------------- Model files --------------------
    def get_model_files(self) -> List[Dict[str, Any]]:
        Singleton.reset_cache()
        no_code_models = []
        models = self.session.fetch_all_models()
        logging.info(f"Total models fetched: {len(models)}")
        for model in models:
            try:
                model_path = model.model_py_content.path.split(self.session.project_py_path + os.path.sep)[-1]
            except NotImplementedError:
                # Store in local files
                logging.warning(f"Model {model.model_name} is stored in GCS. Downloading it to local.")
                model_path = model.model_py_content.url
                destination_path = str(os.path.join(self.session.model_path_prefix, model.model_name + Fc.PY))
                download_from_gcs(gcs_url=model_path, destination_file_name=destination_path)
                model_path = os.path.join(self.session.project_py_name, Fc.MODELS, model.model_name + Fc.PY)
            model_path = model_path.replace(Fc.SLASH, Fc.DOT).split(Fc.PY)[0]
            no_code_models.append({"model_name": model.model_name, "model_path": model_path})
        logging.info(f"The no code models sent for execution - {no_code_models}")
        return no_code_models

    @staticmethod
    def _collect_downstream(model_name: str, children_of: Dict[str, Set[str]]) -> Set[str]:
        """Collect all downstream dependents (children) of a model via BFS."""
        result = set()
        stack = [model_name]
        while stack:
            current = stack.pop()
            for child in children_of.get(current, set()):
                if child not in result:
                    result.add(child)
                    stack.append(child)
        return result

    @staticmethod
    def _collect_upstream(model_name: str, model_dict: Dict[str, Set[str]]) -> Set[str]:
        """Collect all upstream parents of a model via BFS."""
        result = set()
        stack = list(model_dict.get(model_name, set()))
        while stack:
            current = stack.pop()
            if current not in result:
                result.add(current)
                stack.extend(model_dict.get(current, set()) - result)
        return result

    def get_model_child_references(self, model_name: str) -> List[str]:
        """
        Get all downstream dependents (children) of a model.
        These are models that depend on the given model and need to be re-executed
        when the given model changes.
        """
        from backend.application.validate_references import ValidateReferences
        model_dict = {}
        models = self.session.fetch_all_models(fetch_all=True)
        for model in models:
            model_dict[model.model_name] = set(model.model_data.get("reference", []))
        reference_validator = ValidateReferences(model_dict=model_dict, model_name=model_name)
        child_references = list(reference_validator.get_child_references())
        child_references.append(model_name)
        return child_references

    def get_model_execution_subgraph(self, model_name: str) -> Dict[str, List[str]]:
        """
        Get the complete subgraph for executing a model.

        Returns:
            dict with:
            - models_to_execute: model + all downstream dependents (children) - will be executed
            - models_to_import: models_to_execute + all upstream dependencies (parents) - for DAG building

        Parents are imported for DAG building and materialization but NOT executed.
        Disjoint graphs are completely excluded.
        """
        logging.info(f"[get_model_execution_subgraph] Calculating subgraph for model: {model_name}")
        model_dict = {}
        models = self.session.fetch_all_models(fetch_all=True)
        for model in models:
            model_dict[model.model_name] = set(model.model_data.get("reference", []))
        logging.info(f"[get_model_execution_subgraph] Model dependency graph: {model_dict}")

        # Validate model exists before processing
        if model_name not in model_dict:
            logging.warning(f"[get_model_execution_subgraph] Model '{model_name}' not found in database")
            # Return just the model itself - let downstream code handle the error
            return {
                "models_to_execute": [model_name],
                "models_to_import": [model_name]
            }

        # Build reverse graph: model -> set of models that depend on it (children)
        children_of = defaultdict(set)
        for parent, refs in model_dict.items():
            for ref in refs:
                children_of[ref].add(parent)

        # Models that depend on this model (downstream children) - will be executed
        descendants = self._collect_downstream(model_name, children_of)
        descendants.add(model_name)
        logging.info(f"[get_model_execution_subgraph] Downstream children (will execute): {descendants}")

        # Get parents of ALL models in descendants (not just the original model)
        # This ensures that if child model B depends on model C, C is also imported
        ancestors = set()
        for desc_model in descendants:
            ancestors.update(self._collect_upstream(desc_model, model_dict))

        logging.info(f"[get_model_execution_subgraph] All upstream parents (materialize only): {ancestors}")

        result = {
            "models_to_execute": list(descendants),
            "models_to_import": list(descendants | ancestors)
        }
        logging.info(f"[get_model_execution_subgraph] Result: {result}")
        return result

    def get_multi_model_execution_subgraph(self, model_names: List[str]) -> Dict[str, List[str]]:
        """
        Get the combined subgraph for executing multiple models.

        This is used when AI generates/updates multiple models at once.
        Computes the union of all subgraphs to avoid redundant executions.

        Args:
            model_names: List of model names to execute

        Returns:
            dict with:
            - models_to_execute: Union of (each model + their downstream children)
            - models_to_import: models_to_execute + all upstream parents
        """
        if not model_names:
            return {"models_to_execute": [], "models_to_import": []}

        logging.info(f"[get_multi_model_execution_subgraph] Calculating subgraph for models: {model_names}")

        # Build model dependency graph once
        model_dict = {}
        models = self.session.fetch_all_models(fetch_all=True)
        for model in models:
            model_dict[model.model_name] = set(model.model_data.get("reference", []))

        # Build reverse graph once: model -> set of models that depend on it
        children_of = defaultdict(set)
        for parent, refs in model_dict.items():
            for ref in refs:
                children_of[ref].add(parent)

        # Collect all descendants (models to execute) from all input models
        all_descendants = set()
        for model_name in model_names:
            if model_name not in model_dict:
                logging.warning(f"[get_multi_model_execution_subgraph] Model '{model_name}' not found, adding anyway")
                all_descendants.add(model_name)
                continue

            descendants = self._collect_downstream(model_name, children_of)
            descendants.add(model_name)
            all_descendants.update(descendants)

        logging.info(f"[get_multi_model_execution_subgraph] Combined descendants (will execute): {all_descendants}")

        # Get parents of ALL descendants
        all_ancestors = set()
        for desc_model in all_descendants:
            all_ancestors.update(self._collect_upstream(desc_model, model_dict))

        logging.info(f"[get_multi_model_execution_subgraph] Combined ancestors (materialize only): {all_ancestors}")

        result = {
            "models_to_execute": list(all_descendants),
            "models_to_import": list(all_descendants | all_ancestors)
        }
        logging.info(f"[get_multi_model_execution_subgraph] Result: {result}")
        return result

    # -------------------- Database browsing --------------------
    def list_all_schemas(self) -> list[Any]:
        cache_key = self._cache_key("schemas", "list")
        cached = self._cache_get(cache_key)
        if cached is not None:
            logging.info(f"Fetching all schema's from cache: {cache_key}")
            return cached
        schemas = super().list_all_schemas()

        self._cache_set(cache_key, list(schemas))
        return schemas

    def list_all_tables(self, schema_name: str = "") -> list[Any]:
        effective_schema = schema_name or self.schema_name
        cache_key = self._cache_key("tables", "list", effective_schema or "default")
        cached = self._cache_get(cache_key)
        if cached is not None:
            logging.info(f"Fetching all tables from cache: {cache_key}")
            return cached
        tables = super().list_all_tables(schema_name=effective_schema)
        self._cache_set(cache_key, list(tables))
        return tables

    def get_table_records(
            self, schema_name: str, table_name: str, selective_columns: list[str] | None, limit: int, page: int
    ) -> list[Any]:
        try:
            return self.db_adapter.db_connection.get_table_records(
                schema_name=schema_name,
                table_name=table_name,
                selective_columns=selective_columns,
                limit=limit,
                pagination=page,
            )
        except (ProgrammingError, IbisTypeError) as err:
            if selective_columns:
                """
                Sometimes the table may not have the columns that are specified in the selective_columns.
                So, we need to fetch all the columns from the table.
                """
                logging.warning(f"Failed to fetch selective columns from table {table_name}. Fetching all columns.")
                return self.get_table_records(
                    schema_name=schema_name, table_name=table_name, selective_columns=None, limit=limit, page=page
                )
            raise err
        except Exception as e:
            logging.error(f"Failed to fetch table records for table {table_name}.")
            logging.exception(e)
            raise TableContentIssue(table_name=table_name, reason=str(e))

    def get_table_record_count(self, schema_name: str, table_name: str) -> int:
        return self.db_adapter.db_connection.get_table_row_count(schema_name=schema_name, table_name=table_name)

    def store_table_columns(self, transformation_id: str, model_name: str, table_obj: Table):
        column_description = {}
        column_names = []
        for column_name in table_obj.columns:
            column_names.append(str(column_name))
            db_type = str(table_obj[column_name].type())
            column_description[column_name] = {
                "column_name": str(column_name),
                "column_dbtype": db_type,
                "data_type": "",  # left blank here; not used in explorer build path
                "nullable": bool(table_obj[column_name].type().nullable),
            }
        columns = {"column_names": column_names, "column_description": column_description}
        self.session.update_model_dependency(
            model_name=model_name, transformation_id=transformation_id, model_data=columns
        )

    def store_sql_data(self, model_name, sql_query: str):
        sql_data = {"sql": sql_query}
        self.session.update_model_dependency(model_name=model_name, transformation_id="sql", model_data=sql_data)

    def test_connection(self) -> bool:
        try:
            self.db_adapter.db_connection.list_all_schemas()
        except Exception as err:
            logging.warning(f"Test connection failed for fetching tables: {err}")
            raise ConnectionFailedError(db_type=self.database_type, error_message=str(err))
        return True

    def test_connection_data(self, connection_data: dict[str, Any], db_type: str) -> bool:
        db_type = db_type or self.database_type
        if not connection_data:
            connection_data = {"file_path": f"{self.project_path}{os.path.sep}models/local.db"}

        # Decrypt sensitive fields from frontend encrypted data
        decrypted_connection_data = decrypt_sensitive_fields(connection_data)

        connection_cls: type[BaseConnection] = get_adapter_connection_cls(db_type)
        old_connection = self._conn_details
        self._conn_details = decrypted_connection_data
        try:
            connection_cls.connection_details = decrypted_connection_data
            con = connection_cls(**decrypted_connection_data)
            con.list_all_tables(self.get_profile_schema())
            return True
        except Exception as err:
            raise ConnectionFailedError(db_type=db_type, error_message=str(err)) from err
        finally:
            self._conn_details = old_connection

    # -------------------- Unified snapshot based on adapter metadata --------------------
    def _db_snapshot_cache_key(self) -> str:
        return self._cache_key("db", "snapshot")

    def _get_or_build_snapshot_from_db_meta(self) -> dict:
        """
        Load unified snapshot from cache, or build it using get_db_metadata() as the single source.
        Snapshot shape:
          {
            "ui_tree": {...},
            "db_meta_json": {...}
          }
        """
        db_snapshot_key = self._db_snapshot_cache_key()
        cached = self._cache_get(db_snapshot_key)
        if cached is not None and "ui_tree" in cached and "db_meta_json" in cached:
            logging.info(f"Fetched snapshot from cache: {db_snapshot_key}")
            return cached

        db_metadata: dict[str, Any] = self.db_adapter.get_db_details()

        # Build UI tree via DatabaseExplorerTree
        ui_tree = DatabaseExplorerTree.build_ui_tree(
            project_name=self.project_name,
            default_schema=self.get_profile_schema(),
            db_meta_json=db_metadata,
        )

        snapshot = {
            "ui_tree": ui_tree,
            "db_meta_json": db_metadata,
            "db_meta_yaml": yaml.dump(db_metadata, default_flow_style=False, sort_keys=False, indent=2)
        }
        self._cache_set(db_snapshot_key, snapshot)
        return snapshot

    def get_database_explorer(self) -> dict[str, Any]:
        snapshot = self._get_or_build_snapshot_from_db_meta()
        return snapshot["ui_tree"]

    # -------------------- Metadata methods (single source) --------------------
    def get_db_metadata(self) -> str:
        snapshot = self._get_or_build_snapshot_from_db_meta()
        return snapshot["db_meta_yaml"]

    def update_db_metadata(self, db_metadata: str, table_name: str, schema_name: str) -> str:
        updated = self.db_adapter.update_current_table_in_db_metadata(
            db_metadata=db_metadata, table_name=table_name, schema_name=schema_name
        )
        self.clear_database_cache()
        return updated

    def delete_db_metadata_for_table(self, db_metadata: str, table_name: str, schema_name: str) -> str:
        updated = self.db_adapter.delete_current_table_in_db_metadata(
            db_metadata=db_metadata, table_name=table_name, schema_name=schema_name
        )
        self.clear_database_cache()
        return updated

    # -------------------- Clear only database caches --------------------
    def get_project_model_graph_edges(self) -> list[tuple[str, str]]:
        """
        Get edges from the project model graph.
        Returns list of (source_model_name, target_model_name) tuples.
        """
        if hasattr(self.session, 'model_graph') and self.session.model_graph:
            return list(self.session.model_graph.graph.edges())
        return []

    def clear_database_cache(self) -> None:
        try:
            # Keys
            # Delete snapshot metadata
            snapshot_key = self._db_snapshot_cache_key()
            self._cache_delete(snapshot_key)

            # If schemas are cached, delete per-schema tables
            schemas_key = self._cache_key("schemas", "list")
            default_tables_key = self._cache_key("tables", "list", self.schema_name or "default")
            cached_schemas = self._cache_get(schemas_key)
            if cached_schemas is not None:
                for schema in cached_schemas:
                    schema_key = self._cache_key("tables", "list", str(schema) or "default")
                    self._cache_delete(schema_key)
            self._cache_delete(schemas_key, default_tables_key)

            if not (self.schema_name or "").strip():
                self._cache_delete(self._cache_key("tables", "list", "default"))

            logging.info(f"Cleared data warehouse caches for org={self.session.tenant_id} project={self.session.project_id}")
        except Exception as e:
            logging.critical("Failed to clear data warehouse caches")
            logging.exception(e)
