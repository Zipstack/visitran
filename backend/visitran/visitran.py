from __future__ import annotations

import concurrent.futures
import datetime
import importlib
import logging
import re
import sys
import warnings
from collections.abc import ValuesView
from inspect import isclass
from os import path
from types import ModuleType
from typing import TYPE_CHECKING, Any, TypeVar, Union, Optional, Dict, List

import ibis
import networkx as nx
from visitran import utils
from visitran.adapters.adapter import BaseAdapter
from visitran.adapters.seed import BaseSeed
from visitran.errors import (
    ModelNotFound,
    ModelExecutionFailed,
    ObjectForClassNotFoundError,
    RelativePathError,
    ModelImportError,
    VisitranBaseExceptions,
    RunSeedFailedException,
)
from visitran.events.functions import fire_event
from visitran.events.local_context import StateStore
from visitran.events.printer import (
    BASE_RESULT,
    SEED_RESULT,
    SNAPSHOT_RESULT,
    BaseResult,
    ExecStatus,
    SeedResult,
    SnapshotResult,
    parse_and_fire_reports,
    parse_and_fire_seed_report,
    parse_and_fire_snapshot_report,
)
from visitran.events.types import (
    ExecutingModelNode,
    ExecutingSnapshotNode,
    ExecutingTests,
    FoundBaseClass,
    FoundModelClass,
    FoundModels,
    FoundModelSubClass,
    FoundTestModels,
    ImportModelsFailed,
    ModelAlreadyProcessed,
    NodeExecutionError,
    ObjectForClassNotFound,
    ProcessingDAG,
    ProcessingModel,
    SeedExecutionCompleted,
    SeedExecutionError,
    SeedFilesFound,
    SkipExecution,
    SnapshotExecutionCompleted,
    SnapshotFilesFound,
    SortedDAGNodes,
    TestExecutionCompleted,
    TestExecutionFailed,
)
from visitran.materialization import Materialization
from visitran.singleton import Singleton
from visitran.templates.model import VisitranModel
from visitran.templates.snapshot import VisitranSnapshot

warnings.filterwarnings("ignore", message=".*?pkg_resources.*?")
from matplotlib import pyplot as plt  # noqa: E402

if TYPE_CHECKING:  # pragma: no cover
    from ibis.expr.types.relations import Table
    from networkx.classes.digraph import DiGraph

    from adapters.connection import BaseConnection
    from visitran.visitran_context import VisitranContext

    BASE_SQL = TypeVar("BASE_SQL", bound=BaseConnection)


class Visitran:
    """Singleton instance representing Visitran configuration and common
    functionality."""

    def __init__(self, context: Optional[VisitranContext]) -> None:
        self.context: Optional[VisitranContext] = context
        self.dag: DiGraph = nx.DiGraph()
        self.models: dict[str, VisitranModel] = {}
        self._sql_models: dict[str, dict[str, Any]] = {}
        self.sorted_dag_nodes: list[VisitranModel] = []

        self._dbobj: BaseAdapter = self.context.db_adapter
        self.dbtype: str = self.context.database_type

    @property
    def db_adapter(self) -> BaseAdapter:
        return self._dbobj

    @staticmethod
    def _get_object_for_class(model_objs: ValuesView[VisitranModel], cls: type) -> Union[VisitranModel, None]:
        """Returns a class object from a list of objects if it matches given
        class."""
        for model in model_objs:
            if (
                model.__class__ is cls
                or (model.__class__.__module__ == cls.__module__)
                and (model.__class__.__name__ == cls.__name__)
            ):
                return model

        return None

    def build_dag(self) -> None:
        """Constructs Directed Acyclic Graph.

        TBD save the graph in project dir
        """
        value_list = self.models.values()
        for model in value_list:
            dest_table = model.destination_table_name
            fire_event(ProcessingDAG(dest_table=dest_table))
            bases = model.__class__.__bases__
            fire_event(FoundBaseClass(bases=str(bases)))
            for base in bases:
                if base is VisitranModel:
                    continue
                base_obj = self._get_object_for_class(value_list, base)
                if not base_obj:
                    fire_event(ObjectForClassNotFound(base=str(base), values=str(value_list)))
                    raise ObjectForClassNotFoundError(base=str(base), values=str(value_list))
                self.dag.add_edge(f"{base}", f"{model.__class__}")

        # Add edges from project model graph (cross-model dependencies)
        self._add_project_model_graph_edges()

    def _build_model_name_to_class_strs_map(self) -> dict[str, list[str]]:
        """Build a mapping from model names to ALL class strings from that
        model file.

        Each model file can contain multiple classes (e.g., parent
        ephemeral + final table), so we need to map model names to ALL
        classes in that file.
        """
        model_name_to_classes: dict[str, list[str]] = {}

        for class_str, model_obj in self.models.items():
            # Get the module path to identify which model file this class belongs to
            # class_str is like: <class 'project.models.stg_order_summaries.StgOrderSummaries'>
            module = model_obj.__class__.__module__
            # Extract model name from module path (last part before class name)
            # e.g., 'project.models.stg_order_summaries' -> 'stg_order_summaries'
            if '.models.' in module:
                model_name = module.split('.models.')[-1]
            else:
                # Fallback: convert class name to snake_case
                class_name = model_obj.__class__.__name__
                model_name = ''.join(['_' + c.lower() if c.isupper() else c for c in class_name]).lstrip('_')

            if model_name not in model_name_to_classes:
                model_name_to_classes[model_name] = []
            model_name_to_classes[model_name].append(class_str)

        return model_name_to_classes

    def _add_project_model_graph_edges(self) -> None:
        """Add edges from the project model graph to the Visitran DAG.

        This ensures cross-model dependencies (model references) are
        respected in the execution order, not just class inheritance.

        Each model file can have multiple classes (parent ephemeral +
        final table). We add edges from ALL source classes to ALL target
        classes to ensure proper ordering.
        """
        # Check if context has project model graph access
        if not hasattr(self.context, 'get_project_model_graph_edges'):
            return

        try:
            project_graph_edges = self.context.get_project_model_graph_edges()
            if not project_graph_edges:
                return

            # Build mapping from model names to ALL class strings in that model
            model_name_to_classes = self._build_model_name_to_class_strs_map()

            for source_model, target_model in project_graph_edges:
                source_classes = model_name_to_classes.get(source_model, [])
                target_classes = model_name_to_classes.get(target_model, [])

                # Add edges from all source classes to all target classes
                # This ensures all classes in the target model wait for all source classes
                for source_class in source_classes:
                    for target_class in target_classes:
                        if source_class in self.dag.nodes and target_class in self.dag.nodes:
                            if not self.dag.has_edge(source_class, target_class):
                                self.dag.add_edge(source_class, target_class)
                                logging.info(f"Added cross-model edge: {source_class} -> {target_class}")
        except Exception as err:
            logging.warning(f"Failed to add project model graph edges: {err}")

    @staticmethod
    def get_inheritance_level(cls):
        """Return inheritance level from VisitranModel."""
        level = 0
        while cls is not VisitranModel and cls is not object:
            cls = cls.__base__
            level += 1
        return level

    def sort_dag(self) -> None:
        """Sorts the DAG graph list."""

        def sort_func(node_key: str):
            obj: VisitranModel = self.dag.nodes[node_key]["model_object"]
            cls = obj.__class__
            return (
                self.get_inheritance_level(cls),
                obj.destination_table_name or obj.source_table_name
            )

        self.sorted_dag_nodes = list(nx.lexicographical_topological_sort(self.dag, key=sort_func))
        fire_event(SortedDAGNodes(sorted_dag_nodes=str(self.sorted_dag_nodes)))

    def execute_graph(self) -> None:
        """Executes the sorted DAG elements one by one."""
        dag_nodes = self.sorted_dag_nodes
        sequence_number = 1
        while len(dag_nodes):
            node_name: VisitranModel = dag_nodes.pop(0)
            node = self.dag.nodes[node_name]["model_object"]
            is_executable = self.dag.nodes[node_name].get("executable", True)
            try:
                # Apply model_configs override from deployment configuration
                self._apply_model_config_override(node)

                node.materialize(
                    parent_class=node.__class__.__base__,
                    db_connection=self.db_adapter.db_connection,
                )

                # Skip execution for non-executable nodes (parents in selective execution)
                # They are materialized above for DAG dependency resolution but not executed
                if not is_executable:
                    continue

                fire_event(
                    ExecutingModelNode(
                        database=node.database,
                        database_type=node.dbtype,
                        destination_schema_name=node.destination_schema_name,
                        destination_table_exists=node.destination_table_exists,
                        destination_table_name=node.destination_table_name,
                        destination_table_obj=(
                            str(node.destination_table_obj) if hasattr(node, "destination_table_obj") else ""
                        ),
                        materialization=str(node.materialization),
                        select_statement=(str(node.select_statement) if hasattr(node, "select_statement") else ""),
                        source_schema_name=node.source_schema_name,
                        source_table_name=node.source_table_name,
                        source_table_obj=(str(node.source_table_obj) if hasattr(node, "source_table_obj") else ""),
                    )
                )
                self.db_adapter.db_connection.create_schema(node.destination_schema_name)  # create if not exists
                self.db_adapter.run_model(visitran_model=node)

                base_result = BaseResult(
                    node_name=str(node_name),
                    sequence_num=sequence_number,
                    ending_time=datetime.datetime.now(),
                    failures=False,
                    info_message=f"Running {node_name}",
                    status=str(ExecStatus.Success),
                    end_status=str(ExecStatus.OK),
                )
                sequence_number += 1
                BASE_RESULT.append(base_result)
            except VisitranBaseExceptions as visitran_err:
                raise visitran_err
            except Exception as err:
                dest_table = node.destination_table_name
                sch_name = node.destination_schema_name
                err_trace = repr(err)
                base_result = BaseResult(
                    node_name=str(node_name),
                    sequence_num=sequence_number,
                    ending_time=datetime.datetime.now(),
                    status=str(ExecStatus.Error),
                    end_status=str(ExecStatus.Fail),
                    info_message=f"Error occurred while running {node_name}",
                    failures=True,
                )
                sequence_number += 1
                BASE_RESULT.append(base_result)
                parse_and_fire_reports()
                fire_event(NodeExecutionError(dest_table=dest_table, err=err_trace))
                raise ModelExecutionFailed(
                    table_name=dest_table,
                    schema_name=sch_name,
                    model_name=str(node),
                    error_message=err_trace,
                )

    def _apply_model_config_override(self, node: VisitranModel) -> None:
        """Apply deployment-time model configuration overrides.

        This allows the deployment page to override materialization settings
        (e.g., switch from VIEW to INCREMENTAL) without modifying the model code.

        Args:
            node: The VisitranModel instance to configure
        """
        # Get model_configs from context
        model_configs = getattr(self.context, 'model_configs', {})
        if not model_configs:
            return

        # Extract model name from the class module
        # e.g., 'project.models.stg_order_summaries.StgOrderSummaries' -> 'stg_order_summaries'
        module = node.__class__.__module__
        if '.models.' in module:
            model_name = module.split('.models.')[-1]
        else:
            # Fallback: convert class name to snake_case
            class_name = node.__class__.__name__
            model_name = ''.join(['_' + c.lower() if c.isupper() else c for c in class_name]).lstrip('_')

        # Check if there's config for this model
        config = model_configs.get(model_name)
        if not config:
            return

        # Override materialization if specified
        materialization_str = config.get('materialization')
        if materialization_str:
            materialization_map = {
                'TABLE': Materialization.TABLE,
                'VIEW': Materialization.VIEW,
                'INCREMENTAL': Materialization.INCREMENTAL,
                'EPHEMERAL': Materialization.EPHEMERAL,
            }
            if materialization_str.upper() in materialization_map:
                node.materialization = materialization_map[materialization_str.upper()]
                logging.info(f"Model {model_name}: Overriding materialization to {materialization_str}")

        # Apply incremental configuration if switching to INCREMENTAL
        incremental_config = config.get('incremental_config', {})
        if incremental_config:
            # Override primary_key
            if 'primary_key' in incremental_config:
                node.primary_key = incremental_config['primary_key']
                logging.info(f"Model {model_name}: Setting primary_key to {node.primary_key}")

            # Override delta_strategy
            if 'delta_strategy' in incremental_config:
                delta_cfg = incremental_config['delta_strategy']
                node.delta_strategy = {
                    'type': delta_cfg.get('type', ''),
                    'column': delta_cfg.get('column', ''),
                    'key_columns': delta_cfg.get('key_columns', []),
                    'custom_logic': delta_cfg.get('custom_logic'),
                }
                logging.info(f"Model {model_name}: Setting delta_strategy to {node.delta_strategy}")

    def get_model(
        self,
        imported_file: ModuleType,
        base_cls: Union[type[VisitranModel], type[VisitranSnapshot]],
        base_cls_str: str,
    ) -> set[Any]:
        """Gets the Model classes inherited from VisitranSnapshot ||
        VisitranModel from the imported file."""
        cls_set = set()
        cls_strs = dir(imported_file)
        for cls_str in cls_strs:
            if cls_str == str(base_cls_str):
                continue
            cls = getattr(imported_file, cls_str, None)
            if not isclass(cls):
                continue
            fire_event(FoundModelClass(cls_str=cls_str))
            if issubclass(cls, base_cls):
                fire_event(FoundModelSubClass(cls_str=cls_str))
                cls_set.add(cls)
        return cls_set

    def search_n_run_models(self, model_name: str = None, model_names: list = None) -> dict[str, VisitranModel]:
        """Imports all the model classes from given model path, builds a dag,
        sorts and executes dag nodes one by one.

        Selective execution modes:
        - model_names (list): Multi-model execution (AI Apply) - executes all listed models + their children
        - model_name (str): Single model execution (right-click Run) - executes model + its children
        - Neither: Execute ALL models

        In selective mode:
        - models_to_execute: triggered model(s) + all downstream dependents (children)
        - models_to_import: models_to_execute + upstream dependencies (parents for DAG building)
        Disjoint graphs are completely excluded.
        """
        models_to_execute = []
        model_files: list[dict[str, Any]] = self.context.get_model_files()

        if model_names:
            # Multi-model execution (AI Apply)
            subgraph = self.context.get_multi_model_execution_subgraph(model_names)
            models_to_execute = subgraph["models_to_execute"]
            models_to_import = subgraph["models_to_import"]
            model_files = [mf for mf in model_files if mf["model_name"] in models_to_import]
        elif model_name:
            # Single model execution (right-click Run)
            subgraph = self.context.get_model_execution_subgraph(model_name)
            models_to_execute = subgraph["models_to_execute"]
            models_to_import = subgraph["models_to_import"]
            model_files = [mf for mf in model_files if mf["model_name"] in models_to_import]

        # Base_result
        class_cache = set()
        BASE_RESULT.clear()
        for model_file in model_files:
            file_name = model_file["model_name"]
            file_path = model_file["model_path"]
            fire_event(FoundModels(mf=file_name))

            # catch this properly
            imported_file = self.import_process_file(file_name=file_path)

            cls_set = self.get_model(
                imported_file=imported_file,
                base_cls=VisitranModel,
                base_cls_str="VisitranModel",
            )

            for cls in cls_set:
                fire_event(ProcessingModel(cls=str(cls)))
                # process each model only once
                # they might be imported in several places!

                class_str = str(cls)
                class_name = cls.__name__
                if class_str in class_cache:
                    fire_event(ModelAlreadyProcessed(cls=class_str))
                    continue

                obj: VisitranModel = cls()
                obj.visitran = self

                self.models[class_str] = obj
                class_cache.add(class_str)
                if file_name.replace("_", " ").title().replace(" ", "") == class_name:
                    self._sql_models[file_path] = {"model": obj}
                if models_to_execute:
                    # Only execute descendants (children), parents are materialized but not executed
                    is_exec = file_name in models_to_execute
                    self.dag.add_node(
                        node_for_adding=f"{class_str}",
                        model_object=obj,
                        executable=is_exec,
                    )
                else:
                    self.dag.add_node(f"{class_str}", model_object=obj, executable=True)

        self.build_sort_execute()

        for model_path, models in self._sql_models.items():
            try:
                visitran_model: VisitranModel = models["model"]
                ibis_table_obj = visitran_model.select_statement
                if ibis_table_obj is None:
                    logging.warning(f"Skipping SQL save for {model_path}: select_statement is None")
                    continue
                sql_query = ibis.to_sql(ibis_table_obj).__str__()
                visitran_model.save_sql_query(sql_query=sql_query)
            except Exception as _err:
                logging.warning(f"Failed to save SQL for {model_path}: {_err}")
        return self.models

    @property
    def sql_models(self) -> dict[str, dict[str, Any]]:
        return self._sql_models

    def draw_dag(self) -> None:
        if not self.context.is_api_call:
            """Matplotlib internally uses tkinter to make plots using GUI.

            This causes an Runtime error when
            calling this from django.
            So suppressing generation of dag.png when calling this from django.
            """
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                plt.tight_layout()
                pos = nx.planar_layout(self.dag)
                nx.draw_networkx(self.dag, pos, arrows=True, node_size=100, font_size=5)
                plt.savefig("dag.svg", format="svg")
                plt.clf()

    def build_sort_execute(self) -> dict[str, VisitranModel]:
        """Builds the DAG, sorts and executes the DAG."""
        self.build_dag()
        self.dag = utils.select_dag_based_on_include_exclude(
            self.dag, self.context.get_includes, self.context.get_excludes
        )
        # draw dag here
        self.draw_dag()
        self.sort_dag()
        self.execute_graph()
        parse_and_fire_reports()
        return self.models

    def get_table(self, schema_name: str, table_name: str) -> Table:
        """Returns Table object for given table name and schema name."""
        table_obj: Table = self._dbobj.db_connection.get_table_obj(schema_name, table_name)
        return table_obj

    def get_visitran_test_classes(self, imported_file: ModuleType) -> list[Singleton]:
        """Gets the test classes from the given imported file."""
        model_classes = self.get_model(
            imported_file=imported_file,
            base_cls=VisitranModel,
            base_cls_str="VisitranModel",
        )
        model_test_classes = [cl for cl in model_classes if cl.__name__.lower().startswith("test")]
        return model_test_classes

    def search_n_run_tests(self) -> None:
        """Imports all test files from given test path and runs test one by
        one."""
        test_files: list[str] = self.context.get_model_files()
        BASE_RESULT.clear()
        db_connection: BaseConnection = self.db_adapter.db_connection

        for tf in test_files:
            if self._should_skip(tf):
                SkipExecution(filename=tf)
                continue
            fire_event(FoundTestModels(test_path=tf))
            file_name, _ = path.splitext(path.basename(tf))
            try:
                imported_file = self.import_process_file(file_name=file_name)
            except ModelNotFound as err:
                base_result = BaseResult(
                    node_name=str(tf),
                    sequence_num=test_files.index(tf),
                    failures=True,
                    ending_time=datetime.datetime.now(),
                    status=str(ExecStatus.Error),
                    info_message=f"Error occured in {tf} execution",
                    end_status=str(ExecStatus.Fail),
                )
                BASE_RESULT.append(base_result)
                parse_and_fire_reports()
                fire_event(ImportModelsFailed(file_name=file_name, err=f"{err}"))
                continue
            model_test_classes = self.get_visitran_test_classes(imported_file)
            for tst_cls in model_test_classes:
                # process each model only once
                # they might be imported in several places!
                conn_str = db_connection.connection_string
                tst_obj = tst_cls(connection_string=conn_str)  # type: ignore[arg-type]
                destination_obj = db_connection.get_table_obj(
                    schema_name=tst_obj.destination_schema_name,
                    table_name=tst_obj.destination_table_name,
                )
                tst_obj.destination_table_obj = destination_obj
                tst_obj.dbtype = self.dbtype
                test_methods = utils.get_test_methods_from_test_obj(tst_obj)
                self._run_tests(tf, tst_cls, tst_obj, test_methods)

            fire_event(TestExecutionCompleted())

    def _run_tests(self, tf: str, tst_cls: Any, tst_obj: Any, test_methods: list[str]) -> None:
        sequence_num = 1
        for method in test_methods:
            test_func = utils.get_method_from_object(obj=tst_obj, method=method)
            try:
                fire_event(
                    ExecutingTests(
                        test_name=test_func.__name__,
                        test_path=tf,
                        database=tst_obj.database,
                        database_type=tst_obj.dbtype,
                        connection_string=tst_obj.connection_string,
                        destination_schema_name=tst_obj.destination_schema_name,
                        destination_table_name=tst_obj.destination_table_name,
                        materialization=str(tst_obj.materialization),
                        source_schema_name=tst_obj.source_schema_name,
                        source_table_name=tst_obj.source_table_name,
                    )
                )
                test_func()
            except AssertionError as err:
                print(f"Test assertion error: {repr(err)} in {test_func.__name__}")
                fire_event(
                    TestExecutionFailed(
                        err=repr(err),
                        test_path=tf,
                        class_name=tst_cls.__name__,
                        test_func=test_func.__name__,
                    )
                )
                base_result = BaseResult(
                    node_name=str(test_func),
                    failures=True,
                    info_message=f"Test assertion error: \
                    {repr(err)} in {test_func.__name__}",
                    ending_time=datetime.datetime.now(),
                    status=str(ExecStatus.Error),
                    end_status=str(ExecStatus.Fail),
                    sequence_num=sequence_num,
                )
                BASE_RESULT.append(base_result)
                parse_and_fire_reports()
                raise AssertionError(f"{repr(err)} in {test_func.__name__}")

            base_result = BaseResult(
                node_name=str(tst_cls.__name__),
                ending_time=datetime.datetime.now(),
                failures=False,
                info_message=f"Running {tf}",
                status=str(ExecStatus.Success),
                end_status=str(ExecStatus.OK),
                sequence_num=sequence_num,
            )
            BASE_RESULT.append(base_result)
            parse_and_fire_reports()
            sequence_num += 1

    _SAFE_MODULE_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.]*$")

    @staticmethod
    def import_process_file(file_name: str) -> ModuleType:
        if not Visitran._SAFE_MODULE_PATTERN.match(file_name) or ".." in file_name:
            raise ModelImportError(
                model_name=file_name,
                error_message="Invalid module path: contains disallowed characters",
            )
        try:
            if "." in file_name and len(file_name.split(".")) > 0:
                mod_path_begin = file_name.split(".")[0]
                module_name = [k for k in sys.modules.keys() if k.startswith(mod_path_begin)]
                for mod in module_name:
                    del sys.modules[mod]

            imported_file: ModuleType = importlib.import_module(file_name)
            methods_classes = dir(imported_file)
            for cls in methods_classes:
                if not (
                    cls.startswith("__")
                    or cls
                    in (
                        "FormulaSQL",
                        "VisitranModel",
                        "VisitranSnapshot",
                        "Table",
                        "Materialization",
                        "ibis",
                    )
                ):
                    delattr(imported_file, cls)

            importlib.reload(imported_file)
            # imported_file.check_scope()
            return imported_file
        except ModuleNotFoundError as err:
            fire_event(ImportModelsFailed(file_name=file_name, err=f"{err}"))
            raise ModelImportError(model_name=file_name, error_message=str(err))
        except TypeError as err:
            if ".p.y" in str(err):
                raise RelativePathError(file_name=file_name)
            raise err
        except Exception as err:
            raise ModelImportError(model_name=file_name, error_message=str(err))

    def _should_skip(self, file_name: str) -> bool:
        """Logic to skip files based on include and exclude.

        assumes the item won't be present in both list, presence in both
        include exclude is handled in context class it self.
        """
        include_empty = len(self.context.get_includes) > 0
        exclude_empty = len(self.context.get_excludes) > 0

        include_match = any(substring in file_name for substring in self.context.get_includes)
        exclude_match = any(substring in file_name for substring in self.context.get_excludes)

        should_skip = exclude_empty and exclude_match or include_empty and not include_match
        return should_skip

    def run_seeds(self, seed_details=None) -> list:
        """Imports all the seed file from the projects."""
        if seed_details["runAll"]:
            seed_files: list[dict[str, Any]] = self.context.get_seed_files()
        else:
            seed_files: list[dict[str, Any]] = self.context.get_seed_file(csv_file_name=seed_details["fileName"])
        session_id = StateStore.get("log_events_id")
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # map() ensures all threads complete before returning results
            results = list(
                executor.map(
                    lambda file: self.validate_and_run_seed(file, session_id),
                    seed_files,
                )
            )

        fire_event(SeedExecutionCompleted())
        for result in results:
            self.context.update_seed_run_status(**result)
        return results

    def validate_and_run_seed(self, seed_file, session_id: str) -> dict[str, str]:
        StateStore.set("log_events_id", session_id)
        file_name = seed_file["file_name"]
        file_path = seed_file["file_path"]
        try:
            if self._should_skip(file_name):
                SkipExecution(filename=file_name)
            else:
                schema = self.context.schema_name
                fire_event(SeedFilesFound(sf_path=file_name, schema_name=schema, database_type=self.dbtype))
                seed_result = SeedResult(
                    schema_name=schema,
                    seed_path=file_name,
                    status=str(ExecStatus.START),
                )
                SEED_RESULT.append(seed_result)
                seed_obj: BaseSeed = self.db_adapter.run_seeds(schema=schema, abs_path=file_path)
                seed_result = SeedResult(
                    schema_name=schema,
                    seed_path=file_name,
                    status=str(ExecStatus.COMPLETED),
                )
                SEED_RESULT.append(seed_result)
                parse_and_fire_seed_report()
                return {
                    "file_name": file_name,
                    "status": "Success",
                    "destination_table": seed_obj.destination_table_name,
                    "table_schema": schema,
                }
        except Exception as err:
            # Catches all kind of errors and raises with custom exceptions for seeds
            fire_event(SeedExecutionError(file_name, str(err)))
            logging.exception(f"validate and run seed failed with exception {err}")
            raise RunSeedFailedException(file_name=file_name, error_message=str(err))

    def run_snapshot(self) -> None:
        """Imports all snapshots file from the projects."""

        snapshot_files: list[str] = self.context.get_snapshot_files()

        for spf in snapshot_files:
            if self._should_skip(spf):
                SkipExecution(filename=spf)
                continue
            fire_event(SnapshotFilesFound(snp_path=spf))
            file_name, _ = path.splitext(path.basename(spf))

            imported_file = self.import_process_file(file_name=file_name)
            cls_set = self.get_model(
                imported_file=imported_file,
                base_cls=VisitranSnapshot,
                base_cls_str="VisitranSnapshot",
            )
            if cls_set:
                for cls in cls_set:
                    obj: VisitranSnapshot = cls()
                    fire_event(
                        ExecutingSnapshotNode(
                            database=obj.database,
                            database_type="",
                            source_table_name=obj.source_table_name,
                            source_schema_name=obj.source_schema_name,
                            snapshot_table_name=obj.snapshot_table_name,
                            snapshot_schema_name=obj.snapshot_schema_name,
                            unique_key=obj.unique_key,
                            strategy=obj.unique_key,
                            updated_at=obj.unique_key,
                            check_cols=obj.check_cols,
                            invalidate_hard_deletes=obj.invalidate_hard_deletes,
                        )
                    )
                    snapshot_result = SnapshotResult(
                        source_table=obj.source_table_name,
                        unique_key=obj.unique_key,
                        status=str(ExecStatus.START),
                    )
                    SNAPSHOT_RESULT.append(snapshot_result)
                    self.db_adapter.run_scd(visitran_snapshot=obj)
                    snapshot_result = SnapshotResult(
                        source_table=obj.source_table_name,
                        unique_key=obj.unique_key,
                        status=str(ExecStatus.COMPLETED),
                    )
                    SNAPSHOT_RESULT.append(snapshot_result)
                    parse_and_fire_snapshot_report()
            fire_event(SnapshotExecutionCompleted())
