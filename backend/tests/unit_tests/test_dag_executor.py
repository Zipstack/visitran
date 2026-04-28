"""Unit tests for DAG Executor."""

from unittest.mock import MagicMock, patch, PropertyMock

import networkx as nx
import pytest

from backend.application.config_parser.dag_executor import (
    ExecutionStatus,
    ModelExecutionResult,
    DAGExecutionResult,
    DAGExecutionError,
    DAGExecutor,
    execute_dag,
)
from backend.application.config_parser.feature_flags import (
    ExecutionMode,
    FeatureFlags,
)
from backend.application.config_parser.materialization_handler import (
    MaterializationResult,
    MaterializationConfig,
    MaterializationMode,
)


class TestExecutionStatus:
    """Tests for ExecutionStatus enum."""

    def test_values(self):
        """Test enum values."""
        assert ExecutionStatus.PENDING.value == "pending"
        assert ExecutionStatus.RUNNING.value == "running"
        assert ExecutionStatus.COMPLETED.value == "completed"
        assert ExecutionStatus.FAILED.value == "failed"
        assert ExecutionStatus.SKIPPED.value == "skipped"


class TestModelExecutionResult:
    """Tests for ModelExecutionResult dataclass."""

    def test_creation(self):
        """Test creating result."""
        result = ModelExecutionResult(
            schema="dev",
            model="my_model",
            status=ExecutionStatus.COMPLETED,
            execution_time_ms=100.5,
        )

        assert result.schema == "dev"
        assert result.model == "my_model"
        assert result.status == ExecutionStatus.COMPLETED
        assert result.execution_time_ms == 100.5

    def test_qualified_name(self):
        """Test qualified_name property."""
        result = ModelExecutionResult(
            schema="dev",
            model="my_model",
            status=ExecutionStatus.COMPLETED,
        )

        assert result.qualified_name == "dev.my_model"

    def test_success_property(self):
        """Test success property."""
        completed = ModelExecutionResult(
            schema="dev",
            model="m1",
            status=ExecutionStatus.COMPLETED,
        )
        failed = ModelExecutionResult(
            schema="dev",
            model="m2",
            status=ExecutionStatus.FAILED,
        )

        assert completed.success is True
        assert failed.success is False

    def test_to_dict(self):
        """Test conversion to dictionary."""
        result = ModelExecutionResult(
            schema="dev",
            model="my_model",
            status=ExecutionStatus.COMPLETED,
            execution_time_ms=50.0,
        )

        d = result.to_dict()

        assert d["schema"] == "dev"
        assert d["model"] == "my_model"
        assert d["qualified_name"] == "dev.my_model"
        assert d["status"] == "completed"
        assert d["success"] is True

    def test_to_dict_with_error(self):
        """Test conversion with error info."""
        result = ModelExecutionResult(
            schema="dev",
            model="my_model",
            status=ExecutionStatus.FAILED,
            error="Something went wrong",
            error_line=10,
            error_column=5,
            error_file="/path/to/file.yaml",
        )

        d = result.to_dict()

        assert "error" in d
        assert d["error"]["message"] == "Something went wrong"
        assert d["error"]["line"] == 10
        assert d["error"]["column"] == 5
        assert d["error"]["file"] == "/path/to/file.yaml"


class TestDAGExecutionResult:
    """Tests for DAGExecutionResult dataclass."""

    def test_creation(self):
        """Test creating result."""
        result = DAGExecutionResult(
            success=True,
            models_executed=5,
            models_failed=0,
            total_time_ms=500.0,
        )

        assert result.success is True
        assert result.models_executed == 5
        assert result.models_failed == 0
        assert result.total_time_ms == 500.0

    def test_to_dict(self):
        """Test conversion to dictionary."""
        result = DAGExecutionResult(
            success=True,
            models_executed=3,
            models_failed=1,
            models_skipped=2,
            total_time_ms=250.0,
        )

        d = result.to_dict()

        assert d["success"] is True
        assert d["models_executed"] == 3
        assert d["models_failed"] == 1
        assert d["models_skipped"] == 2
        assert d["total_time_ms"] == 250.0
        assert "started_at" in d


class TestDAGExecutionError:
    """Tests for DAGExecutionError exception."""

    def test_basic_error(self):
        """Test basic error creation."""
        error = DAGExecutionError("Something went wrong")

        assert "Something went wrong" in str(error)
        assert error.message == "Something went wrong"

    def test_error_with_model(self):
        """Test error with model name."""
        error = DAGExecutionError(
            "Compilation failed",
            model_name="dev.my_model",
        )

        assert "dev.my_model" in str(error)

    def test_error_with_location(self):
        """Test error with source location."""
        error = DAGExecutionError(
            "Invalid syntax",
            model_name="dev.my_model",
            file_path="/path/to/model.yaml",
            line_number=15,
            column_number=8,
        )

        error_str = str(error)
        assert "/path/to/model.yaml:15:8" in error_str

    def test_to_transformation_error(self):
        """Test conversion to TransformationError."""
        error = DAGExecutionError(
            "Test error",
            model_name="dev.my_model",
            line_number=10,
        )

        trans_error = error.to_transformation_error()

        assert trans_error.model_name == "dev.my_model"
        assert trans_error.line_number == 10


class TestDAGExecutor:
    """Tests for DAGExecutor."""

    def setup_method(self):
        """Reset feature flags before each test."""
        FeatureFlags.reset()

    def _create_mock_config(self, schema, model):
        """Create a mock ConfigParser."""
        config = MagicMock()
        config.destination_schema_name = schema
        config.destination_table_name = model
        config.source_schema_name = "raw"
        config.source_table_name = "source"
        config.materialization = "TABLE"
        return config

    def _create_simple_dag(self):
        """Create a simple DAG for testing."""
        dag = nx.DiGraph()

        # Create nodes with mock configs
        config_a = self._create_mock_config("dev", "model_a")
        config_b = self._create_mock_config("dev", "model_b")
        config_c = self._create_mock_config("dev", "model_c")

        dag.add_node(("dev", "model_a"), config=config_a)
        dag.add_node(("dev", "model_b"), config=config_b)
        dag.add_node(("dev", "model_c"), config=config_c)

        # Edge direction: dependent -> dependency
        # model_b depends on model_a: edge from model_b to model_a
        # model_c depends on model_b: edge from model_c to model_b
        dag.add_edge(("dev", "model_b"), ("dev", "model_a"))
        dag.add_edge(("dev", "model_c"), ("dev", "model_b"))

        return dag

    def test_init(self):
        """Test executor initialization."""
        dag = self._create_simple_dag()
        executor = DAGExecutor(dag)

        assert executor.dag is dag
        assert executor.registry is not None

    def test_execute_disabled(self):
        """Test execution when direct execution is disabled."""
        FeatureFlags.set_state(execution_mode=ExecutionMode.LEGACY)

        dag = self._create_simple_dag()
        executor = DAGExecutor(dag)

        result = executor.execute()

        assert result.success is True
        assert result.models_executed == 0

    def test_execute_empty_dag(self):
        """Test execution with empty DAG."""
        FeatureFlags.set_state(execution_mode=ExecutionMode.DIRECT)

        dag = nx.DiGraph()
        executor = DAGExecutor(dag)

        result = executor.execute()

        assert result.success is True
        assert result.models_executed == 0

    @patch('backend.application.config_parser.dag_executor.IbisBuilder')
    @patch('backend.application.config_parser.dag_executor.MaterializationHandler')
    def test_execute_single_model(self, mock_mat_handler_cls, mock_ibis_builder_cls):
        """Test execution with single model."""
        FeatureFlags.set_state(execution_mode=ExecutionMode.DIRECT)

        # Setup mocks
        mock_builder = MagicMock()
        mock_compilation = MagicMock()
        mock_compilation.sql = "SELECT * FROM raw.source"
        mock_builder.compile_transformation.return_value = mock_compilation
        mock_ibis_builder_cls.return_value = mock_builder

        mock_mat_handler = MagicMock()
        mock_mat_result = MaterializationResult(
            config=MaterializationConfig(
                schema_name="dev",
                table_name="model_a",
                sql="SELECT 1",
            ),
            success=True,
        )
        mock_mat_handler.execute.return_value = mock_mat_result
        mock_mat_handler_cls.return_value = mock_mat_handler

        # Create single-node DAG
        dag = nx.DiGraph()
        config = self._create_mock_config("dev", "model_a")
        dag.add_node(("dev", "model_a"), config=config)

        executor = DAGExecutor(dag)
        result = executor.execute()

        assert result.success is True
        assert result.models_executed == 1
        assert len(result.model_results) == 1

    @patch('backend.application.config_parser.dag_executor.IbisBuilder')
    @patch('backend.application.config_parser.dag_executor.MaterializationHandler')
    def test_execute_respects_topological_order(self, mock_mat_handler_cls, mock_ibis_builder_cls):
        """Test that models execute in topological order."""
        FeatureFlags.set_state(execution_mode=ExecutionMode.DIRECT)

        # Setup mocks
        mock_builder = MagicMock()
        mock_compilation = MagicMock()
        mock_compilation.sql = "SELECT 1"
        mock_builder.compile_transformation.return_value = mock_compilation
        mock_ibis_builder_cls.return_value = mock_builder

        mock_mat_handler = MagicMock()
        mock_mat_handler.execute.return_value = MaterializationResult(
            config=MaterializationConfig(
                schema_name="dev",
                table_name="test",
                sql="SELECT 1",
            ),
            success=True,
        )
        mock_mat_handler_cls.return_value = mock_mat_handler

        dag = self._create_simple_dag()
        executor = DAGExecutor(dag)

        execution_order = []

        def track_start(step):
            execution_order.append(step.model)

        executor.on_model_start = track_start
        result = executor.execute()

        assert result.success is True
        # model_a should execute before model_b before model_c
        assert execution_order.index("model_a") < execution_order.index("model_b")
        assert execution_order.index("model_b") < execution_order.index("model_c")

    @patch('backend.application.config_parser.dag_executor.IbisBuilder')
    @patch('backend.application.config_parser.dag_executor.MaterializationHandler')
    def test_execute_fail_fast(self, mock_mat_handler_cls, mock_ibis_builder_cls):
        """Test fail_fast stops execution on first failure."""
        FeatureFlags.set_state(execution_mode=ExecutionMode.DIRECT)

        # Setup mocks - first model fails
        mock_builder = MagicMock()
        mock_builder.compile_transformation.side_effect = Exception("Compilation error")
        mock_ibis_builder_cls.return_value = mock_builder

        mock_mat_handler = MagicMock()
        mock_mat_handler_cls.return_value = mock_mat_handler

        dag = self._create_simple_dag()
        executor = DAGExecutor(dag, fail_fast=True)

        result = executor.execute()

        assert result.success is False
        assert result.models_failed == 1
        assert result.models_skipped == 2  # Remaining models skipped

    @patch('backend.application.config_parser.dag_executor.IbisBuilder')
    @patch('backend.application.config_parser.dag_executor.MaterializationHandler')
    def test_execute_continue_on_failure(self, mock_mat_handler_cls, mock_ibis_builder_cls):
        """Test execution continues when fail_fast=False."""
        FeatureFlags.set_state(execution_mode=ExecutionMode.DIRECT)

        # Setup mocks
        call_count = [0]

        def compile_side_effect(sql, line_number=None):
            call_count[0] += 1
            # Fail for first model
            if call_count[0] == 1:
                raise Exception("Compilation error")
            result = MagicMock()
            result.sql = "SELECT 1"
            return result

        mock_builder = MagicMock()
        mock_builder.compile_transformation.side_effect = compile_side_effect
        mock_ibis_builder_cls.return_value = mock_builder

        mock_mat_handler = MagicMock()
        mock_mat_handler.execute.return_value = MaterializationResult(
            config=MaterializationConfig(
                schema_name="dev",
                table_name="test",
                sql="SELECT 1",
            ),
            success=True,
        )
        mock_mat_handler_cls.return_value = mock_mat_handler

        dag = self._create_simple_dag()
        executor = DAGExecutor(dag, fail_fast=False)

        result = executor.execute()

        # Even with fail_fast=False, downstream models are skipped
        # because their dependencies failed
        assert result.success is False
        assert result.models_failed >= 1

    def test_get_model_status_pending(self):
        """Test get_model_status returns PENDING for unprocessed model."""
        dag = self._create_simple_dag()
        executor = DAGExecutor(dag)

        status = executor.get_model_status("dev", "model_a")

        assert status == ExecutionStatus.PENDING

    @patch('backend.application.config_parser.dag_executor.IbisBuilder')
    @patch('backend.application.config_parser.dag_executor.MaterializationHandler')
    def test_get_model_result(self, mock_mat_handler_cls, mock_ibis_builder_cls):
        """Test get_model_result after execution."""
        FeatureFlags.set_state(execution_mode=ExecutionMode.DIRECT)

        # Setup mocks
        mock_builder = MagicMock()
        mock_compilation = MagicMock()
        mock_compilation.sql = "SELECT 1"
        mock_builder.compile_transformation.return_value = mock_compilation
        mock_ibis_builder_cls.return_value = mock_builder

        mock_mat_handler = MagicMock()
        mock_mat_handler.execute.return_value = MaterializationResult(
            config=MaterializationConfig(
                schema_name="dev",
                table_name="model_a",
                sql="SELECT 1",
            ),
            success=True,
        )
        mock_mat_handler_cls.return_value = mock_mat_handler

        dag = nx.DiGraph()
        config = self._create_mock_config("dev", "model_a")
        dag.add_node(("dev", "model_a"), config=config)

        executor = DAGExecutor(dag)
        executor.execute()

        result = executor.get_model_result("dev", "model_a")

        assert result is not None
        assert result.status == ExecutionStatus.COMPLETED

    def test_hooks_are_called(self):
        """Test that execution hooks are called."""
        FeatureFlags.set_state(execution_mode=ExecutionMode.DIRECT)

        dag = nx.DiGraph()  # Empty DAG

        executor = DAGExecutor(dag)

        execution_started = []
        execution_completed = []

        executor.on_execution_start = lambda plan: execution_started.append(True)
        executor.on_execution_complete = lambda result: execution_completed.append(True)

        executor.execute()

        # Start and complete hooks should be called even for empty DAG
        assert len(execution_started) == 1
        assert len(execution_completed) == 1


class TestDAGExecutorParallelValidation:
    """Tests for parallel validation mode."""

    def setup_method(self):
        """Reset feature flags before each test."""
        FeatureFlags.reset()

    def _create_mock_config(self, schema, model):
        """Create a mock ConfigParser."""
        config = MagicMock()
        config.destination_schema_name = schema
        config.destination_table_name = model
        config.source_schema_name = "raw"
        config.source_table_name = "source"
        config.materialization = "TABLE"
        return config

    @patch('backend.application.config_parser.dag_executor.validate_sql_equivalence')
    @patch('backend.application.config_parser.dag_executor.IbisBuilder')
    @patch('backend.application.config_parser.dag_executor.MaterializationHandler')
    def test_parallel_mode_validates_sql(
        self,
        mock_mat_handler_cls,
        mock_ibis_builder_cls,
        mock_validate,
    ):
        """Test that parallel mode performs SQL validation."""
        FeatureFlags.set_state(execution_mode=ExecutionMode.PARALLEL)

        # Setup mocks
        mock_builder = MagicMock()
        mock_compilation = MagicMock()
        mock_compilation.sql = "SELECT * FROM raw.source"
        mock_builder.compile_transformation.return_value = mock_compilation
        mock_ibis_builder_cls.return_value = mock_builder

        mock_mat_handler = MagicMock()
        mock_mat_handler.execute.return_value = MaterializationResult(
            config=MaterializationConfig(
                schema_name="dev",
                table_name="model_a",
                sql="SELECT 1",
            ),
            success=True,
        )
        mock_mat_handler_cls.return_value = mock_mat_handler

        mock_validation_result = MagicMock()
        mock_validation_result.match_status = True
        mock_validate.return_value = mock_validation_result

        # Create single-node DAG with legacy SQL available
        dag = nx.DiGraph()
        config = self._create_mock_config("dev", "model_a")
        config.get_legacy_sql.return_value = "SELECT * FROM raw.source"
        dag.add_node(("dev", "model_a"), config=config)

        executor = DAGExecutor(dag)
        result = executor.execute()

        assert result.success is True
        # Validation should have been called
        mock_validate.assert_called()


class TestExecuteDagFunction:
    """Tests for execute_dag convenience function."""

    def setup_method(self):
        """Reset feature flags before each test."""
        FeatureFlags.reset()

    def test_execute_dag_empty(self):
        """Test execute_dag with empty DAG."""
        FeatureFlags.set_state(execution_mode=ExecutionMode.DIRECT)

        dag = nx.DiGraph()
        result = execute_dag(dag)

        assert result.success is True
        assert result.models_executed == 0

    def test_execute_dag_disabled(self):
        """Test execute_dag when direct execution is disabled."""
        FeatureFlags.set_state(execution_mode=ExecutionMode.LEGACY)

        dag = nx.DiGraph()
        dag.add_node(("dev", "model_a"))

        result = execute_dag(dag)

        assert result.success is True
        assert result.models_executed == 0


class TestDAGExecutorDependencyChecking:
    """Tests for dependency checking in DAGExecutor."""

    def setup_method(self):
        """Reset feature flags before each test."""
        FeatureFlags.reset()

    def _create_mock_config(self, schema, model):
        """Create a mock ConfigParser."""
        config = MagicMock()
        config.destination_schema_name = schema
        config.destination_table_name = model
        config.source_schema_name = "raw"
        config.source_table_name = "source"
        config.materialization = "TABLE"
        return config

    @patch('backend.application.config_parser.dag_executor.IbisBuilder')
    @patch('backend.application.config_parser.dag_executor.MaterializationHandler')
    def test_skips_model_with_failed_dependency(
        self,
        mock_mat_handler_cls,
        mock_ibis_builder_cls,
    ):
        """Test that models are skipped when dependencies fail."""
        FeatureFlags.set_state(execution_mode=ExecutionMode.DIRECT)

        # Setup mocks
        call_count = [0]

        def compile_side_effect(sql, line_number=None):
            call_count[0] += 1
            if call_count[0] == 1:
                # First model fails
                raise Exception("Compilation error")
            result = MagicMock()
            result.sql = sql
            return result

        mock_builder = MagicMock()
        mock_builder.compile_transformation.side_effect = compile_side_effect
        mock_ibis_builder_cls.return_value = mock_builder

        mock_mat_handler = MagicMock()
        mock_mat_handler.execute.return_value = MaterializationResult(
            config=MaterializationConfig(
                schema_name="dev",
                table_name="test",
                sql="SELECT 1",
            ),
            success=True,
        )
        mock_mat_handler_cls.return_value = mock_mat_handler

        # Create DAG: model_b depends on model_a
        # Edge direction: dependent -> dependency
        dag = nx.DiGraph()
        config_a = self._create_mock_config("dev", "model_a")
        config_b = self._create_mock_config("dev", "model_b")
        dag.add_node(("dev", "model_a"), config=config_a)
        dag.add_node(("dev", "model_b"), config=config_b)
        dag.add_edge(("dev", "model_b"), ("dev", "model_a"))

        executor = DAGExecutor(dag, fail_fast=False)
        result = executor.execute()

        assert result.models_failed == 1
        # model_b should be skipped because model_a failed
        model_b_result = executor.get_model_result("dev", "model_b")
        assert model_b_result is not None
        assert model_b_result.status == ExecutionStatus.SKIPPED


class TestDAGExecutorMaterialization:
    """Tests for materialization handling in DAGExecutor."""

    def setup_method(self):
        """Reset feature flags before each test."""
        FeatureFlags.reset()

    def _create_mock_config(self, schema, model, materialization="TABLE"):
        """Create a mock ConfigParser."""
        config = MagicMock()
        config.destination_schema_name = schema
        config.destination_table_name = model
        config.source_schema_name = "raw"
        config.source_table_name = "source"
        config.materialization = materialization
        return config

    @patch('backend.application.config_parser.dag_executor.IbisBuilder')
    @patch('backend.application.config_parser.dag_executor.MaterializationHandler')
    def test_ephemeral_not_registered_as_table(
        self,
        mock_mat_handler_cls,
        mock_ibis_builder_cls,
    ):
        """Test that ephemeral models don't get registered as tables."""
        FeatureFlags.set_state(execution_mode=ExecutionMode.DIRECT)

        # Setup mocks
        mock_builder = MagicMock()
        mock_compilation = MagicMock()
        mock_compilation.sql = "SELECT 1"
        mock_builder.compile_transformation.return_value = mock_compilation
        mock_ibis_builder_cls.return_value = mock_builder

        mock_mat_handler = MagicMock()
        mat_config = MaterializationConfig(
            schema_name="dev",
            table_name="ephemeral_model",
            sql="SELECT 1",
            mode=MaterializationMode.EPHEMERAL,
        )
        mock_mat_handler.execute.return_value = MaterializationResult(
            config=mat_config,
            success=True,
            cte_sql="ephemeral_model AS (SELECT 1)",
        )
        mock_mat_handler_cls.return_value = mock_mat_handler

        # Create DAG with ephemeral model
        dag = nx.DiGraph()
        config = self._create_mock_config("dev", "ephemeral_model", "EPHEMERAL")
        dag.add_node(("dev", "ephemeral_model"), config=config)

        mock_registry = MagicMock()
        mock_registry.contains.return_value = False

        executor = DAGExecutor(dag, registry=mock_registry)
        result = executor.execute()

        assert result.success is True
        # Registry.register should NOT be called for ephemeral
        mock_registry.register.assert_not_called()
