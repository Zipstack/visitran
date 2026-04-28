"""Unit tests for DAG Builder with NetworkX integration."""

from pathlib import Path
from unittest.mock import MagicMock, patch
import tempfile

import pytest
import networkx as nx

from backend.application.config_parser.dag_builder import (
    DAGBuilder,
    DAGBuildError,
    ModelNode,
    CycleDetectedError,
    MissingDependencyError,
)
from backend.application.config_parser.model_registry import ModelRegistry
from backend.application.config_parser.config_parser import ConfigParser


class TestModelNode:
    """Tests for the ModelNode dataclass."""

    def test_model_node_creation(self):
        """Test creating a ModelNode with all fields."""
        node = ModelNode(
            schema="public",
            model="orders",
            config=None,
            file_path=Path("/tmp/orders.yaml"),
            dependencies=[("public", "customers")],
        )

        assert node.schema == "public"
        assert node.model == "orders"
        assert node.key == ("public", "orders")
        assert node.qualified_name == "public.orders"
        assert node.dependencies == [("public", "customers")]

    def test_model_node_key_property(self):
        """Test the key property returns (schema, model) tuple."""
        node = ModelNode(schema="analytics", model="summary")
        assert node.key == ("analytics", "summary")

    def test_model_node_qualified_name(self):
        """Test the qualified_name property."""
        node = ModelNode(schema="dw", model="fact_sales")
        assert node.qualified_name == "dw.fact_sales"


class TestDAGBuildError:
    """Tests for the DAGBuildError dataclass."""

    def test_error_creation(self):
        """Test creating DAGBuildError with all fields."""
        error = DAGBuildError(
            message="Test error",
            file_path=Path("/tmp/model.yaml"),
            line_number=10,
            column_number=5,
            model_key=("public", "orders"),
        )

        assert error.message == "Test error"
        assert error.line_number == 10
        assert error.column_number == 5

    def test_to_transformation_error(self):
        """Test conversion to TransformationError."""
        error = DAGBuildError(
            message="Validation failed",
            line_number=15,
            column_number=3,
        )

        te = error.to_transformation_error("test_model")
        assert te.model_name == "test_model"
        assert te.line_number == 15
        assert te.column_number == 3


class TestCycleDetectedError:
    """Tests for the CycleDetectedError exception."""

    def test_cycle_error_creation(self):
        """Test creating CycleDetectedError."""
        cycle = [("public", "a"), ("public", "b"), ("public", "a")]
        error = CycleDetectedError(cycle)

        assert error.cycle == cycle
        assert "public.a" in error.message
        assert "public.b" in error.message
        assert "Circular dependency" in error.message

    def test_cycle_error_with_custom_message(self):
        """Test CycleDetectedError with custom message."""
        cycle = [("s", "m")]
        error = CycleDetectedError(cycle, message="Custom cycle message")
        assert error.message == "Custom cycle message"


class TestMissingDependencyError:
    """Tests for the MissingDependencyError exception."""

    def test_missing_dep_error_creation(self):
        """Test creating MissingDependencyError."""
        error = MissingDependencyError(
            model_key=("public", "orders"),
            dependency_key=("public", "nonexistent"),
        )

        assert error.model_key == ("public", "orders")
        assert error.dependency_key == ("public", "nonexistent")
        assert "public.orders" in error.message
        assert "public.nonexistent" in error.message

    def test_missing_dep_error_with_file_path(self):
        """Test MissingDependencyError includes file path."""
        error = MissingDependencyError(
            model_key=("public", "orders"),
            dependency_key=("public", "missing"),
            file_path=Path("/models/orders.yaml"),
        )

        assert "orders.yaml" in error.message


class TestDAGBuilderInit:
    """Tests for DAGBuilder initialization."""

    def setup_method(self):
        """Reset ModelRegistry before each test."""
        ModelRegistry.reset_instance()

    def test_init_with_no_args(self):
        """Test initializing DAGBuilder with no arguments."""
        builder = DAGBuilder()

        assert builder.model_count == 0
        assert isinstance(builder.dag, nx.DiGraph)
        assert len(builder.errors) == 0

    def test_init_with_yaml_paths(self):
        """Test initializing with YAML paths."""
        builder = DAGBuilder(yaml_paths=["/tmp/a.yaml", Path("/tmp/b.yaml")])

        assert len(builder._yaml_paths) == 2
        assert all(isinstance(p, Path) for p in builder._yaml_paths)

    def test_init_with_registry(self):
        """Test initializing with existing registry."""
        registry = ModelRegistry()
        builder = DAGBuilder(registry=registry)

        assert builder.registry is registry

    def test_init_with_configs(self):
        """Test initializing with pre-parsed configs."""
        config = MagicMock(spec=ConfigParser)
        builder = DAGBuilder(configs=[config])

        assert len(builder._configs) == 1


class TestDAGBuilderPassOne:
    """Tests for DAGBuilder Pass 1 (Model Registration)."""

    def setup_method(self):
        """Reset ModelRegistry and ConfigParser caches before each test."""
        ModelRegistry.reset_instance()
        ConfigParser._instances.clear()

    def test_pass_one_with_configs(self):
        """Test Pass 1 with pre-parsed ConfigParser instances."""
        config = MagicMock(spec=ConfigParser)
        config.destination_schema_name = "public"
        config.model_name = "orders"
        config.destination_table_name = "orders"
        config.source_schema_name = "raw"
        config.materialization = "TABLE"
        config.get.return_value = []
        config.reference = []
        config.source_model = None

        builder = DAGBuilder(configs=[config])
        builder.execute_pass_one()

        assert builder.model_count == 1
        assert ("public", "orders") in builder._model_nodes

    def test_pass_one_extracts_dependencies_from_field(self):
        """Test that dependencies field is extracted."""
        config = MagicMock(spec=ConfigParser)
        config.destination_schema_name = "public"
        config.model_name = "orders"
        config.destination_table_name = "orders"
        config.source_schema_name = "raw"
        config.materialization = "TABLE"
        config.get.side_effect = lambda key, default=None: (
            [{"schema": "public", "model": "customers"}]
            if key == "dependencies"
            else default
        )
        config.reference = []
        config.source_model = None

        builder = DAGBuilder(configs=[config])
        builder.execute_pass_one()

        node = builder._model_nodes[("public", "orders")]
        assert ("public", "customers") in node.dependencies

    def test_pass_one_extracts_dependencies_from_reference(self):
        """Test that reference field is extracted as dependencies."""
        config = MagicMock(spec=ConfigParser)
        config.destination_schema_name = "analytics"
        config.model_name = "summary"
        config.destination_table_name = "summary"
        config.source_schema_name = "public"
        config.materialization = "TABLE"
        config.get.return_value = []
        config.reference = ["public.orders", "customers"]
        config.source_model = None

        builder = DAGBuilder(configs=[config])
        builder.execute_pass_one()

        node = builder._model_nodes[("analytics", "summary")]
        assert ("public", "orders") in node.dependencies
        assert ("public", "customers") in node.dependencies

    def test_pass_one_extracts_source_model(self):
        """Test that source_model is extracted as dependency."""
        config = MagicMock(spec=ConfigParser)
        config.destination_schema_name = "analytics"
        config.model_name = "derived"
        config.destination_table_name = "derived"
        config.source_schema_name = "public"
        config.materialization = "TABLE"
        config.get.return_value = []
        config.reference = []
        config.source_model = "base_model"

        builder = DAGBuilder(configs=[config])
        builder.execute_pass_one()

        node = builder._model_nodes[("analytics", "derived")]
        assert ("public", "base_model") in node.dependencies

    def test_pass_one_skips_duplicate_dependencies(self):
        """Test that duplicate dependencies are deduplicated."""
        config = MagicMock(spec=ConfigParser)
        config.destination_schema_name = "public"
        config.model_name = "orders"
        config.destination_table_name = "orders"
        config.source_schema_name = "public"
        config.materialization = "TABLE"
        config.get.side_effect = lambda key, default=None: (
            ["public.customers"] if key == "dependencies" else default
        )
        config.reference = ["public.customers"]  # Same dependency
        config.source_model = None

        builder = DAGBuilder(configs=[config])
        builder.execute_pass_one()

        node = builder._model_nodes[("public", "orders")]
        assert node.dependencies.count(("public", "customers")) == 1

    def test_pass_one_cannot_run_twice(self):
        """Test that Pass 1 only runs once."""
        config = MagicMock(spec=ConfigParser)
        config.destination_schema_name = "public"
        config.model_name = "orders"
        config.destination_table_name = "orders"
        config.source_schema_name = "raw"
        config.materialization = "TABLE"
        config.get.return_value = []
        config.reference = []
        config.source_model = None

        builder = DAGBuilder(configs=[config])
        builder.execute_pass_one()
        builder.execute_pass_one()  # Should be skipped with warning

        assert builder._pass_one_complete is True


class TestDAGBuilderPassTwo:
    """Tests for DAGBuilder Pass 2 (Graph Construction)."""

    def setup_method(self):
        """Reset ModelRegistry and ConfigParser caches before each test."""
        ModelRegistry.reset_instance()
        ConfigParser._instances.clear()

    def _create_mock_config(
        self, schema: str, model: str, deps: list = None
    ) -> MagicMock:
        """Helper to create mock ConfigParser."""
        config = MagicMock(spec=ConfigParser)
        config.destination_schema_name = schema
        config.model_name = model
        config.destination_table_name = model
        config.source_schema_name = schema
        config.materialization = "TABLE"
        config.get.side_effect = lambda key, default=None: (
            deps if key == "dependencies" and deps else default
        )
        config.reference = []
        config.source_model = None
        return config

    def test_pass_two_requires_pass_one(self):
        """Test that Pass 2 fails if Pass 1 not executed."""
        builder = DAGBuilder()

        with pytest.raises(RuntimeError, match="Pass 1 must be executed"):
            builder.execute_pass_two()

    def test_pass_two_creates_nodes(self):
        """Test that Pass 2 creates DAG nodes."""
        config = self._create_mock_config("public", "orders")

        builder = DAGBuilder(configs=[config])
        builder.execute_pass_one()
        builder.execute_pass_two()

        assert ("public", "orders") in builder.dag.nodes

    def test_pass_two_creates_edges(self):
        """Test that Pass 2 creates edges for dependencies."""
        # Create two models where orders depends on customers
        customers = self._create_mock_config("public", "customers")
        orders = self._create_mock_config(
            "public", "orders", deps=[{"schema": "public", "model": "customers"}]
        )

        builder = DAGBuilder(configs=[customers, orders])
        builder.execute_pass_one()
        builder.execute_pass_two()

        # Edge: orders -> customers (orders depends on customers)
        assert builder.dag.has_edge(("public", "orders"), ("public", "customers"))

    def test_pass_two_missing_dependency_strict(self):
        """Test that missing dependency raises error in strict mode."""
        orders = self._create_mock_config(
            "public", "orders", deps=[{"schema": "public", "model": "missing"}]
        )

        builder = DAGBuilder(configs=[orders])
        builder.execute_pass_one()

        with pytest.raises(MissingDependencyError):
            builder.execute_pass_two(strict=True)

    def test_pass_two_missing_dependency_non_strict(self):
        """Test that missing dependency is collected in non-strict mode."""
        orders = self._create_mock_config(
            "public", "orders", deps=[{"schema": "public", "model": "missing"}]
        )

        builder = DAGBuilder(configs=[orders])
        builder.execute_pass_one()
        builder.execute_pass_two(strict=False)

        assert len(builder.errors) == 1
        assert "missing" in builder.errors[0].message

    def test_pass_two_node_has_metadata(self):
        """Test that DAG nodes have metadata attached."""
        config = self._create_mock_config("analytics", "summary")

        builder = DAGBuilder(configs=[config])
        builder.execute_pass_one()
        builder.execute_pass_two()

        node_data = builder.dag.nodes[("analytics", "summary")]
        assert node_data["schema"] == "analytics"
        assert node_data["model"] == "summary"
        assert node_data["qualified_name"] == "analytics.summary"


class TestDAGBuilderBuild:
    """Tests for the DAGBuilder.build() method."""

    def setup_method(self):
        """Reset ModelRegistry and ConfigParser caches before each test."""
        ModelRegistry.reset_instance()
        ConfigParser._instances.clear()

    def _create_mock_config(
        self, schema: str, model: str, deps: list = None
    ) -> MagicMock:
        """Helper to create mock ConfigParser."""
        config = MagicMock(spec=ConfigParser)
        config.destination_schema_name = schema
        config.model_name = model
        config.destination_table_name = model
        config.source_schema_name = schema
        config.materialization = "TABLE"
        config.get.side_effect = lambda key, default=None: (
            deps if key == "dependencies" and deps else default
        )
        config.reference = []
        config.source_model = None
        return config

    def test_build_executes_both_passes(self):
        """Test that build() executes both passes."""
        config = self._create_mock_config("public", "orders")

        builder = DAGBuilder(configs=[config])
        dag = builder.build()

        assert builder._pass_one_complete
        assert builder._pass_two_complete
        assert ("public", "orders") in dag.nodes

    def test_build_validates_cycles_by_default(self):
        """Test that build() validates for cycles by default."""
        from visitran.errors import TransformationError

        # Create a cycle: a -> b -> a
        a = self._create_mock_config(
            "public", "a", deps=[{"schema": "public", "model": "b"}]
        )
        b = self._create_mock_config(
            "public", "b", deps=[{"schema": "public", "model": "a"}]
        )

        builder = DAGBuilder(configs=[a, b])

        # With incremental_cycle_check=True (default), raises TransformationError
        with pytest.raises(TransformationError):
            builder.build()

    def test_build_can_skip_cycle_validation(self):
        """Test that build() can skip cycle validation."""
        a = self._create_mock_config(
            "public", "a", deps=[{"schema": "public", "model": "b"}]
        )
        b = self._create_mock_config(
            "public", "b", deps=[{"schema": "public", "model": "a"}]
        )

        builder = DAGBuilder(configs=[a, b])
        # Must also disable incremental_cycle_check to allow cycle
        dag = builder.build(validate_cycles=False, incremental_cycle_check=False)

        # Should succeed without raising
        assert dag.number_of_nodes() == 2


class TestDAGBuilderCycleDetection:
    """Tests for cycle detection functionality."""

    def setup_method(self):
        """Reset ModelRegistry and ConfigParser caches before each test."""
        ModelRegistry.reset_instance()
        ConfigParser._instances.clear()

    def _create_mock_config(
        self, schema: str, model: str, deps: list = None
    ) -> MagicMock:
        """Helper to create mock ConfigParser."""
        config = MagicMock(spec=ConfigParser)
        config.destination_schema_name = schema
        config.model_name = model
        config.destination_table_name = model
        config.source_schema_name = schema
        config.materialization = "TABLE"
        config.get.side_effect = lambda key, default=None: (
            deps if key == "dependencies" and deps else default
        )
        config.reference = []
        config.source_model = None
        return config

    def test_no_cycle_in_linear_chain(self):
        """Test that linear dependency chain has no cycle."""
        a = self._create_mock_config("p", "a")
        b = self._create_mock_config("p", "b", deps=[{"schema": "p", "model": "a"}])
        c = self._create_mock_config("p", "c", deps=[{"schema": "p", "model": "b"}])

        builder = DAGBuilder(configs=[a, b, c])
        builder.build()  # Should not raise

    def test_self_dependency_is_cycle(self):
        """Test that self-dependency is detected as cycle."""
        from visitran.errors import TransformationError

        a = self._create_mock_config(
            "p", "a", deps=[{"schema": "p", "model": "a"}]
        )

        builder = DAGBuilder(configs=[a])

        # With incremental cycle check, raises TransformationError
        with pytest.raises(TransformationError) as exc_info:
            builder.build()

        assert "p.a" in exc_info.value.error_message

    def test_two_node_cycle(self):
        """Test detection of two-node cycle."""
        from visitran.errors import TransformationError

        a = self._create_mock_config("p", "a", deps=[{"schema": "p", "model": "b"}])
        b = self._create_mock_config("p", "b", deps=[{"schema": "p", "model": "a"}])

        builder = DAGBuilder(configs=[a, b])

        with pytest.raises(TransformationError) as exc_info:
            builder.build()

        assert "Circular dependency" in exc_info.value.error_message

    def test_three_node_cycle(self):
        """Test detection of three-node cycle."""
        from visitran.errors import TransformationError

        a = self._create_mock_config("p", "a", deps=[{"schema": "p", "model": "b"}])
        b = self._create_mock_config("p", "b", deps=[{"schema": "p", "model": "c"}])
        c = self._create_mock_config("p", "c", deps=[{"schema": "p", "model": "a"}])

        builder = DAGBuilder(configs=[a, b, c])

        with pytest.raises(TransformationError) as exc_info:
            builder.build()

        assert "Circular dependency" in exc_info.value.error_message


class TestDAGBuilderTopologicalOrder:
    """Tests for topological ordering functionality."""

    def setup_method(self):
        """Reset ModelRegistry and ConfigParser caches before each test."""
        ModelRegistry.reset_instance()
        ConfigParser._instances.clear()

    def _create_mock_config(
        self, schema: str, model: str, deps: list = None
    ) -> MagicMock:
        """Helper to create mock ConfigParser."""
        config = MagicMock(spec=ConfigParser)
        config.destination_schema_name = schema
        config.model_name = model
        config.destination_table_name = model
        config.source_schema_name = schema
        config.materialization = "TABLE"
        config.get.side_effect = lambda key, default=None: (
            deps if key == "dependencies" and deps else default
        )
        config.reference = []
        config.source_model = None
        return config

    def test_topological_order_requires_build(self):
        """Test that topological order requires build to be called."""
        builder = DAGBuilder()

        with pytest.raises(RuntimeError, match="DAG must be built"):
            builder.get_topological_order()

    def test_topological_order_dependencies_first(self):
        """Test that dependencies come before dependents in order."""
        a = self._create_mock_config("p", "a")
        b = self._create_mock_config("p", "b", deps=[{"schema": "p", "model": "a"}])
        c = self._create_mock_config("p", "c", deps=[{"schema": "p", "model": "b"}])

        builder = DAGBuilder(configs=[a, b, c])
        builder.build()

        order = builder.get_topological_order()

        # a must come before b, b must come before c
        idx_a = order.index(("p", "a"))
        idx_b = order.index(("p", "b"))
        idx_c = order.index(("p", "c"))

        assert idx_a < idx_b < idx_c

    def test_topological_order_diamond_dependency(self):
        """Test topological order with diamond dependency pattern."""
        #     a
        #    / \
        #   b   c
        #    \ /
        #     d
        a = self._create_mock_config("p", "a")
        b = self._create_mock_config("p", "b", deps=[{"schema": "p", "model": "a"}])
        c = self._create_mock_config("p", "c", deps=[{"schema": "p", "model": "a"}])
        d = self._create_mock_config(
            "p", "d",
            deps=[
                {"schema": "p", "model": "b"},
                {"schema": "p", "model": "c"},
            ],
        )

        builder = DAGBuilder(configs=[a, b, c, d])
        builder.build()

        order = builder.get_topological_order()

        idx_a = order.index(("p", "a"))
        idx_b = order.index(("p", "b"))
        idx_c = order.index(("p", "c"))
        idx_d = order.index(("p", "d"))

        # a must come first, d must come last
        assert idx_a < idx_b
        assert idx_a < idx_c
        assert idx_b < idx_d
        assert idx_c < idx_d


class TestDAGBuilderDependencyQueries:
    """Tests for dependency query methods."""

    def setup_method(self):
        """Reset ModelRegistry and ConfigParser caches before each test."""
        ModelRegistry.reset_instance()
        ConfigParser._instances.clear()

    def _create_mock_config(
        self, schema: str, model: str, deps: list = None
    ) -> MagicMock:
        """Helper to create mock ConfigParser."""
        config = MagicMock(spec=ConfigParser)
        config.destination_schema_name = schema
        config.model_name = model
        config.destination_table_name = model
        config.source_schema_name = schema
        config.materialization = "TABLE"
        config.get.side_effect = lambda key, default=None: (
            deps if key == "dependencies" and deps else default
        )
        config.reference = []
        config.source_model = None
        return config

    def test_get_dependencies_returns_direct_deps(self):
        """Test get_dependencies returns only direct dependencies."""
        a = self._create_mock_config("p", "a")
        b = self._create_mock_config("p", "b", deps=[{"schema": "p", "model": "a"}])
        c = self._create_mock_config("p", "c", deps=[{"schema": "p", "model": "b"}])

        builder = DAGBuilder(configs=[a, b, c])
        builder.build()

        # c depends on b directly, not a
        deps = builder.get_dependencies("p", "c")
        assert ("p", "b") in deps
        assert ("p", "a") not in deps

    def test_get_dependents_returns_direct_dependents(self):
        """Test get_dependents returns only direct dependents."""
        a = self._create_mock_config("p", "a")
        b = self._create_mock_config("p", "b", deps=[{"schema": "p", "model": "a"}])
        c = self._create_mock_config("p", "c", deps=[{"schema": "p", "model": "b"}])

        builder = DAGBuilder(configs=[a, b, c])
        builder.build()

        # Only b directly depends on a
        dependents = builder.get_dependents("p", "a")
        assert ("p", "b") in dependents
        assert ("p", "c") not in dependents

    def test_get_all_upstream_returns_transitive(self):
        """Test get_all_upstream returns all transitive dependencies."""
        a = self._create_mock_config("p", "a")
        b = self._create_mock_config("p", "b", deps=[{"schema": "p", "model": "a"}])
        c = self._create_mock_config("p", "c", deps=[{"schema": "p", "model": "b"}])

        builder = DAGBuilder(configs=[a, b, c])
        builder.build()

        upstream = builder.get_all_upstream("p", "c")
        assert ("p", "a") in upstream
        assert ("p", "b") in upstream

    def test_get_all_downstream_returns_transitive(self):
        """Test get_all_downstream returns all transitive dependents."""
        a = self._create_mock_config("p", "a")
        b = self._create_mock_config("p", "b", deps=[{"schema": "p", "model": "a"}])
        c = self._create_mock_config("p", "c", deps=[{"schema": "p", "model": "b"}])

        builder = DAGBuilder(configs=[a, b, c])
        builder.build()

        downstream = builder.get_all_downstream("p", "a")
        assert ("p", "b") in downstream
        assert ("p", "c") in downstream

    def test_query_unknown_model_raises_keyerror(self):
        """Test that querying unknown model raises KeyError."""
        a = self._create_mock_config("p", "a")

        builder = DAGBuilder(configs=[a])
        builder.build()

        with pytest.raises(KeyError):
            builder.get_dependencies("p", "unknown")


class TestDAGBuilderAddModel:
    """Tests for manually adding models."""

    def setup_method(self):
        """Reset ModelRegistry and ConfigParser caches before each test."""
        ModelRegistry.reset_instance()
        ConfigParser._instances.clear()

    def _create_mock_config(self, schema: str, model: str) -> MagicMock:
        """Helper to create mock ConfigParser."""
        config = MagicMock(spec=ConfigParser)
        config.destination_schema_name = schema
        config.model_name = model
        config.destination_table_name = model
        return config

    def test_add_model_registers_in_builder(self):
        """Test that add_model adds model to builder."""
        config = self._create_mock_config("public", "orders")

        builder = DAGBuilder()
        builder.add_model("public", "orders", config)

        assert ("public", "orders") in builder._model_nodes

    def test_add_model_with_dependencies(self):
        """Test adding model with explicit dependencies."""
        config = self._create_mock_config("public", "orders")

        builder = DAGBuilder()
        builder.add_model(
            "public",
            "orders",
            config,
            dependencies=[("public", "customers")],
        )

        node = builder._model_nodes[("public", "orders")]
        assert ("public", "customers") in node.dependencies

    def test_add_model_after_build_fails(self):
        """Test that adding model after build raises error."""
        config1 = self._create_mock_config("public", "orders")
        config2 = self._create_mock_config("public", "customers")

        builder = DAGBuilder()
        builder.add_model("public", "orders", config1)
        builder.execute_pass_two()

        with pytest.raises(RuntimeError):
            builder.add_model("public", "customers", config2)


class TestDAGBuilderClear:
    """Tests for clear functionality."""

    def setup_method(self):
        """Reset ModelRegistry and ConfigParser caches before each test."""
        ModelRegistry.reset_instance()
        ConfigParser._instances.clear()

    def _create_mock_config(self, schema: str, model: str) -> MagicMock:
        """Helper to create mock ConfigParser."""
        config = MagicMock(spec=ConfigParser)
        config.destination_schema_name = schema
        config.model_name = model
        config.destination_table_name = model
        config.source_schema_name = schema
        config.materialization = "TABLE"
        config.get.return_value = []
        config.reference = []
        config.source_model = None
        return config

    def test_clear_resets_state(self):
        """Test that clear resets builder to initial state."""
        config = self._create_mock_config("public", "orders")

        builder = DAGBuilder(configs=[config])
        builder.build(validate_cycles=False)

        assert builder.model_count == 1

        builder.clear()

        assert builder.model_count == 0
        assert builder.dag.number_of_nodes() == 0
        assert builder._pass_one_complete is False
        assert builder._pass_two_complete is False


class TestDAGBuilderGetDag:
    """Tests for get_dag method."""

    def setup_method(self):
        """Reset ModelRegistry and ConfigParser caches before each test."""
        ModelRegistry.reset_instance()
        ConfigParser._instances.clear()

    def test_get_dag_before_build_raises_error(self):
        """Test that get_dag before build raises RuntimeError."""
        builder = DAGBuilder()

        with pytest.raises(RuntimeError, match="DAG not yet constructed"):
            builder.get_dag()

    def test_get_dag_returns_graph(self):
        """Test that get_dag returns the NetworkX graph."""
        config = MagicMock(spec=ConfigParser)
        config.destination_schema_name = "p"
        config.model_name = "m"
        config.destination_table_name = "m"
        config.source_schema_name = "p"
        config.materialization = "TABLE"
        config.get.return_value = []
        config.reference = []
        config.source_model = None

        builder = DAGBuilder(configs=[config])
        builder.build(validate_cycles=False)

        dag = builder.get_dag()
        assert isinstance(dag, nx.DiGraph)


class TestDAGBuilderYAMLFiles:
    """Tests for loading from YAML files."""

    def setup_method(self):
        """Reset ModelRegistry and ConfigParser caches before each test."""
        ModelRegistry.reset_instance()
        ConfigParser._instances.clear()

    def test_file_not_found_strict_raises(self):
        """Test that missing file raises FileNotFoundError in strict mode."""
        builder = DAGBuilder(yaml_paths=["/nonexistent/model.yaml"])

        with pytest.raises(FileNotFoundError):
            builder.execute_pass_one(strict=True)

    def test_file_not_found_non_strict_collects_error(self):
        """Test that missing file is collected in non-strict mode."""
        builder = DAGBuilder(yaml_paths=["/nonexistent/model.yaml"])
        builder.execute_pass_one(strict=False)

        assert len(builder.errors) == 1
        assert "not found" in builder.errors[0].message.lower()

    def test_load_valid_yaml_file(self):
        """Test loading a valid YAML file."""
        yaml_content = """
source:
  table_name: raw_orders
  schema_name: raw
model:
  table_name: orders
  schema_name: public
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(yaml_content)
            f.flush()
            file_path = Path(f.name)

        try:
            builder = DAGBuilder(yaml_paths=[file_path])
            builder.execute_pass_one()

            assert builder.model_count == 1
        finally:
            file_path.unlink()
