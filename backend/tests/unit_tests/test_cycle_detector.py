"""Unit tests for Incremental Cycle Detection."""

import pytest
import networkx as nx

from backend.application.config_parser.cycle_detector import (
    CycleDetector,
    CycleInfo,
    CycleNodeInfo,
    check_would_create_cycle,
    find_cycle_path,
)
from visitran.errors import TransformationError


class TestCycleNodeInfo:
    """Tests for CycleNodeInfo dataclass."""

    def test_creation(self):
        """Test creating CycleNodeInfo with all fields."""
        info = CycleNodeInfo(
            schema="public",
            model="orders",
            file_path="/models/orders.yaml",
            line_number=10,
        )

        assert info.schema == "public"
        assert info.model == "orders"
        assert info.key == ("public", "orders")
        assert info.qualified_name == "public.orders"

    def test_location_str_with_all_info(self):
        """Test location_str with file and line."""
        info = CycleNodeInfo(
            schema="p", model="m",
            file_path="/path/to/file.yaml",
            line_number=42,
        )
        assert info.location_str == "/path/to/file.yaml:42"

    def test_location_str_file_only(self):
        """Test location_str with file but no line."""
        info = CycleNodeInfo(
            schema="p", model="m",
            file_path="/path/to/file.yaml",
        )
        assert info.location_str == "/path/to/file.yaml"

    def test_location_str_unknown(self):
        """Test location_str with no file info."""
        info = CycleNodeInfo(schema="p", model="m")
        assert info.location_str == "unknown location"


class TestCycleInfo:
    """Tests for CycleInfo dataclass."""

    def test_creation(self):
        """Test creating CycleInfo."""
        cycle_path = [("p", "a"), ("p", "b"), ("p", "a")]
        trigger = (("p", "a"), ("p", "b"))

        info = CycleInfo(cycle_path=cycle_path, trigger_edge=trigger)

        assert info.cycle_path == cycle_path
        assert info.trigger_edge == trigger

    def test_cycle_str(self):
        """Test formatted cycle string."""
        cycle_path = [("p", "a"), ("p", "b"), ("p", "c"), ("p", "a")]
        info = CycleInfo(
            cycle_path=cycle_path,
            trigger_edge=(("p", "a"), ("p", "b")),
        )

        assert info.cycle_str == "p.a → p.b → p.c → p.a"

    def test_model_names(self):
        """Test model names list."""
        cycle_path = [("public", "orders"), ("public", "items")]
        info = CycleInfo(
            cycle_path=cycle_path,
            trigger_edge=(("public", "orders"), ("public", "items")),
        )

        assert info.model_names == ["public.orders", "public.items"]

    def test_to_transformation_error(self):
        """Test conversion to TransformationError."""
        cycle_path = [("p", "a"), ("p", "b"), ("p", "a")]
        nodes_info = [
            CycleNodeInfo("p", "a", "/a.yaml", 10),
            CycleNodeInfo("p", "b", "/b.yaml", 20),
        ]
        info = CycleInfo(
            cycle_path=cycle_path,
            trigger_edge=(("p", "a"), ("p", "b")),
            nodes_info=nodes_info,
        )

        error = info.to_transformation_error()

        assert isinstance(error, TransformationError)
        assert "Circular dependency" in error.error_message
        assert "p.a → p.b → p.a" in error.error_message
        assert "/a.yaml:10" in error.error_message
        assert "/b.yaml:20" in error.error_message

    def test_to_transformation_error_with_model_name(self):
        """Test conversion with explicit model name."""
        info = CycleInfo(
            cycle_path=[("p", "a"), ("p", "a")],
            trigger_edge=(("p", "a"), ("p", "a")),
        )

        error = info.to_transformation_error(model_name="custom_model")
        assert error.model_name == "custom_model"


class TestCycleDetectorInit:
    """Tests for CycleDetector initialization."""

    def test_init_creates_new_dag(self):
        """Test initialization creates new DiGraph."""
        detector = CycleDetector()
        assert isinstance(detector.dag, nx.DiGraph)
        assert detector.dag.number_of_nodes() == 0

    def test_init_with_existing_dag(self):
        """Test initialization with existing DiGraph."""
        existing = nx.DiGraph()
        existing.add_node(("p", "a"))

        detector = CycleDetector(existing)
        assert detector.dag is existing
        assert detector.dag.number_of_nodes() == 1


class TestCycleDetectorNodeMetadata:
    """Tests for node metadata management."""

    def test_set_node_metadata(self):
        """Test setting node metadata."""
        detector = CycleDetector()
        detector.set_node_metadata(
            ("p", "m"),
            file_path="/test.yaml",
            line_number=10,
        )

        metadata = detector.get_node_metadata(("p", "m"))
        assert metadata["file_path"] == "/test.yaml"
        assert metadata["line_number"] == 10

    def test_get_node_metadata_missing(self):
        """Test getting metadata for unknown node."""
        detector = CycleDetector()
        metadata = detector.get_node_metadata(("p", "unknown"))
        assert metadata == {}

    def test_set_node_metadata_extra_fields(self):
        """Test setting extra metadata fields."""
        detector = CycleDetector()
        detector.set_node_metadata(
            ("p", "m"),
            custom_field="value",
        )

        metadata = detector.get_node_metadata(("p", "m"))
        assert metadata["custom_field"] == "value"


class TestCycleDetectorCheckEdge:
    """Tests for check_edge_would_create_cycle."""

    def test_self_loop_creates_cycle(self):
        """Test that self-loop is detected as cycle."""
        detector = CycleDetector()
        detector.dag.add_node(("p", "a"))

        result = detector.check_edge_would_create_cycle(("p", "a"), ("p", "a"))

        assert result is not None
        assert ("p", "a") in result.cycle_path

    def test_no_cycle_empty_graph(self):
        """Test no cycle in empty graph."""
        detector = CycleDetector()

        result = detector.check_edge_would_create_cycle(("p", "a"), ("p", "b"))

        assert result is None

    def test_no_cycle_linear_chain(self):
        """Test no cycle in linear chain."""
        detector = CycleDetector()
        detector.dag.add_edge(("p", "a"), ("p", "b"))

        # Adding c -> a doesn't create cycle
        result = detector.check_edge_would_create_cycle(("p", "c"), ("p", "a"))
        assert result is None

    def test_two_node_cycle_detected(self):
        """Test two-node cycle detection."""
        detector = CycleDetector()
        detector.dag.add_edge(("p", "a"), ("p", "b"))

        # Adding b -> a creates cycle
        result = detector.check_edge_would_create_cycle(("p", "b"), ("p", "a"))

        assert result is not None
        assert ("p", "a") in result.cycle_path
        assert ("p", "b") in result.cycle_path

    def test_three_node_cycle_detected(self):
        """Test three-node cycle detection."""
        detector = CycleDetector()
        detector.dag.add_edge(("p", "a"), ("p", "b"))
        detector.dag.add_edge(("p", "b"), ("p", "c"))

        # Adding c -> a creates cycle
        result = detector.check_edge_would_create_cycle(("p", "c"), ("p", "a"))

        assert result is not None
        # Should contain all three nodes
        cycle_nodes = set(result.cycle_path)
        assert ("p", "a") in cycle_nodes
        assert ("p", "b") in cycle_nodes
        assert ("p", "c") in cycle_nodes

    def test_does_not_modify_graph(self):
        """Test that check doesn't modify the graph."""
        detector = CycleDetector()
        detector.dag.add_edge(("p", "a"), ("p", "b"))
        initial_edges = list(detector.dag.edges())

        detector.check_edge_would_create_cycle(("p", "b"), ("p", "a"))

        assert list(detector.dag.edges()) == initial_edges


class TestCycleDetectorAddEdgeWithCheck:
    """Tests for add_edge_with_check method."""

    def test_adds_edge_when_no_cycle(self):
        """Test edge is added when no cycle would be created."""
        detector = CycleDetector()

        result = detector.add_edge_with_check(("p", "a"), ("p", "b"))

        assert result is None
        assert detector.dag.has_edge(("p", "a"), ("p", "b"))

    def test_creates_nodes_if_missing(self):
        """Test nodes are created if they don't exist."""
        detector = CycleDetector()

        detector.add_edge_with_check(("p", "a"), ("p", "b"))

        assert ("p", "a") in detector.dag.nodes
        assert ("p", "b") in detector.dag.nodes

    def test_raises_on_cycle_strict_mode(self):
        """Test TransformationError raised in strict mode."""
        detector = CycleDetector()
        detector.dag.add_edge(("p", "a"), ("p", "b"))

        with pytest.raises(TransformationError) as exc_info:
            detector.add_edge_with_check(("p", "b"), ("p", "a"), raise_on_cycle=True)

        assert "Circular dependency" in str(exc_info.value.error_message)

    def test_returns_info_non_strict_mode(self):
        """Test CycleInfo returned in non-strict mode."""
        detector = CycleDetector()
        detector.dag.add_edge(("p", "a"), ("p", "b"))

        result = detector.add_edge_with_check(
            ("p", "b"), ("p", "a"),
            raise_on_cycle=False,
        )

        assert result is not None
        assert isinstance(result, CycleInfo)
        # Edge should NOT be added
        assert not detector.dag.has_edge(("p", "b"), ("p", "a"))

    def test_stores_metadata(self):
        """Test metadata is stored for nodes."""
        detector = CycleDetector()

        detector.add_edge_with_check(
            ("p", "a"), ("p", "b"),
            from_metadata={"file_path": "/a.yaml"},
            to_metadata={"file_path": "/b.yaml"},
        )

        assert detector.get_node_metadata(("p", "a"))["file_path"] == "/a.yaml"
        assert detector.get_node_metadata(("p", "b"))["file_path"] == "/b.yaml"

    def test_cycle_detection_is_immediate(self):
        """Test that cycle is detected immediately, not after more edges."""
        detector = CycleDetector()

        # Add edges a -> b -> c
        detector.add_edge_with_check(("p", "a"), ("p", "b"))
        detector.add_edge_with_check(("p", "b"), ("p", "c"))

        # Adding c -> a should immediately raise
        with pytest.raises(TransformationError):
            detector.add_edge_with_check(("p", "c"), ("p", "a"))


class TestCycleDetectorAddNode:
    """Tests for add_node method."""

    def test_add_node_with_metadata(self):
        """Test adding node with metadata."""
        detector = CycleDetector()

        detector.add_node(
            ("p", "model"),
            file_path="/test.yaml",
            line_number=15,
        )

        assert ("p", "model") in detector.dag.nodes
        metadata = detector.get_node_metadata(("p", "model"))
        assert metadata["file_path"] == "/test.yaml"
        assert metadata["line_number"] == 15


class TestCycleDetectorDetectAllCycles:
    """Tests for detect_all_cycles method."""

    def test_no_cycles(self):
        """Test no cycles in valid DAG."""
        detector = CycleDetector()
        detector.dag.add_edge(("p", "a"), ("p", "b"))
        detector.dag.add_edge(("p", "b"), ("p", "c"))

        cycles = detector.detect_all_cycles()
        assert len(cycles) == 0

    def test_detects_single_cycle(self):
        """Test detecting single cycle."""
        detector = CycleDetector()
        detector.dag.add_edge(("p", "a"), ("p", "b"))
        detector.dag.add_edge(("p", "b"), ("p", "a"))

        cycles = detector.detect_all_cycles()
        assert len(cycles) == 1

    def test_detects_self_loop(self):
        """Test detecting self-loop cycle."""
        detector = CycleDetector()
        detector.dag.add_edge(("p", "a"), ("p", "a"))

        cycles = detector.detect_all_cycles()
        assert len(cycles) == 1


class TestCycleDetectorClear:
    """Tests for clear method."""

    def test_clear_removes_all(self):
        """Test clear removes nodes, edges, and metadata."""
        detector = CycleDetector()
        detector.add_node(("p", "a"), file_path="/a.yaml")
        detector.dag.add_edge(("p", "a"), ("p", "b"))

        detector.clear()

        assert detector.dag.number_of_nodes() == 0
        assert detector.dag.number_of_edges() == 0
        assert detector.get_node_metadata(("p", "a")) == {}


class TestConvenienceFunctions:
    """Tests for module-level convenience functions."""

    def test_check_would_create_cycle_self_loop(self):
        """Test check_would_create_cycle for self-loop."""
        dag = nx.DiGraph()
        dag.add_node(("p", "a"))

        assert check_would_create_cycle(dag, ("p", "a"), ("p", "a")) is True

    def test_check_would_create_cycle_no_cycle(self):
        """Test check_would_create_cycle when no cycle."""
        dag = nx.DiGraph()
        dag.add_edge(("p", "a"), ("p", "b"))

        assert check_would_create_cycle(dag, ("p", "c"), ("p", "a")) is False

    def test_check_would_create_cycle_detects_cycle(self):
        """Test check_would_create_cycle detects potential cycle."""
        dag = nx.DiGraph()
        dag.add_edge(("p", "a"), ("p", "b"))

        assert check_would_create_cycle(dag, ("p", "b"), ("p", "a")) is True

    def test_find_cycle_path_self_loop(self):
        """Test find_cycle_path for self-loop."""
        dag = nx.DiGraph()
        dag.add_node(("p", "a"))

        path = find_cycle_path(dag, ("p", "a"), ("p", "a"))
        assert path == [("p", "a"), ("p", "a")]

    def test_find_cycle_path_no_cycle(self):
        """Test find_cycle_path returns None when no cycle."""
        dag = nx.DiGraph()
        dag.add_edge(("p", "a"), ("p", "b"))

        path = find_cycle_path(dag, ("p", "c"), ("p", "a"))
        assert path is None

    def test_find_cycle_path_finds_path(self):
        """Test find_cycle_path returns correct path."""
        dag = nx.DiGraph()
        dag.add_edge(("p", "a"), ("p", "b"))
        dag.add_edge(("p", "b"), ("p", "c"))

        path = find_cycle_path(dag, ("p", "c"), ("p", "a"))

        assert path is not None
        assert ("p", "c") in path
        assert ("p", "a") in path
        assert ("p", "b") in path


class TestDAGBuilderIncrementalCycleDetection:
    """Tests for incremental cycle detection in DAGBuilder."""

    def setup_method(self):
        """Reset singletons before each test."""
        from backend.application.config_parser.model_registry import ModelRegistry
        from backend.application.config_parser.config_parser import ConfigParser

        ModelRegistry.reset_instance()
        ConfigParser._instances.clear()

    def _create_mock_config(self, schema, model, deps=None):
        """Create a mock ConfigParser."""
        from unittest.mock import MagicMock
        from backend.application.config_parser.config_parser import ConfigParser

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

    def test_incremental_detection_raises_transformation_error(self):
        """Test that incremental detection raises TransformationError."""
        from backend.application.config_parser.dag_builder import DAGBuilder

        # Create a -> b -> a cycle
        a = self._create_mock_config("p", "a", deps=[{"schema": "p", "model": "b"}])
        b = self._create_mock_config("p", "b", deps=[{"schema": "p", "model": "a"}])

        builder = DAGBuilder(configs=[a, b])

        with pytest.raises(TransformationError) as exc_info:
            builder.build(incremental_cycle_check=True)

        assert "Circular dependency" in exc_info.value.error_message

    def test_incremental_vs_posthoc_detection(self):
        """Test that incremental detection happens immediately."""
        from backend.application.config_parser.dag_builder import DAGBuilder

        # Create a -> b -> c -> a cycle
        a = self._create_mock_config("p", "a", deps=[{"schema": "p", "model": "b"}])
        b = self._create_mock_config("p", "b", deps=[{"schema": "p", "model": "c"}])
        c = self._create_mock_config("p", "c", deps=[{"schema": "p", "model": "a"}])

        # With incremental checking
        builder = DAGBuilder(configs=[a, b, c])
        with pytest.raises(TransformationError):
            builder.build(incremental_cycle_check=True, strict=True)

    def test_no_cycle_with_incremental_detection(self):
        """Test that valid DAG builds successfully with incremental detection."""
        from backend.application.config_parser.dag_builder import DAGBuilder

        a = self._create_mock_config("p", "a")
        b = self._create_mock_config("p", "b", deps=[{"schema": "p", "model": "a"}])
        c = self._create_mock_config("p", "c", deps=[{"schema": "p", "model": "b"}])

        builder = DAGBuilder(configs=[a, b, c])
        dag = builder.build(incremental_cycle_check=True)

        assert dag.number_of_nodes() == 3
        assert dag.number_of_edges() == 2

    def test_self_reference_detected_immediately(self):
        """Test self-reference cycle is detected immediately."""
        from backend.application.config_parser.dag_builder import DAGBuilder

        # Model that depends on itself
        a = self._create_mock_config("p", "a", deps=[{"schema": "p", "model": "a"}])

        builder = DAGBuilder(configs=[a])

        with pytest.raises(TransformationError) as exc_info:
            builder.build(incremental_cycle_check=True)

        assert "p.a" in exc_info.value.error_message

    def test_non_strict_mode_collects_cycle_errors(self):
        """Test non-strict mode collects cycle errors without raising."""
        from backend.application.config_parser.dag_builder import DAGBuilder

        # Create a -> b -> a cycle
        a = self._create_mock_config("p", "a", deps=[{"schema": "p", "model": "b"}])
        b = self._create_mock_config("p", "b", deps=[{"schema": "p", "model": "a"}])

        builder = DAGBuilder(configs=[a, b])
        builder.execute_pass_one(strict=False)
        builder.execute_pass_two(strict=False, incremental_cycle_check=True)

        # Should have collected errors
        assert len(builder.errors) > 0
        assert any("Circular" in e.message for e in builder.errors)
