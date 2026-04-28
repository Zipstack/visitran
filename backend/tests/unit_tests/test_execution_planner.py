"""Unit tests for Execution Planner."""

from unittest.mock import MagicMock
import pytest
import networkx as nx

from backend.application.config_parser.execution_planner import (
    ExecutionStep,
    ExecutionPlan,
    ExecutionPlanner,
    ExecutionPlanError,
    create_execution_plan,
    get_execution_order,
)
from backend.application.config_parser.config_parser import ConfigParser


class TestExecutionStep:
    """Tests for ExecutionStep dataclass."""

    def test_creation(self):
        """Test creating ExecutionStep with all fields."""
        config = MagicMock(spec=ConfigParser)
        step = ExecutionStep(
            schema="public",
            model="orders",
            config=config,
            order=0,
            metadata={"file_path": "/test.yaml"},
        )

        assert step.schema == "public"
        assert step.model == "orders"
        assert step.config is config
        assert step.order == 0
        assert step.metadata["file_path"] == "/test.yaml"

    def test_key_property(self):
        """Test the key property returns tuple."""
        step = ExecutionStep(schema="analytics", model="summary")
        assert step.key == ("analytics", "summary")

    def test_qualified_name(self):
        """Test qualified_name property."""
        step = ExecutionStep(schema="dw", model="fact_sales")
        assert step.qualified_name == "dw.fact_sales"

    def test_repr(self):
        """Test string representation."""
        step = ExecutionStep(schema="p", model="m", order=5)
        assert "5" in repr(step)
        assert "p.m" in repr(step)


class TestExecutionPlan:
    """Tests for ExecutionPlan dataclass."""

    def test_empty_plan(self):
        """Test empty execution plan."""
        plan = ExecutionPlan()
        assert len(plan) == 0
        assert plan.is_empty is True

    def test_plan_with_steps(self):
        """Test plan with multiple steps."""
        steps = [
            ExecutionStep(schema="p", model="a", order=0),
            ExecutionStep(schema="p", model="b", order=1),
            ExecutionStep(schema="p", model="c", order=2),
        ]
        plan = ExecutionPlan(steps=steps)

        assert len(plan) == 3
        assert plan.is_empty is False

    def test_iteration(self):
        """Test iterating over plan."""
        steps = [
            ExecutionStep(schema="p", model="a", order=0),
            ExecutionStep(schema="p", model="b", order=1),
        ]
        plan = ExecutionPlan(steps=steps)

        models = [step.model for step in plan]
        assert models == ["a", "b"]

    def test_indexing(self):
        """Test accessing steps by index."""
        steps = [
            ExecutionStep(schema="p", model="a", order=0),
            ExecutionStep(schema="p", model="b", order=1),
        ]
        plan = ExecutionPlan(steps=steps)

        assert plan[0].model == "a"
        assert plan[1].model == "b"

    def test_get_step(self):
        """Test getting step by schema and model."""
        steps = [
            ExecutionStep(schema="public", model="orders", order=0),
            ExecutionStep(schema="analytics", model="summary", order=1),
        ]
        plan = ExecutionPlan(steps=steps)

        step = plan.get_step("analytics", "summary")
        assert step is not None
        assert step.model == "summary"

    def test_get_step_not_found(self):
        """Test get_step returns None for unknown model."""
        plan = ExecutionPlan()
        assert plan.get_step("p", "unknown") is None

    def test_get_order(self):
        """Test getting execution order for a model."""
        steps = [
            ExecutionStep(schema="p", model="a", order=0),
            ExecutionStep(schema="p", model="b", order=1),
            ExecutionStep(schema="p", model="c", order=2),
        ]
        plan = ExecutionPlan(steps=steps)

        assert plan.get_order("p", "a") == 0
        assert plan.get_order("p", "b") == 1
        assert plan.get_order("p", "c") == 2
        assert plan.get_order("p", "unknown") is None

    def test_get_keys(self):
        """Test getting all keys in order."""
        steps = [
            ExecutionStep(schema="p", model="a", order=0),
            ExecutionStep(schema="q", model="b", order=1),
        ]
        plan = ExecutionPlan(steps=steps)

        keys = plan.get_keys()
        assert keys == [("p", "a"), ("q", "b")]

    def test_get_configs(self):
        """Test getting all configs in order."""
        config_a = MagicMock(spec=ConfigParser)
        config_b = MagicMock(spec=ConfigParser)
        steps = [
            ExecutionStep(schema="p", model="a", config=config_a, order=0),
            ExecutionStep(schema="p", model="b", config=config_b, order=1),
        ]
        plan = ExecutionPlan(steps=steps)

        configs = plan.get_configs()
        assert configs == [config_a, config_b]


class TestExecutionPlanError:
    """Tests for ExecutionPlanError exception."""

    def test_creation(self):
        """Test creating ExecutionPlanError."""
        error = ExecutionPlanError(
            "Test error",
            affected_models=[("p", "a"), ("p", "b")],
        )

        assert error.message == "Test error"
        assert len(error.affected_models) == 2

    def test_to_transformation_error(self):
        """Test conversion to TransformationError."""
        error = ExecutionPlanError("Cycle detected")
        te = error.to_transformation_error("test_model")

        assert te.model_name == "test_model"
        assert "Cycle detected" in te.error_message


class TestExecutionPlannerInit:
    """Tests for ExecutionPlanner initialization."""

    def test_init_with_dag(self):
        """Test initializing with a DAG."""
        dag = nx.DiGraph()
        dag.add_node(("p", "a"))

        planner = ExecutionPlanner(dag)
        assert planner.dag is dag


class TestExecutionPlannerCreatePlan:
    """Tests for ExecutionPlanner.create_plan()."""

    def test_empty_dag(self):
        """Test creating plan from empty DAG."""
        dag = nx.DiGraph()
        planner = ExecutionPlanner(dag)

        plan = planner.create_plan()
        assert len(plan) == 0
        assert plan.is_empty is True

    def test_single_node(self):
        """Test creating plan with single node."""
        dag = nx.DiGraph()
        dag.add_node(("p", "a"), config=None)

        planner = ExecutionPlanner(dag)
        plan = planner.create_plan()

        assert len(plan) == 1
        assert plan[0].key == ("p", "a")

    def test_linear_chain(self):
        """Test creating plan from linear dependency chain."""
        dag = nx.DiGraph()
        dag.add_node(("p", "a"), config=MagicMock())
        dag.add_node(("p", "b"), config=MagicMock())
        dag.add_node(("p", "c"), config=MagicMock())
        # b depends on a, c depends on b
        dag.add_edge(("p", "b"), ("p", "a"))
        dag.add_edge(("p", "c"), ("p", "b"))

        planner = ExecutionPlanner(dag)
        plan = planner.create_plan()

        keys = plan.get_keys()
        # a must come before b, b must come before c
        assert keys.index(("p", "a")) < keys.index(("p", "b"))
        assert keys.index(("p", "b")) < keys.index(("p", "c"))

    def test_diamond_dependency(self):
        """Test creating plan with diamond dependency pattern."""
        #     a
        #    / \
        #   b   c
        #    \ /
        #     d
        dag = nx.DiGraph()
        for node in [("p", "a"), ("p", "b"), ("p", "c"), ("p", "d")]:
            dag.add_node(node, config=MagicMock())
        dag.add_edge(("p", "b"), ("p", "a"))
        dag.add_edge(("p", "c"), ("p", "a"))
        dag.add_edge(("p", "d"), ("p", "b"))
        dag.add_edge(("p", "d"), ("p", "c"))

        planner = ExecutionPlanner(dag)
        plan = planner.create_plan()

        keys = plan.get_keys()
        # a must come first, d must come last
        assert keys.index(("p", "a")) < keys.index(("p", "b"))
        assert keys.index(("p", "a")) < keys.index(("p", "c"))
        assert keys.index(("p", "b")) < keys.index(("p", "d"))
        assert keys.index(("p", "c")) < keys.index(("p", "d"))

    def test_preserves_config_references(self):
        """Test that ConfigParser references are preserved."""
        config_a = MagicMock(spec=ConfigParser)
        config_b = MagicMock(spec=ConfigParser)

        dag = nx.DiGraph()
        dag.add_node(("p", "a"), config=config_a)
        dag.add_node(("p", "b"), config=config_b)
        dag.add_edge(("p", "b"), ("p", "a"))

        planner = ExecutionPlanner(dag)
        plan = planner.create_plan()

        step_a = plan.get_step("p", "a")
        step_b = plan.get_step("p", "b")

        assert step_a.config is config_a
        assert step_b.config is config_b

    def test_preserves_metadata(self):
        """Test that node metadata is preserved."""
        dag = nx.DiGraph()
        dag.add_node(
            ("p", "a"),
            config=None,
            file_path="/test.yaml",
            qualified_name="p.a",
        )

        planner = ExecutionPlanner(dag)
        plan = planner.create_plan()

        step = plan[0]
        assert step.metadata["file_path"] == "/test.yaml"
        assert step.metadata["qualified_name"] == "p.a"

    def test_execution_order_numbers(self):
        """Test that execution order numbers are correct."""
        dag = nx.DiGraph()
        dag.add_node(("p", "a"), config=None)
        dag.add_node(("p", "b"), config=None)
        dag.add_node(("p", "c"), config=None)
        dag.add_edge(("p", "b"), ("p", "a"))
        dag.add_edge(("p", "c"), ("p", "b"))

        planner = ExecutionPlanner(dag)
        plan = planner.create_plan()

        for i, step in enumerate(plan):
            assert step.order == i

    def test_cycle_raises_error(self):
        """Test that cycle raises ExecutionPlanError."""
        dag = nx.DiGraph()
        dag.add_node(("p", "a"), config=None)
        dag.add_node(("p", "b"), config=None)
        dag.add_edge(("p", "a"), ("p", "b"))
        dag.add_edge(("p", "b"), ("p", "a"))

        planner = ExecutionPlanner(dag)

        with pytest.raises(ExecutionPlanError) as exc_info:
            planner.create_plan()

        assert "cycles" in exc_info.value.message.lower()
        assert len(exc_info.value.affected_models) > 0


class TestExecutionPlannerValidate:
    """Tests for ExecutionPlanner.validate()."""

    def test_valid_dag(self):
        """Test validation passes for valid DAG."""
        dag = nx.DiGraph()
        dag.add_node(("p", "a"))
        dag.add_node(("p", "b"))
        dag.add_edge(("p", "b"), ("p", "a"))

        planner = ExecutionPlanner(dag)
        assert planner.validate() is True

    def test_invalid_dag_with_cycle(self):
        """Test validation fails for DAG with cycle."""
        dag = nx.DiGraph()
        dag.add_node(("p", "a"))
        dag.add_node(("p", "b"))
        dag.add_edge(("p", "a"), ("p", "b"))
        dag.add_edge(("p", "b"), ("p", "a"))

        planner = ExecutionPlanner(dag)

        with pytest.raises(ExecutionPlanError):
            planner.validate()


class TestExecutionPlannerGetExecutionOrder:
    """Tests for get_execution_order method."""

    def test_returns_key_list(self):
        """Test that get_execution_order returns keys."""
        dag = nx.DiGraph()
        dag.add_node(("p", "a"), config=None)
        dag.add_node(("p", "b"), config=None)
        dag.add_edge(("p", "b"), ("p", "a"))

        planner = ExecutionPlanner(dag)
        order = planner.get_execution_order()

        assert isinstance(order, list)
        assert all(isinstance(k, tuple) for k in order)
        assert order.index(("p", "a")) < order.index(("p", "b"))


class TestConvenienceFunctions:
    """Tests for module-level convenience functions."""

    def test_create_execution_plan(self):
        """Test create_execution_plan function."""
        dag = nx.DiGraph()
        dag.add_node(("p", "a"), config=None)

        plan = create_execution_plan(dag)
        assert isinstance(plan, ExecutionPlan)
        assert len(plan) == 1

    def test_get_execution_order(self):
        """Test get_execution_order function."""
        dag = nx.DiGraph()
        dag.add_node(("p", "a"), config=None)
        dag.add_node(("p", "b"), config=None)
        dag.add_edge(("p", "b"), ("p", "a"))

        order = get_execution_order(dag)
        assert order == [("p", "a"), ("p", "b")]


class TestIntegrationWithDAGBuilder:
    """Integration tests with DAGBuilder."""

    def setup_method(self):
        """Reset singletons before each test."""
        from backend.application.config_parser.model_registry import ModelRegistry
        from backend.application.config_parser.config_parser import ConfigParser

        ModelRegistry.reset_instance()
        ConfigParser._instances.clear()

    def _create_mock_config(self, schema, model, deps=None):
        """Create a mock ConfigParser."""
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

    def test_planner_with_dag_builder_output(self):
        """Test ExecutionPlanner works with DAGBuilder output."""
        from backend.application.config_parser.dag_builder import DAGBuilder

        a = self._create_mock_config("p", "a")
        b = self._create_mock_config("p", "b", deps=[{"schema": "p", "model": "a"}])
        c = self._create_mock_config("p", "c", deps=[{"schema": "p", "model": "b"}])

        builder = DAGBuilder(configs=[a, b, c])
        dag = builder.build()

        planner = ExecutionPlanner(dag)
        plan = planner.create_plan()

        assert len(plan) == 3
        keys = plan.get_keys()
        assert keys.index(("p", "a")) < keys.index(("p", "b"))
        assert keys.index(("p", "b")) < keys.index(("p", "c"))

    def test_planner_preserves_configs_from_builder(self):
        """Test that configs from DAGBuilder are preserved in plan."""
        from backend.application.config_parser.dag_builder import DAGBuilder

        config_a = self._create_mock_config("p", "a")
        config_b = self._create_mock_config("p", "b", deps=[{"schema": "p", "model": "a"}])

        builder = DAGBuilder(configs=[config_a, config_b])
        dag = builder.build()

        planner = ExecutionPlanner(dag)
        plan = planner.create_plan()

        # Configs should be accessible from plan
        step_a = plan.get_step("p", "a")
        step_b = plan.get_step("p", "b")

        assert step_a.config is config_a
        assert step_b.config is config_b
