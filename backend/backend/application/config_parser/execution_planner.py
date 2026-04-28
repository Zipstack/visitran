"""
Execution Planner for DAG-based Model Execution.

This module converts a validated dependency DAG into a topologically sorted
execution order. It preserves ConfigParser references for downstream
Ibis expression builders.

Usage:
    planner = ExecutionPlanner(dag)
    plan = planner.create_plan()

    for step in plan:
        print(f"Execute {step.schema}.{step.model}")
        config = step.config  # Access ConfigParser for SQL content
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Iterator, Optional

import networkx as nx

from visitran.errors import TransformationError

if TYPE_CHECKING:
    from backend.application.config_parser.config_parser import ConfigParser

logger = logging.getLogger(__name__)


@dataclass
class ExecutionStep:
    """
    Represents a single step in the execution plan.

    Attributes:
        schema: The schema name for the model
        model: The model name
        config: Reference to the ConfigParser instance
        order: The execution order (0-indexed position in plan)
        metadata: Additional metadata from the DAG node
    """

    schema: str
    model: str
    config: Optional[ConfigParser] = None
    order: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def key(self) -> tuple[str, str]:
        """Return the (schema, model) tuple key."""
        return (self.schema, self.model)

    @property
    def qualified_name(self) -> str:
        """Return schema.model qualified name."""
        return f"{self.schema}.{self.model}"

    def __repr__(self) -> str:
        return f"ExecutionStep({self.order}: {self.qualified_name})"


@dataclass
class ExecutionPlan:
    """
    A topologically sorted execution plan for models.

    The plan contains an ordered list of ExecutionSteps where all
    dependencies are executed before their dependent models.

    Attributes:
        steps: Ordered list of execution steps
        _index: Mapping from (schema, model) to step index
    """

    steps: list[ExecutionStep] = field(default_factory=list)
    _index: dict[tuple[str, str], int] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        """Build index after initialization."""
        self._rebuild_index()

    def _rebuild_index(self) -> None:
        """Rebuild the key-to-index mapping."""
        self._index = {step.key: i for i, step in enumerate(self.steps)}

    def __len__(self) -> int:
        """Return number of steps in the plan."""
        return len(self.steps)

    def __iter__(self) -> Iterator[ExecutionStep]:
        """Iterate over execution steps in order."""
        return iter(self.steps)

    def __getitem__(self, index: int) -> ExecutionStep:
        """Get step by index."""
        return self.steps[index]

    def get_step(self, schema: str, model: str) -> Optional[ExecutionStep]:
        """
        Get the execution step for a specific model.

        Args:
            schema: The schema name
            model: The model name

        Returns:
            The ExecutionStep if found, None otherwise
        """
        idx = self._index.get((schema, model))
        if idx is not None:
            return self.steps[idx]
        return None

    def get_order(self, schema: str, model: str) -> Optional[int]:
        """
        Get the execution order for a model.

        Args:
            schema: The schema name
            model: The model name

        Returns:
            The order index (0-based) if found, None otherwise
        """
        return self._index.get((schema, model))

    def get_keys(self) -> list[tuple[str, str]]:
        """Return all (schema, model) keys in execution order."""
        return [step.key for step in self.steps]

    def get_configs(self) -> list[Optional[ConfigParser]]:
        """Return all ConfigParser references in execution order."""
        return [step.config for step in self.steps]

    @property
    def is_empty(self) -> bool:
        """Check if the plan is empty."""
        return len(self.steps) == 0


class ExecutionPlanError(Exception):
    """
    Raised when execution plan creation fails.

    Attributes:
        message: Description of the error
        affected_models: List of models involved in the error
    """

    def __init__(
        self,
        message: str,
        affected_models: Optional[list[tuple[str, str]]] = None,
    ):
        self.message = message
        self.affected_models = affected_models or []
        super().__init__(self.message)

    def to_transformation_error(self, model_name: str = "unknown") -> TransformationError:
        """Convert to TransformationError for consistent error handling."""
        return TransformationError(
            model_name=model_name,
            transformation_id=None,
            error_message=self.message,
            line_number=None,
            column_number=None,
            yaml_snippet=None,
        )


class ExecutionPlanner:
    """
    Creates topologically sorted execution plans from dependency DAGs.

    The planner accepts a NetworkX DiGraph (from DAGBuilder) and produces
    an ExecutionPlan containing the models in execution order.

    Attributes:
        _dag: The dependency DAG to plan
    """

    def __init__(self, dag: nx.DiGraph) -> None:
        """
        Initialize the planner with a DAG.

        Args:
            dag: NetworkX DiGraph with model dependencies.
                 Nodes should be (schema, model) tuples with
                 'config' attribute containing ConfigParser.
        """
        self._dag = dag

    @property
    def dag(self) -> nx.DiGraph:
        """Return the underlying DAG."""
        return self._dag

    def create_plan(self) -> ExecutionPlan:
        """
        Create an execution plan from the DAG.

        Returns:
            ExecutionPlan with steps in dependency-respecting order

        Raises:
            ExecutionPlanError: If the graph contains cycles or is invalid
        """
        if self._dag.number_of_nodes() == 0:
            logger.debug("Empty DAG, returning empty execution plan")
            return ExecutionPlan(steps=[])

        try:
            # Get topological order (dependencies first)
            # nx.topological_sort returns nodes with no dependencies first
            sorted_nodes = list(nx.topological_sort(self._dag))

            # Reverse so dependencies come before dependents
            # (NetworkX returns nodes in reverse topological order for our edge direction)
            sorted_nodes = list(reversed(sorted_nodes))

            # Build execution steps
            steps = []
            for order, node_key in enumerate(sorted_nodes):
                node_data = self._dag.nodes[node_key]

                step = ExecutionStep(
                    schema=node_key[0],
                    model=node_key[1],
                    config=node_data.get("config"),
                    order=order,
                    metadata={
                        k: v for k, v in node_data.items()
                        if k != "config"
                    },
                )
                steps.append(step)

            plan = ExecutionPlan(steps=steps)
            logger.info(f"Created execution plan with {len(plan)} steps")
            return plan

        except nx.NetworkXUnfeasible as e:
            # Graph has a cycle
            affected = self._find_cycle_models()
            raise ExecutionPlanError(
                f"Cannot create execution plan: graph contains cycles. "
                f"Affected models: {', '.join(f'{s}.{m}' for s, m in affected)}",
                affected_models=affected,
            ) from e

        except nx.NetworkXError as e:
            # Other graph error
            raise ExecutionPlanError(
                f"Cannot create execution plan: {str(e)}",
            ) from e

    def _find_cycle_models(self) -> list[tuple[str, str]]:
        """
        Find models involved in cycles.

        Returns:
            List of (schema, model) tuples forming cycles
        """
        try:
            cycle = nx.find_cycle(self._dag)
            return [edge[0] for edge in cycle]
        except nx.NetworkXNoCycle:
            return []

    def validate(self) -> bool:
        """
        Validate that the DAG can produce an execution plan.

        Returns:
            True if the DAG is valid (acyclic)

        Raises:
            ExecutionPlanError: If the DAG contains cycles
        """
        if not nx.is_directed_acyclic_graph(self._dag):
            affected = self._find_cycle_models()
            raise ExecutionPlanError(
                "DAG contains cycles and cannot be executed",
                affected_models=affected,
            )
        return True

    def get_execution_order(self) -> list[tuple[str, str]]:
        """
        Get just the execution order without full plan.

        This is a convenience method when only the order is needed.

        Returns:
            List of (schema, model) tuples in execution order
        """
        plan = self.create_plan()
        return plan.get_keys()


def create_execution_plan(dag: nx.DiGraph) -> ExecutionPlan:
    """
    Convenience function to create an execution plan from a DAG.

    Args:
        dag: NetworkX DiGraph with model dependencies

    Returns:
        ExecutionPlan with steps in dependency-respecting order

    Raises:
        ExecutionPlanError: If the graph contains cycles
    """
    planner = ExecutionPlanner(dag)
    return planner.create_plan()


def get_execution_order(dag: nx.DiGraph) -> list[tuple[str, str]]:
    """
    Convenience function to get execution order from a DAG.

    Args:
        dag: NetworkX DiGraph with model dependencies

    Returns:
        List of (schema, model) tuples in execution order

    Raises:
        ExecutionPlanError: If the graph contains cycles
    """
    planner = ExecutionPlanner(dag)
    return planner.get_execution_order()
