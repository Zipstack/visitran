"""
Incremental Cycle Detection for DAG Construction.

This module provides cycle detection that validates DAG integrity after each
edge addition. When a cycle is detected, it reports the full cycle path with
YAML source locations for each participating model.

Usage:
    detector = CycleDetector(dag)

    # Check before adding an edge
    detector.check_edge_would_create_cycle(from_node, to_node)

    # Or add edge with automatic checking
    detector.add_edge_with_check(from_node, to_node, node_metadata)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

import networkx as nx

from visitran.errors import TransformationError

if TYPE_CHECKING:
    from backend.application.config_parser.config_parser import ConfigParser

logger = logging.getLogger(__name__)


@dataclass
class CycleNodeInfo:
    """
    Information about a node participating in a cycle.

    Attributes:
        schema: The schema name
        model: The model name
        file_path: Path to the YAML file defining this model
        line_number: Line number where the model is defined (1-based)
    """

    schema: str
    model: str
    file_path: Optional[str] = None
    line_number: Optional[int] = None

    @property
    def key(self) -> tuple[str, str]:
        """Return the (schema, model) key."""
        return (self.schema, self.model)

    @property
    def qualified_name(self) -> str:
        """Return schema.model qualified name."""
        return f"{self.schema}.{self.model}"

    @property
    def location_str(self) -> str:
        """Return file:line string for error messages."""
        if self.file_path and self.line_number:
            return f"{self.file_path}:{self.line_number}"
        elif self.file_path:
            return str(self.file_path)
        return "unknown location"


@dataclass
class CycleInfo:
    """
    Complete information about a detected cycle.

    Attributes:
        cycle_path: Ordered list of nodes in the cycle
        trigger_edge: The edge that created the cycle (from, to)
        nodes_info: Detailed info for each node in the cycle
    """

    cycle_path: list[tuple[str, str]]
    trigger_edge: tuple[tuple[str, str], tuple[str, str]]
    nodes_info: list[CycleNodeInfo] = field(default_factory=list)

    @property
    def cycle_str(self) -> str:
        """Return formatted cycle path string with arrows."""
        names = [f"{s}.{m}" for s, m in self.cycle_path]
        return " → ".join(names)

    @property
    def model_names(self) -> list[str]:
        """Return list of model qualified names in cycle."""
        return [f"{s}.{m}" for s, m in self.cycle_path]

    def to_transformation_error(
        self,
        model_name: Optional[str] = None,
    ) -> TransformationError:
        """
        Convert cycle info to TransformationError.

        Args:
            model_name: Optional model name for the error

        Returns:
            TransformationError with full cycle details
        """
        # Use the first model in cycle if not specified
        if model_name is None:
            model_name = self.cycle_path[0][1] if self.cycle_path else "unknown"

        # Build detailed error message
        message_parts = [
            f"Circular dependency detected: {self.cycle_str}",
            "",
            "Models in cycle:",
        ]

        for info in self.nodes_info:
            message_parts.append(f"  - {info.qualified_name} ({info.location_str})")

        message_parts.extend([
            "",
            f"The cycle was created when adding dependency: "
            f"{self.trigger_edge[0][0]}.{self.trigger_edge[0][1]} → "
            f"{self.trigger_edge[1][0]}.{self.trigger_edge[1][1]}",
        ])

        error_message = "\n".join(message_parts)

        # Get line number from trigger node if available
        trigger_info = next(
            (n for n in self.nodes_info if n.key == self.trigger_edge[0]),
            None,
        )

        return TransformationError(
            model_name=model_name,
            transformation_id=None,
            error_message=error_message,
            line_number=trigger_info.line_number if trigger_info else None,
            column_number=None,
            yaml_snippet=None,
        )


class CycleDetector:
    """
    Incremental cycle detector for DAG construction.

    This class wraps a NetworkX DiGraph and provides methods to detect
    cycles immediately when edges are added, rather than after full
    graph construction.

    The detector can be used in two modes:
    1. Check-only mode: Use check_edge_would_create_cycle() before adding
    2. Add-and-check mode: Use add_edge_with_check() for atomic add+check

    Attributes:
        _dag: The NetworkX DiGraph being monitored
        _node_metadata: Metadata for each node (schema, model, file, line)
    """

    def __init__(self, dag: Optional[nx.DiGraph] = None) -> None:
        """
        Initialize the cycle detector.

        Args:
            dag: Optional existing DiGraph to monitor. Creates new if None.
        """
        self._dag = dag if dag is not None else nx.DiGraph()
        self._node_metadata: dict[tuple[str, str], dict[str, Any]] = {}

    @property
    def dag(self) -> nx.DiGraph:
        """Return the underlying DAG."""
        return self._dag

    def set_node_metadata(
        self,
        node: tuple[str, str],
        file_path: Optional[str] = None,
        line_number: Optional[int] = None,
        config: Optional[ConfigParser] = None,
        **extra: Any,
    ) -> None:
        """
        Set metadata for a node.

        Args:
            node: The (schema, model) node key
            file_path: Path to the YAML file
            line_number: Line number in the YAML file
            config: Optional ConfigParser for extracting metadata
            **extra: Additional metadata to store
        """
        metadata = {
            "file_path": file_path,
            "line_number": line_number,
            **extra,
        }

        # Extract metadata from ConfigParser if provided
        if config is not None:
            if file_path is None and hasattr(config, "yaml_content"):
                # Try to get file path from config
                pass  # ConfigParser doesn't store file path directly
            # Could extract line numbers for specific transformations here

        self._node_metadata[node] = metadata

    def get_node_metadata(self, node: tuple[str, str]) -> dict[str, Any]:
        """Get metadata for a node."""
        return self._node_metadata.get(node, {})

    def check_edge_would_create_cycle(
        self,
        from_node: tuple[str, str],
        to_node: tuple[str, str],
    ) -> Optional[CycleInfo]:
        """
        Check if adding an edge would create a cycle WITHOUT modifying the graph.

        This performs a path check: if there's already a path from to_node
        to from_node, adding from_node -> to_node would create a cycle.

        Args:
            from_node: Source node (schema, model) - the dependent model
            to_node: Target node (schema, model) - the dependency

        Returns:
            CycleInfo if adding the edge would create a cycle, None otherwise
        """
        # Self-loop check
        if from_node == to_node:
            return self._create_cycle_info(
                cycle_path=[from_node, from_node],
                trigger_edge=(from_node, to_node),
            )

        # If to_node can already reach from_node, adding this edge creates cycle
        if to_node in self._dag and from_node in self._dag:
            if nx.has_path(self._dag, to_node, from_node):
                # Find the path that would form the cycle
                path = nx.shortest_path(self._dag, to_node, from_node)
                # The cycle is: from_node -> to_node -> ... -> from_node
                cycle_path = [from_node] + path
                return self._create_cycle_info(
                    cycle_path=cycle_path,
                    trigger_edge=(from_node, to_node),
                )

        return None

    def add_edge_with_check(
        self,
        from_node: tuple[str, str],
        to_node: tuple[str, str],
        from_metadata: Optional[dict[str, Any]] = None,
        to_metadata: Optional[dict[str, Any]] = None,
        raise_on_cycle: bool = True,
    ) -> Optional[CycleInfo]:
        """
        Add an edge to the DAG with immediate cycle checking.

        This method:
        1. Ensures both nodes exist in the graph
        2. Checks if the edge would create a cycle
        3. If cycle detected and raise_on_cycle=True, raises TransformationError
        4. If no cycle, adds the edge

        Args:
            from_node: Source node (schema, model) - the dependent model
            to_node: Target node (schema, model) - the dependency
            from_metadata: Optional metadata for the source node
            to_metadata: Optional metadata for the target node
            raise_on_cycle: If True, raise TransformationError on cycle

        Returns:
            CycleInfo if cycle detected and raise_on_cycle=False, else None

        Raises:
            TransformationError: If cycle detected and raise_on_cycle=True
        """
        # Ensure nodes exist
        if from_node not in self._dag:
            self._dag.add_node(from_node)
        if to_node not in self._dag:
            self._dag.add_node(to_node)

        # Store metadata
        if from_metadata:
            self._node_metadata[from_node] = {
                **self._node_metadata.get(from_node, {}),
                **from_metadata,
            }
        if to_metadata:
            self._node_metadata[to_node] = {
                **self._node_metadata.get(to_node, {}),
                **to_metadata,
            }

        # Check for cycle
        cycle_info = self.check_edge_would_create_cycle(from_node, to_node)

        if cycle_info is not None:
            if raise_on_cycle:
                raise cycle_info.to_transformation_error()
            return cycle_info

        # No cycle - add the edge
        self._dag.add_edge(from_node, to_node)
        return None

    def add_node(
        self,
        node: tuple[str, str],
        file_path: Optional[str] = None,
        line_number: Optional[int] = None,
        **extra: Any,
    ) -> None:
        """
        Add a node to the DAG with metadata.

        Args:
            node: The (schema, model) node key
            file_path: Path to the YAML file
            line_number: Line number in the YAML file
            **extra: Additional metadata
        """
        self._dag.add_node(node)
        self.set_node_metadata(node, file_path, line_number, **extra)

    def detect_all_cycles(self) -> list[CycleInfo]:
        """
        Detect all cycles in the current graph.

        This is a post-hoc check, not incremental. Use for validation
        after bulk operations.

        Returns:
            List of CycleInfo for each cycle found
        """
        cycles = []
        try:
            for cycle in nx.simple_cycles(self._dag):
                # simple_cycles returns cycles as lists of nodes
                # Add the first node again to complete the cycle
                cycle_path = list(cycle) + [cycle[0]]
                # For post-hoc detection, trigger edge is first -> last
                trigger_edge = (cycle[0], cycle[-1])
                cycles.append(self._create_cycle_info(cycle_path, trigger_edge))
        except nx.NetworkXNoCycle:
            pass
        return cycles

    def _create_cycle_info(
        self,
        cycle_path: list[tuple[str, str]],
        trigger_edge: tuple[tuple[str, str], tuple[str, str]],
    ) -> CycleInfo:
        """
        Create a CycleInfo with node metadata.

        Args:
            cycle_path: Ordered list of nodes in the cycle
            trigger_edge: The (from, to) edge that triggered detection

        Returns:
            CycleInfo with populated nodes_info
        """
        nodes_info = []
        seen = set()

        for node in cycle_path:
            if node in seen:
                continue  # Skip duplicate at end of cycle
            seen.add(node)

            metadata = self._node_metadata.get(node, {})
            info = CycleNodeInfo(
                schema=node[0],
                model=node[1],
                file_path=metadata.get("file_path"),
                line_number=metadata.get("line_number"),
            )
            nodes_info.append(info)

        return CycleInfo(
            cycle_path=cycle_path,
            trigger_edge=trigger_edge,
            nodes_info=nodes_info,
        )

    def clear(self) -> None:
        """Clear all nodes, edges, and metadata."""
        self._dag.clear()
        self._node_metadata.clear()


def check_would_create_cycle(
    dag: nx.DiGraph,
    from_node: tuple[str, str],
    to_node: tuple[str, str],
) -> bool:
    """
    Convenience function to check if adding an edge would create a cycle.

    Args:
        dag: The NetworkX DiGraph
        from_node: Source node (schema, model)
        to_node: Target node (schema, model)

    Returns:
        True if adding the edge would create a cycle
    """
    if from_node == to_node:
        return True

    if to_node in dag and from_node in dag:
        return nx.has_path(dag, to_node, from_node)

    return False


def find_cycle_path(
    dag: nx.DiGraph,
    from_node: tuple[str, str],
    to_node: tuple[str, str],
) -> Optional[list[tuple[str, str]]]:
    """
    Find the cycle path that would be created by adding an edge.

    Args:
        dag: The NetworkX DiGraph
        from_node: Source node (schema, model)
        to_node: Target node (schema, model)

    Returns:
        List of nodes forming the cycle if one would be created, None otherwise
    """
    if from_node == to_node:
        return [from_node, from_node]

    if to_node in dag and from_node in dag:
        try:
            path = nx.shortest_path(dag, to_node, from_node)
            return [from_node] + path
        except nx.NetworkXNoPath:
            pass

    return None
