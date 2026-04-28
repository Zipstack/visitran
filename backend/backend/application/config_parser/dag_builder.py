"""
DAG Builder for Model Dependencies.

This module provides a two-pass DAG builder that constructs a NetworkX DiGraph
representing model dependencies. It integrates with ModelRegistry for model
storage and ConfigParser for YAML configuration parsing.

Usage:
    builder = DAGBuilder(yaml_paths, registry)
    dag = builder.build()  # Executes both passes and returns the DAG

    # Or manually:
    builder.execute_pass_one()  # Register all models
    builder.execute_pass_two()  # Build dependency graph
    dag = builder.get_dag()
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

import networkx as nx

from visitran.errors import TransformationError

if TYPE_CHECKING:
    from backend.application.config_parser.config_parser import ConfigParser
    from backend.application.config_parser.model_registry import ModelRegistry
    from backend.application.config_parser.cycle_detector import CycleDetector, CycleInfo

logger = logging.getLogger(__name__)


@dataclass
class ModelNode:
    """
    Represents a model node in the dependency DAG.

    Attributes:
        schema: The schema name for the model
        model: The model name
        config: Reference to the ConfigParser instance
        file_path: Path to the source YAML file (if loaded from file)
        dependencies: List of (schema, model) tuples this model depends on
    """

    schema: str
    model: str
    config: Optional[ConfigParser] = None
    file_path: Optional[Path] = None
    dependencies: list[tuple[str, str]] = field(default_factory=list)

    @property
    def key(self) -> tuple[str, str]:
        """Return the (schema, model) tuple key."""
        return (self.schema, self.model)

    @property
    def qualified_name(self) -> str:
        """Return the schema.model qualified name."""
        return f"{self.schema}.{self.model}"


@dataclass
class DAGBuildError:
    """
    Represents an error encountered during DAG construction.

    Attributes:
        message: Human-readable error description
        file_path: Path to the file where error occurred
        line_number: Line number in the file (1-based)
        column_number: Column number in the file (1-based)
        model_key: The (schema, model) key if applicable
    """

    message: str
    file_path: Optional[Path] = None
    line_number: Optional[int] = None
    column_number: Optional[int] = None
    model_key: Optional[tuple[str, str]] = None

    def to_transformation_error(self, model_name: str = "unknown") -> TransformationError:
        """Convert to TransformationError for consistent error handling."""
        return TransformationError(
            model_name=model_name,
            transformation_id=None,
            error_message=self.message,
            line_number=self.line_number,
            column_number=self.column_number,
            yaml_snippet=None,
        )


class CycleDetectedError(Exception):
    """
    Raised when a cycle is detected in the dependency graph.

    Attributes:
        cycle: List of (schema, model) tuples forming the cycle
        message: Human-readable description of the cycle
    """

    def __init__(self, cycle: list[tuple[str, str]], message: Optional[str] = None):
        self.cycle = cycle
        cycle_str = " -> ".join(f"{s}.{m}" for s, m in cycle)
        self.message = message or f"Circular dependency detected: {cycle_str}"
        super().__init__(self.message)


class MissingDependencyError(Exception):
    """
    Raised when a model references a dependency that doesn't exist.

    Attributes:
        model_key: The (schema, model) of the model with the missing dependency
        dependency_key: The (schema, model) of the missing dependency
        file_path: Path to the file containing the invalid reference
    """

    def __init__(
        self,
        model_key: tuple[str, str],
        dependency_key: tuple[str, str],
        file_path: Optional[Path] = None,
    ):
        self.model_key = model_key
        self.dependency_key = dependency_key
        self.file_path = file_path
        model_str = f"{model_key[0]}.{model_key[1]}"
        dep_str = f"{dependency_key[0]}.{dependency_key[1]}"
        file_info = f" in {file_path}" if file_path else ""
        self.message = f"Model '{model_str}' references undefined dependency '{dep_str}'{file_info}"
        super().__init__(self.message)


class DAGBuilder:
    """
    Two-pass DAG builder for model dependencies.

    This class implements a two-pass strategy for constructing a dependency DAG:
    - Pass 1: Register all models in the ModelRegistry
    - Pass 2: Build the NetworkX DiGraph with dependency edges

    The builder supports multiple input sources:
    - YAML file paths
    - Pre-parsed ConfigParser instances
    - Dictionary configurations

    Attributes:
        _registry: The ModelRegistry instance for model storage
        _dag: NetworkX DiGraph representing dependencies
        _yaml_paths: List of YAML file paths to process
        _configs: List of pre-parsed ConfigParser instances
        _model_nodes: Dictionary mapping (schema, model) to ModelNode
        _errors: List of errors encountered during building
    """

    def __init__(
        self,
        yaml_paths: Optional[list[Union[str, Path]]] = None,
        registry: Optional[ModelRegistry] = None,
        configs: Optional[list[ConfigParser]] = None,
    ) -> None:
        """
        Initialize the DAG builder.

        Args:
            yaml_paths: List of paths to YAML configuration files
            registry: ModelRegistry instance (creates new if not provided)
            configs: List of pre-parsed ConfigParser instances
        """
        # Import here to avoid circular imports
        from backend.application.config_parser.model_registry import ModelRegistry

        self._registry = registry or ModelRegistry()
        self._dag: nx.DiGraph = nx.DiGraph()
        self._yaml_paths: list[Path] = []
        self._configs: list[ConfigParser] = configs or []
        self._model_nodes: dict[tuple[str, str], ModelNode] = {}
        self._errors: list[DAGBuildError] = []
        self._pass_one_complete: bool = False
        self._pass_two_complete: bool = False

        # Convert string paths to Path objects
        if yaml_paths:
            self._yaml_paths = [Path(p) if isinstance(p, str) else p for p in yaml_paths]

    @property
    def dag(self) -> nx.DiGraph:
        """Return the constructed DAG."""
        return self._dag

    @property
    def registry(self) -> ModelRegistry:
        """Return the ModelRegistry instance."""
        return self._registry

    @property
    def errors(self) -> list[DAGBuildError]:
        """Return list of errors encountered during building."""
        return self._errors.copy()

    @property
    def model_count(self) -> int:
        """Return the number of models in the DAG."""
        return len(self._model_nodes)

    def get_dag(self) -> nx.DiGraph:
        """
        Get the constructed DAG.

        Returns:
            The NetworkX DiGraph representing model dependencies

        Raises:
            RuntimeError: If build() hasn't been called yet
        """
        if not self._pass_two_complete:
            raise RuntimeError(
                "DAG not yet constructed. Call build() or execute_pass_two() first."
            )
        return self._dag

    def build(
        self,
        validate_cycles: bool = True,
        strict: bool = True,
        incremental_cycle_check: bool = True,
    ) -> nx.DiGraph:
        """
        Execute both passes and return the constructed DAG.

        This is the main entry point for DAG construction. It performs:
        1. Pass 1: Model registration
        2. Pass 2: Graph construction with optional incremental cycle detection
        3. Optional post-hoc cycle validation (if not using incremental detection)

        Args:
            validate_cycles: If True, check for and report cycles
            strict: If True, raise on errors; otherwise collect errors
            incremental_cycle_check: If True, check for cycles during edge addition

        Returns:
            The constructed NetworkX DiGraph

        Raises:
            CycleDetectedError: If cycles are detected via post-hoc validation
            TransformationError: If cycles are detected incrementally, or YAML fails
            MissingDependencyError: If dependencies reference undefined models
        """
        self.execute_pass_one(strict=strict)
        self.execute_pass_two(strict=strict, incremental_cycle_check=incremental_cycle_check)

        # Only run post-hoc validation if incremental checking is disabled
        # and validate_cycles is requested
        if validate_cycles and not incremental_cycle_check:
            self.validate_no_cycles()

        return self._dag

    def execute_pass_one(self, strict: bool = True) -> None:
        """
        Pass 1: Register all models in the ModelRegistry.

        Iterates through YAML files and pre-parsed configs, creating
        ConfigParser instances and registering them in the ModelRegistry.

        Args:
            strict: If True, raise on first error; otherwise collect errors

        Raises:
            TransformationError: If YAML parsing fails (in strict mode)
            FileNotFoundError: If a YAML file doesn't exist (in strict mode)
        """
        if self._pass_one_complete:
            logger.warning("Pass 1 already completed, skipping re-execution")
            return

        # Process YAML files
        for yaml_path in self._yaml_paths:
            try:
                self._process_yaml_file(yaml_path)
            except Exception as e:
                error = DAGBuildError(
                    message=str(e),
                    file_path=yaml_path,
                )
                self._errors.append(error)
                if strict:
                    raise

        # Process pre-parsed configs
        for config in self._configs:
            try:
                self._register_config(config)
            except Exception as e:
                error = DAGBuildError(
                    message=str(e),
                    model_key=(
                        config.destination_schema_name,
                        config.model_name,
                    ),
                )
                self._errors.append(error)
                if strict:
                    raise

        self._pass_one_complete = True
        logger.info(f"Pass 1 complete: {len(self._model_nodes)} models registered")

    def execute_pass_two(
        self,
        strict: bool = True,
        incremental_cycle_check: bool = True,
    ) -> None:
        """
        Pass 2: Build the dependency graph.

        Creates NetworkX DiGraph nodes for each model and edges
        for their dependencies.

        Args:
            strict: If True, raise on missing dependencies; otherwise collect errors
            incremental_cycle_check: If True, check for cycles after each edge addition

        Raises:
            RuntimeError: If Pass 1 hasn't been executed
            MissingDependencyError: If a dependency doesn't exist (in strict mode)
            TransformationError: If a cycle is detected during edge addition
        """
        if not self._pass_one_complete:
            raise RuntimeError("Pass 1 must be executed before Pass 2")

        if self._pass_two_complete:
            logger.warning("Pass 2 already completed, skipping re-execution")
            return

        # Import CycleDetector here to avoid circular imports
        from backend.application.config_parser.cycle_detector import CycleDetector

        # Create cycle detector if incremental checking enabled
        cycle_detector: Optional[CycleDetector] = None
        if incremental_cycle_check:
            cycle_detector = CycleDetector(self._dag)

        # Create nodes for all models
        for key, node in self._model_nodes.items():
            node_metadata = {
                "schema": node.schema,
                "model": node.model,
                "config": node.config,
                "file_path": str(node.file_path) if node.file_path else None,
                "qualified_name": node.qualified_name,
            }
            self._dag.add_node(key, **node_metadata)

            # Also register with cycle detector for metadata tracking
            if cycle_detector:
                cycle_detector.set_node_metadata(
                    key,
                    file_path=str(node.file_path) if node.file_path else None,
                    line_number=None,  # Could be extracted from config if available
                )

        # Create edges for dependencies
        for key, node in self._model_nodes.items():
            # Get explicit dependencies (from reference field, etc.)
            all_dependencies = list(node.dependencies)

            # Now that all models are registered, extract JOIN/UNION dependencies
            if node.config:
                # Extract tables from JOIN transformations
                join_tables = self._extract_join_tables(node.config)
                for dep_tuple in join_tables:
                    if dep_tuple not in all_dependencies and self._is_known_model(dep_tuple):
                        all_dependencies.append(dep_tuple)

                # Extract tables from UNION transformations
                union_tables = self._extract_union_tables(node.config)
                for dep_tuple in union_tables:
                    if dep_tuple not in all_dependencies and self._is_known_model(dep_tuple):
                        all_dependencies.append(dep_tuple)

            for dep_key in all_dependencies:
                if dep_key not in self._model_nodes:
                    error = MissingDependencyError(
                        model_key=key,
                        dependency_key=dep_key,
                        file_path=node.file_path,
                    )
                    self._errors.append(
                        DAGBuildError(
                            message=error.message,
                            file_path=node.file_path,
                            model_key=key,
                        )
                    )
                    if strict:
                        raise error
                    continue

                # Check for cycle before adding edge (if incremental checking enabled)
                if cycle_detector:
                    from_metadata = {
                        "file_path": str(node.file_path) if node.file_path else None,
                    }
                    dep_node = self._model_nodes.get(dep_key)
                    to_metadata = {
                        "file_path": str(dep_node.file_path) if dep_node and dep_node.file_path else None,
                    } if dep_node else {}

                    # This will raise TransformationError if cycle detected
                    cycle_info = cycle_detector.add_edge_with_check(
                        key,
                        dep_key,
                        from_metadata=from_metadata,
                        to_metadata=to_metadata,
                        raise_on_cycle=strict,
                    )

                    if cycle_info is not None:
                        # Non-strict mode: collect the error
                        self._errors.append(
                            DAGBuildError(
                                message=f"Circular dependency: {cycle_info.cycle_str}",
                                file_path=node.file_path,
                                model_key=key,
                            )
                        )
                else:
                    # No incremental checking - just add the edge
                    self._dag.add_edge(key, dep_key)

        self._pass_two_complete = True
        logger.info(
            f"Pass 2 complete: {self._dag.number_of_nodes()} nodes, "
            f"{self._dag.number_of_edges()} edges"
        )

    def validate_no_cycles(self) -> None:
        """
        Validate that the DAG contains no cycles.

        Raises:
            CycleDetectedError: If a cycle is detected
            RuntimeError: If the DAG hasn't been built yet
        """
        if not self._pass_two_complete:
            raise RuntimeError("DAG must be built before validating cycles")

        try:
            cycle = nx.find_cycle(self._dag)
            # Convert edge list to node list
            cycle_nodes = [edge[0] for edge in cycle]
            # Add the first node again to show the cycle completes
            cycle_nodes.append(cycle_nodes[0])
            raise CycleDetectedError(cycle_nodes)
        except nx.NetworkXNoCycle:
            # No cycle found - this is the expected case
            logger.debug("DAG validation passed: no cycles detected")

    def get_topological_order(self) -> list[tuple[str, str]]:
        """
        Get models in topological order (dependencies first).

        Returns:
            List of (schema, model) tuples in execution order

        Raises:
            CycleDetectedError: If the graph contains cycles
            RuntimeError: If the DAG hasn't been built yet
        """
        if not self._pass_two_complete:
            raise RuntimeError("DAG must be built before getting topological order")

        try:
            # Reverse because we want dependencies first
            return list(reversed(list(nx.topological_sort(self._dag))))
        except nx.NetworkXUnfeasible:
            # This shouldn't happen if validate_no_cycles passed
            cycle = nx.find_cycle(self._dag)
            cycle_nodes = [edge[0] for edge in cycle]
            cycle_nodes.append(cycle_nodes[0])
            raise CycleDetectedError(cycle_nodes)

    def get_dependencies(self, schema: str, model: str) -> list[tuple[str, str]]:
        """
        Get direct dependencies of a model.

        Args:
            schema: The schema name
            model: The model name

        Returns:
            List of (schema, model) tuples for direct dependencies
        """
        key = (schema, model)
        if key not in self._dag:
            raise KeyError(f"Model '{schema}.{model}' not found in DAG")
        return list(self._dag.successors(key))

    def get_dependents(self, schema: str, model: str) -> list[tuple[str, str]]:
        """
        Get models that depend on this model.

        Args:
            schema: The schema name
            model: The model name

        Returns:
            List of (schema, model) tuples for models that depend on this one
        """
        key = (schema, model)
        if key not in self._dag:
            raise KeyError(f"Model '{schema}.{model}' not found in DAG")
        return list(self._dag.predecessors(key))

    def get_all_upstream(self, schema: str, model: str) -> set[tuple[str, str]]:
        """
        Get all transitive dependencies of a model.

        Args:
            schema: The schema name
            model: The model name

        Returns:
            Set of (schema, model) tuples for all upstream dependencies
        """
        key = (schema, model)
        if key not in self._dag:
            raise KeyError(f"Model '{schema}.{model}' not found in DAG")
        return set(nx.descendants(self._dag, key))

    def get_all_downstream(self, schema: str, model: str) -> set[tuple[str, str]]:
        """
        Get all models that transitively depend on this model.

        Args:
            schema: The schema name
            model: The model name

        Returns:
            Set of (schema, model) tuples for all downstream dependents
        """
        key = (schema, model)
        if key not in self._dag:
            raise KeyError(f"Model '{schema}.{model}' not found in DAG")
        return set(nx.ancestors(self._dag, key))

    def _process_yaml_file(self, yaml_path: Path) -> None:
        """
        Process a single YAML file and register its model.

        Args:
            yaml_path: Path to the YAML configuration file

        Raises:
            FileNotFoundError: If the file doesn't exist
            TransformationError: If YAML parsing fails
        """
        from backend.application.config_parser.yaml_loader import YAMLConfigLoader

        if not yaml_path.exists():
            raise FileNotFoundError(f"YAML file not found: {yaml_path}")

        loader = YAMLConfigLoader()
        config = loader.load_from_file(yaml_path)

        self._register_config(config, yaml_path)

    def _register_config(
        self,
        config: ConfigParser,
        file_path: Optional[Path] = None,
    ) -> None:
        """
        Register a ConfigParser instance in the registry and model nodes.

        Args:
            config: The ConfigParser instance
            file_path: Optional path to the source file
        """
        schema = config.destination_schema_name
        model = config.model_name
        key = (schema, model)

        # Extract dependencies from config
        dependencies = self._extract_dependencies(config)

        # Create model node
        node = ModelNode(
            schema=schema,
            model=model,
            config=config,
            file_path=file_path,
            dependencies=dependencies,
        )
        self._model_nodes[key] = node

        # Register in ModelRegistry
        materialization = config.materialization if hasattr(config, 'materialization') else "TABLE"
        try:
            self._registry.register(
                schema=schema,
                model=model,
                config=config,
                table_name=config.destination_table_name,
                materialization_type=materialization,
            )
        except ValueError:
            # Model already registered - this is ok if it's the same config
            logger.debug(f"Model {key} already registered in registry")

    def _extract_dependencies(self, config: ConfigParser) -> list[tuple[str, str]]:
        """
        Extract dependency information from a ConfigParser.

        Looks for dependencies in multiple places:
        - 'dependencies' field in YAML
        - 'reference' field
        - 'source_model' property
        - Tables used in JOIN transformations (if they are models)
        - Tables used in UNION transformations (if they are models)

        Args:
            config: The ConfigParser instance

        Returns:
            List of (schema, model) tuples representing dependencies
        """
        dependencies: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()

        # Check for explicit dependencies field
        deps_field = config.get("dependencies", [])
        if isinstance(deps_field, list):
            for dep in deps_field:
                dep_tuple = self._parse_dependency(dep, config)
                if dep_tuple and dep_tuple not in seen:
                    dependencies.append(dep_tuple)
                    seen.add(dep_tuple)

        # Check reference field
        references = config.reference or []
        for ref in references:
            dep_tuple = self._parse_dependency(ref, config)
            if dep_tuple and dep_tuple not in seen:
                dependencies.append(dep_tuple)
                seen.add(dep_tuple)

        # Check source_model property
        source_model = config.source_model
        if source_model:
            # source_model is just a model name, use source schema
            dep_tuple = (config.source_schema_name, source_model)
            if dep_tuple not in seen:
                dependencies.append(dep_tuple)
                seen.add(dep_tuple)

        # Extract tables from JOIN transformations
        join_tables = self._extract_join_tables(config)
        for dep_tuple in join_tables:
            if dep_tuple not in seen and self._is_known_model(dep_tuple):
                dependencies.append(dep_tuple)
                seen.add(dep_tuple)

        # Extract tables from UNION transformations
        union_tables = self._extract_union_tables(config)
        for dep_tuple in union_tables:
            if dep_tuple not in seen and self._is_known_model(dep_tuple):
                dependencies.append(dep_tuple)
                seen.add(dep_tuple)

        return dependencies

    def _extract_join_tables(self, config: ConfigParser) -> list[tuple[str, str]]:
        """
        Extract all tables used in JOIN transformations.

        Args:
            config: The ConfigParser instance

        Returns:
            List of (schema, table) tuples from JOIN transformations
        """
        tables: list[tuple[str, str]] = []
        transform = config.get("transform", {}) or {}

        for trans_id, trans_data in transform.items():
            if not isinstance(trans_data, dict):
                continue

            if trans_data.get("type") == "join":
                join_config = trans_data.get("join", {}) or {}
                join_tables = join_config.get("tables", []) or []

                for join_item in join_tables:
                    if not isinstance(join_item, dict):
                        continue

                    joined_table = join_item.get("joined_table", {}) or {}
                    schema = joined_table.get("schema_name")
                    table_name = joined_table.get("table_name")

                    if schema and table_name:
                        tables.append((schema, table_name))

        return tables

    def _extract_union_tables(self, config: ConfigParser) -> list[tuple[str, str]]:
        """
        Extract all tables used in UNION transformations.

        Args:
            config: The ConfigParser instance

        Returns:
            List of (schema, table) tuples from UNION transformations
        """
        tables: list[tuple[str, str]] = []
        transform = config.get("transform", {}) or {}

        for trans_id, trans_data in transform.items():
            if not isinstance(trans_data, dict):
                continue

            if trans_data.get("type") == "union":
                union_config = trans_data.get("union", {}) or {}
                branches = union_config.get("branches", []) or []

                for branch in branches:
                    if not isinstance(branch, dict):
                        continue

                    schema = branch.get("schema")
                    table_name = branch.get("table")

                    if schema and table_name:
                        tables.append((schema, table_name))

        return tables

    def _is_known_model(self, table_key: tuple[str, str]) -> bool:
        """
        Check if a table is a known model (registered in model nodes or registry).

        This is used to distinguish between raw database tables and model outputs.
        Only model outputs should create dependencies.

        Args:
            table_key: (schema, table_name) tuple

        Returns:
            True if the table is a known model, False otherwise
        """
        # Check if it's in our model nodes (being built in this DAG)
        if table_key in self._model_nodes:
            return True

        # Check if it's in the registry (already registered models)
        if self._registry and self._registry.contains(table_key[0], table_key[1]):
            return True

        return False

    def _parse_dependency(
        self,
        dep: Union[str, dict[str, str]],
        config: ConfigParser,
    ) -> Optional[tuple[str, str]]:
        """
        Parse a dependency reference into a (schema, model) tuple.

        Supports formats:
        - "schema.model" string
        - "model" string (uses config's source schema as default)
        - {"schema": "x", "model": "y"} dict

        Args:
            dep: The dependency reference
            config: The ConfigParser for default schema

        Returns:
            (schema, model) tuple, or None if invalid
        """
        if isinstance(dep, dict):
            schema = dep.get("schema", config.source_schema_name)
            model = dep.get("model") or dep.get("name")
            if model:
                return (schema, model)
            return None

        if isinstance(dep, str):
            if "." in dep:
                parts = dep.split(".", 1)
                return (parts[0], parts[1])
            else:
                # Use source schema as default
                return (config.source_schema_name, dep)

        return None

    def add_model(
        self,
        schema: str,
        model: str,
        config: ConfigParser,
        dependencies: Optional[list[tuple[str, str]]] = None,
    ) -> None:
        """
        Manually add a model to the builder.

        This can be used to add models programmatically without YAML files.

        Args:
            schema: The schema name
            model: The model name
            config: The ConfigParser instance
            dependencies: Optional list of (schema, model) dependency tuples
        """
        if self._pass_two_complete:
            raise RuntimeError("Cannot add models after DAG construction")

        key = (schema, model)
        node = ModelNode(
            schema=schema,
            model=model,
            config=config,
            dependencies=dependencies or [],
        )
        self._model_nodes[key] = node

        # Register in ModelRegistry
        try:
            self._registry.register(
                schema=schema,
                model=model,
                config=config,
                table_name=config.destination_table_name,
            )
        except ValueError:
            logger.debug(f"Model {key} already registered")

        self._pass_one_complete = True

    def clear(self) -> None:
        """Reset the builder to initial state."""
        self._dag.clear()
        self._model_nodes.clear()
        self._errors.clear()
        self._pass_one_complete = False
        self._pass_two_complete = False
