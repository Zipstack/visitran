"""
YAML Source Location Tracker

This module provides utilities for parsing YAML files while capturing
line and column numbers for each node. This enables precise error
reporting with source location context.

Usage:
    tracker = YAMLSourceTracker()
    data, source_map = tracker.load_with_locations(yaml_content)

    # Later, get source location for any transformation
    location = source_map.get("transform_001")  # Returns (line, column) tuple
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import yaml

logger = logging.getLogger(__name__)


class SourceLocation:
    """Represents a source location in a YAML file."""

    __slots__ = ("line", "column", "end_line", "end_column")

    def __init__(
        self,
        line: int,
        column: int,
        end_line: Optional[int] = None,
        end_column: Optional[int] = None,
    ) -> None:
        """
        Initialize a source location.

        Args:
            line: 1-based line number
            column: 1-based column number
            end_line: Optional end line (for multi-line nodes)
            end_column: Optional end column
        """
        self.line = line
        self.column = column
        self.end_line = end_line
        self.end_column = end_column

    def as_tuple(self) -> tuple[int, int]:
        """Return (line, column) tuple."""
        return (self.line, self.column)

    def __repr__(self) -> str:
        return f"SourceLocation(line={self.line}, column={self.column})"


class YAMLSourceTracker:
    """
    Tracks source locations while parsing YAML content.

    Uses PyYAML's yaml.compose() to access node positions via start_mark
    attributes, then maps transformation identifiers to their locations.
    """

    def __init__(self) -> None:
        """Initialize the source tracker."""
        self._source_map: dict[str, SourceLocation] = {}

    @property
    def source_map(self) -> dict[str, SourceLocation]:
        """Return the current source map."""
        return self._source_map

    def clear(self) -> None:
        """Clear the source map."""
        self._source_map.clear()

    def load_with_locations(
        self, yaml_content: str
    ) -> tuple[dict[str, Any], dict[str, SourceLocation]]:
        """
        Load YAML content and extract source locations for transformations.

        Args:
            yaml_content: The raw YAML string to parse

        Returns:
            Tuple of (parsed_data, source_map) where:
            - parsed_data: The standard Python dict representation
            - source_map: Dict mapping transformation IDs to SourceLocation objects
        """
        self.clear()

        # First, compose to get the node tree with positions
        try:
            root_node = yaml.compose(yaml_content)
        except yaml.YAMLError as e:
            logger.warning(f"YAML composition failed, falling back to safe_load: {e}")
            # Fallback to regular parsing without source tracking
            try:
                data = yaml.safe_load(yaml_content) or {}
            except yaml.YAMLError:
                # If both compose and safe_load fail, return empty
                data = {}
            return data, {}

        if root_node is None:
            return {}, {}

        # Extract source locations from the node tree
        self._extract_transform_locations(root_node)

        # Also parse normally to get the data
        data = yaml.safe_load(yaml_content) or {}

        return data, self._source_map.copy()

    def _extract_transform_locations(self, root_node: yaml.Node) -> None:
        """
        Extract source locations for transformation definitions.

        Traverses the YAML node tree looking for transformation definitions
        and records their source positions.

        Args:
            root_node: The root YAML node from yaml.compose()
        """
        if not isinstance(root_node, yaml.MappingNode):
            return

        # Find the 'transform' key in the root mapping
        transform_node = None
        for key_node, value_node in root_node.value:
            if isinstance(key_node, yaml.ScalarNode) and key_node.value == "transform":
                transform_node = value_node
                break

        if transform_node is None:
            return

        # The transform section should be a mapping of transformation IDs to configs
        if isinstance(transform_node, yaml.MappingNode):
            self._process_transform_mapping(transform_node)
        elif isinstance(transform_node, yaml.SequenceNode):
            # Handle case where transforms is a list
            self._process_transform_sequence(transform_node)

    def _process_transform_mapping(self, transform_node: yaml.MappingNode) -> None:
        """
        Process a transform node that is a mapping (dict).

        Args:
            transform_node: The YAML mapping node containing transformations
        """
        for key_node, value_node in transform_node.value:
            if isinstance(key_node, yaml.ScalarNode):
                transform_id = key_node.value
                # Record the location of the transformation definition
                self._record_location(transform_id, key_node)

                # Also look for nested transformation IDs within the value
                self._extract_nested_ids(value_node, prefix=transform_id)

    def _process_transform_sequence(self, transform_node: yaml.SequenceNode) -> None:
        """
        Process a transform node that is a sequence (list).

        Args:
            transform_node: The YAML sequence node containing transformations
        """
        for idx, item_node in enumerate(transform_node.value):
            if isinstance(item_node, yaml.MappingNode):
                # Look for transformation_id in the item
                for key_node, value_node in item_node.value:
                    if isinstance(key_node, yaml.ScalarNode):
                        if key_node.value == "transformation_id":
                            if isinstance(value_node, yaml.ScalarNode):
                                transform_id = value_node.value
                                self._record_location(transform_id, item_node)
                        elif key_node.value == "id":
                            if isinstance(value_node, yaml.ScalarNode):
                                transform_id = value_node.value
                                self._record_location(transform_id, item_node)

    def _extract_nested_ids(self, node: yaml.Node, prefix: str = "") -> None:
        """
        Extract transformation IDs from nested structures.

        Args:
            node: The YAML node to search
            prefix: Optional prefix for nested IDs
        """
        if isinstance(node, yaml.MappingNode):
            for key_node, value_node in node.value:
                if isinstance(key_node, yaml.ScalarNode):
                    key = key_node.value
                    # Check for transformation_id or id fields
                    if key in ("transformation_id", "id"):
                        if isinstance(value_node, yaml.ScalarNode):
                            self._record_location(value_node.value, value_node)
                    else:
                        # Recurse into nested mappings
                        self._extract_nested_ids(value_node, prefix)

        elif isinstance(node, yaml.SequenceNode):
            for item in node.value:
                self._extract_nested_ids(item, prefix)

    def _record_location(self, identifier: str, node: yaml.Node) -> None:
        """
        Record the source location for an identifier.

        Args:
            identifier: The transformation or field identifier
            node: The YAML node containing position information
        """
        if node.start_mark is None:
            logger.debug(f"No start_mark for identifier: {identifier}")
            return

        # PyYAML uses 0-based line/column, convert to 1-based
        line = node.start_mark.line + 1
        column = node.start_mark.column + 1

        end_line = None
        end_column = None
        if node.end_mark is not None:
            end_line = node.end_mark.line + 1
            end_column = node.end_mark.column + 1

        location = SourceLocation(line, column, end_line, end_column)
        self._source_map[identifier] = location

        logger.debug(f"Recorded location for '{identifier}': {location}")

    def get_source_location(
        self, identifier: str
    ) -> Optional[tuple[int, int]]:
        """
        Get the source location for a transformation identifier.

        Args:
            identifier: The transformation ID to look up

        Returns:
            Tuple of (line, column) if found, None otherwise
        """
        location = self._source_map.get(identifier)
        if location:
            return location.as_tuple()
        return None


class SourceAwareConfigParser:
    """
    Mixin class that adds source location tracking to ConfigParser.

    This can be used as a mixin or base class to add source location
    capabilities to any config parser.
    """

    def __init__(self) -> None:
        self._source_map: dict[str, tuple[int, int]] = {}
        self._yaml_content: Optional[str] = None

    def set_yaml_content(self, yaml_content: str) -> None:
        """
        Set the original YAML content for source tracking.

        This should be called before parsing to enable source location tracking.

        Args:
            yaml_content: The raw YAML string
        """
        self._yaml_content = yaml_content
        tracker = YAMLSourceTracker()
        _, source_locations = tracker.load_with_locations(yaml_content)

        # Convert to simple tuple format
        self._source_map = {
            key: loc.as_tuple() for key, loc in source_locations.items()
        }

    def set_source_map(self, source_map: dict[str, tuple[int, int]]) -> None:
        """
        Directly set the source map.

        Args:
            source_map: Dict mapping identifiers to (line, column) tuples
        """
        self._source_map = source_map.copy()

    def get_source_location(
        self, transformation_id: str
    ) -> Optional[tuple[int, int]]:
        """
        Get the source location for a transformation.

        Args:
            transformation_id: The transformation identifier

        Returns:
            Tuple of (line_number, column_number) if found, None otherwise
        """
        return self._source_map.get(transformation_id)

    def has_source_location(self, transformation_id: str) -> bool:
        """Check if a transformation has source location info."""
        return transformation_id in self._source_map

    @property
    def tracked_transformations(self) -> list[str]:
        """Return list of transformations with source location info."""
        return list(self._source_map.keys())


def parse_yaml_with_locations(
    yaml_content: str,
) -> tuple[dict[str, Any], dict[str, tuple[int, int]]]:
    """
    Convenience function to parse YAML with source location tracking.

    This function raises yaml.YAMLError on invalid YAML syntax.
    Use YAMLSourceTracker.load_with_locations() directly for graceful
    error handling without raising exceptions.

    Args:
        yaml_content: The raw YAML string to parse

    Returns:
        Tuple of (parsed_data, source_map) where source_map maps
        transformation IDs to (line, column) tuples

    Raises:
        yaml.YAMLError: If the YAML content is invalid
    """
    # First validate the YAML is parseable (this raises on invalid YAML)
    yaml.safe_load(yaml_content)

    # Now get the source locations
    tracker = YAMLSourceTracker()
    data, locations = tracker.load_with_locations(yaml_content)

    # Convert to simple tuple format
    source_map = {key: loc.as_tuple() for key, loc in locations.items()}

    return data, source_map
