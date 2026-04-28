"""Unit tests for YAML source location tracking."""

import pytest

from backend.application.config_parser.yaml_source_tracker import (
    SourceLocation,
    YAMLSourceTracker,
    parse_yaml_with_locations,
)


class TestSourceLocation:
    """Tests for SourceLocation class."""

    def test_source_location_creation(self):
        """Test creating a SourceLocation instance."""
        loc = SourceLocation(10, 5)
        assert loc.line == 10
        assert loc.column == 5
        assert loc.end_line is None
        assert loc.end_column is None

    def test_source_location_with_end(self):
        """Test SourceLocation with end positions."""
        loc = SourceLocation(10, 5, 15, 20)
        assert loc.line == 10
        assert loc.column == 5
        assert loc.end_line == 15
        assert loc.end_column == 20

    def test_source_location_as_tuple(self):
        """Test as_tuple method."""
        loc = SourceLocation(10, 5)
        assert loc.as_tuple() == (10, 5)

    def test_source_location_repr(self):
        """Test string representation."""
        loc = SourceLocation(10, 5)
        assert "line=10" in repr(loc)
        assert "column=5" in repr(loc)


class TestYAMLSourceTracker:
    """Tests for YAMLSourceTracker class."""

    def test_load_empty_yaml(self):
        """Test loading empty YAML content."""
        tracker = YAMLSourceTracker()
        data, source_map = tracker.load_with_locations("")
        assert data == {}
        assert source_map == {}

    def test_load_yaml_without_transforms(self):
        """Test loading YAML without transform section."""
        yaml_content = """
name: test_model
source:
  table: orders
"""
        tracker = YAMLSourceTracker()
        data, source_map = tracker.load_with_locations(yaml_content)

        assert data["name"] == "test_model"
        assert source_map == {}

    def test_load_yaml_with_transform_mapping(self):
        """Test loading YAML with transform as mapping."""
        yaml_content = """
name: test_model
transform:
  filter_001:
    type: filter
    filter:
      column: status
      operator: equals
      value: active
  join_002:
    type: join
    join:
      table: customers
"""
        tracker = YAMLSourceTracker()
        data, source_map = tracker.load_with_locations(yaml_content)

        assert data["name"] == "test_model"
        assert "filter_001" in source_map
        assert "join_002" in source_map

        # Verify locations are tuples of (line, column)
        assert isinstance(source_map["filter_001"].as_tuple(), tuple)
        assert len(source_map["filter_001"].as_tuple()) == 2

    def test_source_locations_are_correct(self):
        """Test that source locations point to correct positions."""
        yaml_content = """transform:
  first_transform:
    type: filter
"""
        tracker = YAMLSourceTracker()
        data, source_map = tracker.load_with_locations(yaml_content)

        # first_transform should be on line 2 (1-indexed)
        loc = source_map.get("first_transform")
        assert loc is not None
        assert loc.line == 2

    def test_get_source_location_method(self):
        """Test get_source_location returns correct tuple."""
        yaml_content = """
transform:
  my_transform:
    type: filter
"""
        tracker = YAMLSourceTracker()
        tracker.load_with_locations(yaml_content)

        location = tracker.get_source_location("my_transform")
        assert location is not None
        assert isinstance(location, tuple)
        assert len(location) == 2

    def test_get_source_location_missing(self):
        """Test get_source_location returns None for missing ID."""
        tracker = YAMLSourceTracker()
        tracker.load_with_locations("name: test")

        location = tracker.get_source_location("nonexistent")
        assert location is None

    def test_clear_resets_source_map(self):
        """Test that clear() resets the source map."""
        yaml_content = """
transform:
  my_transform:
    type: filter
"""
        tracker = YAMLSourceTracker()
        tracker.load_with_locations(yaml_content)
        assert len(tracker.source_map) > 0

        tracker.clear()
        assert len(tracker.source_map) == 0

    def test_invalid_yaml_fallback(self):
        """Test that invalid YAML falls back gracefully."""
        yaml_content = "invalid: yaml: content: [["
        tracker = YAMLSourceTracker()

        # Should not raise, but may return empty source map
        data, source_map = tracker.load_with_locations(yaml_content)
        # Source map should be empty on parse failure
        assert isinstance(source_map, dict)


class TestParseYamlWithLocations:
    """Tests for parse_yaml_with_locations convenience function."""

    def test_parse_yaml_with_locations(self):
        """Test the convenience function."""
        yaml_content = """
transform:
  transform_a:
    type: filter
  transform_b:
    type: join
"""
        data, source_map = parse_yaml_with_locations(yaml_content)

        assert "transform" in data
        assert "transform_a" in source_map
        assert "transform_b" in source_map

        # Source map should contain tuples
        assert isinstance(source_map["transform_a"], tuple)

    def test_parse_empty_yaml(self):
        """Test parsing empty YAML."""
        data, source_map = parse_yaml_with_locations("")
        assert data == {}
        assert source_map == {}


class TestConfigParserSourceTracking:
    """Tests for ConfigParser source location integration."""

    def test_config_parser_set_yaml_content(self):
        """Test setting YAML content on ConfigParser."""
        from backend.application.config_parser.config_parser import ConfigParser

        yaml_content = """
source:
  table_name: orders
  schema_name: public
model:
  table_name: processed_orders
  schema_name: analytics
transform:
  filter_active:
    type: filter
transform_order:
  - filter_active
"""
        # Clear any cached instances
        ConfigParser._instances.clear()

        import yaml
        model_data = yaml.safe_load(yaml_content)
        parser = ConfigParser(model_data, "test_model")
        parser.set_yaml_content(yaml_content)

        # Should have source location for the transformation
        assert parser.has_source_location("filter_active")
        location = parser.get_source_location("filter_active")
        assert location is not None
        assert isinstance(location, tuple)

    def test_config_parser_set_source_map_directly(self):
        """Test setting source map directly."""
        from backend.application.config_parser.config_parser import ConfigParser

        # Clear any cached instances
        ConfigParser._instances.clear()

        model_data = {
            "source": {"table_name": "orders", "schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "public"},
        }
        parser = ConfigParser(model_data, "test_model_2")

        source_map = {
            "transform_1": (10, 5),
            "transform_2": (20, 3),
        }
        parser.set_source_map(source_map)

        assert parser.get_source_location("transform_1") == (10, 5)
        assert parser.get_source_location("transform_2") == (20, 3)
        assert parser.get_source_location("nonexistent") is None

    def test_config_parser_tracked_transformations(self):
        """Test tracked_transformations property."""
        from backend.application.config_parser.config_parser import ConfigParser

        # Clear any cached instances
        ConfigParser._instances.clear()

        model_data = {
            "source": {"table_name": "orders", "schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "public"},
        }
        parser = ConfigParser(model_data, "test_model_3")

        source_map = {
            "transform_a": (1, 1),
            "transform_b": (2, 1),
        }
        parser.set_source_map(source_map)

        tracked = parser.tracked_transformations
        assert "transform_a" in tracked
        assert "transform_b" in tracked

    def test_source_map_is_copied(self):
        """Test that set_source_map copies the input."""
        from backend.application.config_parser.config_parser import ConfigParser

        # Clear any cached instances
        ConfigParser._instances.clear()

        model_data = {
            "source": {"table_name": "orders", "schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "public"},
        }
        parser = ConfigParser(model_data, "test_model_4")

        source_map = {"transform_1": (10, 5)}
        parser.set_source_map(source_map)

        # Modify original - should not affect parser
        source_map["transform_1"] = (99, 99)
        assert parser.get_source_location("transform_1") == (10, 5)
