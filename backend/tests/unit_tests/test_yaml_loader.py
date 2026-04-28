"""Unit tests for YAML loader with database integration."""

from pathlib import Path
from unittest.mock import MagicMock, patch
import tempfile

import pytest
import yaml

from backend.application.config_parser.yaml_loader import (
    YAMLConfigLoader,
    YAMLParseError,
    load_yaml_config,
)
from backend.application.config_parser.config_parser import ConfigParser
from visitran.errors import TransformationError


class TestYAMLConfigLoaderLoadFromString:
    """Tests for loading YAML from string."""

    def setup_method(self):
        """Clear ConfigParser cache before each test."""
        ConfigParser._instances.clear()

    def test_load_simple_yaml(self):
        """Test loading a simple valid YAML string."""
        yaml_content = """
source:
  table_name: orders
  schema_name: public
model:
  table_name: processed_orders
  schema_name: analytics
"""
        loader = YAMLConfigLoader()
        parser = loader.load_from_string(yaml_content, "test_model")

        assert parser.model_name == "test_model"
        assert parser.source_table_name == "orders"
        assert parser.source_schema_name == "public"
        assert parser.destination_table_name == "processed_orders"

    def test_load_yaml_with_transforms_populates_source_map(self):
        """Test that source map is populated for transformations."""
        yaml_content = """
source:
  table_name: orders
  schema_name: public
model:
  table_name: output
  schema_name: public
transform:
  filter_active:
    type: filter
    filter:
      column: status
      operator: equals
      value: active
  rename_columns:
    type: rename
    rename:
      old_name: id
      new_name: order_id
transform_order:
  - filter_active
  - rename_columns
"""
        loader = YAMLConfigLoader()
        parser = loader.load_from_string(yaml_content, "transform_model")

        # Source map should contain transformation locations
        assert parser.has_source_location("filter_active")
        assert parser.has_source_location("rename_columns")

        # Locations should be tuples
        loc = parser.get_source_location("filter_active")
        assert isinstance(loc, tuple)
        assert len(loc) == 2

    def test_load_yaml_invalid_syntax_raises_transformation_error(self):
        """Test that invalid YAML syntax raises TransformationError."""
        invalid_yaml = """
source:
  table_name: orders
  - invalid: list in mapping
"""
        loader = YAMLConfigLoader()

        with pytest.raises(TransformationError) as exc_info:
            loader.load_from_string(invalid_yaml, "invalid_model")

        error = exc_info.value
        assert error.model_name == "invalid_model"
        assert error.line_number is not None
        assert error.column_number is not None
        assert "YAML" in error.error_message

    def test_load_yaml_invalid_syntax_includes_snippet(self):
        """Test that error includes YAML context snippet."""
        invalid_yaml = """line_one: value
line_two: value
line_three: [invalid: yaml
line_four: value
line_five: value
"""
        loader = YAMLConfigLoader()

        with pytest.raises(TransformationError) as exc_info:
            loader.load_from_string(invalid_yaml, "snippet_test")

        error = exc_info.value
        # Should have a snippet with context
        assert error.yaml_snippet is not None

    def test_load_yaml_non_strict_returns_empty_parser(self):
        """Test that strict=False returns empty parser on error."""
        invalid_yaml = "invalid: yaml: content: [["
        loader = YAMLConfigLoader()

        parser = loader.load_from_string(invalid_yaml, "non_strict", strict=False)

        assert parser is not None
        assert parser.model_name == "non_strict"

    def test_load_empty_yaml_returns_empty_parser(self):
        """Test loading empty YAML content."""
        loader = YAMLConfigLoader()
        parser = loader.load_from_string("", "empty_model")

        assert parser is not None
        assert parser.model_name == "empty_model"

    def test_load_yaml_clears_singleton_cache(self):
        """Test that loading clears the singleton cache for the model name."""
        yaml_v1 = """
source:
  table_name: table_v1
  schema_name: public
model:
  table_name: output
  schema_name: public
"""
        yaml_v2 = """
source:
  table_name: table_v2
  schema_name: public
model:
  table_name: output
  schema_name: public
"""
        loader = YAMLConfigLoader()

        parser_v1 = loader.load_from_string(yaml_v1, "cached_model")
        assert parser_v1.source_table_name == "table_v1"

        parser_v2 = loader.load_from_string(yaml_v2, "cached_model")
        assert parser_v2.source_table_name == "table_v2"


class TestYAMLConfigLoaderLoadFromFile:
    """Tests for loading YAML from file."""

    def setup_method(self):
        """Clear ConfigParser cache before each test."""
        ConfigParser._instances.clear()

    def test_load_from_file_success(self):
        """Test loading from a valid YAML file."""
        yaml_content = """
source:
  table_name: orders
  schema_name: public
model:
  table_name: output
  schema_name: analytics
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(yaml_content)
            f.flush()
            file_path = Path(f.name)

        try:
            loader = YAMLConfigLoader()
            parser = loader.load_from_file(file_path)

            assert parser.source_table_name == "orders"
            assert parser.model_name == file_path.stem
        finally:
            file_path.unlink()

    def test_load_from_file_with_custom_model_name(self):
        """Test loading with a custom model name."""
        yaml_content = """
source:
  table_name: orders
  schema_name: public
model:
  table_name: output
  schema_name: public
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(yaml_content)
            f.flush()
            file_path = Path(f.name)

        try:
            loader = YAMLConfigLoader()
            parser = loader.load_from_file(file_path, model_name="custom_name")

            assert parser.model_name == "custom_name"
        finally:
            file_path.unlink()

    def test_load_from_file_not_found_raises_error(self):
        """Test that FileNotFoundError is raised for missing files."""
        loader = YAMLConfigLoader()

        with pytest.raises(FileNotFoundError):
            loader.load_from_file("/nonexistent/path/config.yaml")

    def test_load_from_file_invalid_yaml_raises_transformation_error(self):
        """Test that invalid YAML in file raises TransformationError."""
        invalid_yaml = "invalid: yaml: [["

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(invalid_yaml)
            f.flush()
            file_path = Path(f.name)

        try:
            loader = YAMLConfigLoader()

            with pytest.raises(TransformationError) as exc_info:
                loader.load_from_file(file_path)

            error = exc_info.value
            assert error.line_number is not None
        finally:
            file_path.unlink()


class TestYAMLConfigLoaderLoadFromModel:
    """Tests for loading from database ConfigModels."""

    def setup_method(self):
        """Clear ConfigParser cache before each test."""
        ConfigParser._instances.clear()

    def test_load_from_model_success(self):
        """Test loading from a mock ConfigModels instance."""
        mock_model = MagicMock()
        mock_model.model_name = "db_model"
        mock_model.model_data = {
            "source": {"table_name": "orders", "schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "analytics"},
        }

        loader = YAMLConfigLoader()
        parser = loader.load_from_model(mock_model)

        assert parser.model_name == "db_model"
        assert parser.source_table_name == "orders"

    def test_load_from_model_clears_cache(self):
        """Test that loading from model clears singleton cache."""
        # Pre-populate cache
        mock_model_v1 = MagicMock()
        mock_model_v1.model_name = "cached_db_model"
        mock_model_v1.model_data = {
            "source": {"table_name": "table_v1", "schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "public"},
        }

        loader = YAMLConfigLoader()
        parser_v1 = loader.load_from_model(mock_model_v1)

        mock_model_v2 = MagicMock()
        mock_model_v2.model_name = "cached_db_model"
        mock_model_v2.model_data = {
            "source": {"table_name": "table_v2", "schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "public"},
        }

        parser_v2 = loader.load_from_model(mock_model_v2)

        assert parser_v2.source_table_name == "table_v2"


class TestYAMLConfigLoaderMultiDocument:
    """Tests for multi-document YAML handling."""

    def setup_method(self):
        """Clear ConfigParser cache before each test."""
        ConfigParser._instances.clear()

    def test_load_multi_document_yaml(self):
        """Test loading multiple YAML documents."""
        yaml_content = """
source:
  table_name: orders
  schema_name: public
model:
  table_name: output1
  schema_name: public
---
source:
  table_name: customers
  schema_name: public
model:
  table_name: output2
  schema_name: public
"""
        loader = YAMLConfigLoader()
        parsers = loader.load_multi_document(yaml_content, "multi_doc")

        assert len(parsers) == 2
        assert parsers[0].source_table_name == "orders"
        assert parsers[1].source_table_name == "customers"

    def test_load_multi_document_names_indexed(self):
        """Test that multi-document models are named with indices."""
        yaml_content = """
source:
  table_name: orders
  schema_name: public
model:
  table_name: out1
  schema_name: public
---
source:
  table_name: customers
  schema_name: public
model:
  table_name: out2
  schema_name: public
"""
        loader = YAMLConfigLoader()
        parsers = loader.load_multi_document(yaml_content, "doc")

        assert parsers[0].model_name == "doc_0"
        assert parsers[1].model_name == "doc_1"

    def test_load_single_document_no_index(self):
        """Test that single document doesn't get indexed."""
        yaml_content = """
source:
  table_name: orders
  schema_name: public
model:
  table_name: output
  schema_name: public
"""
        loader = YAMLConfigLoader()
        parsers = loader.load_multi_document(yaml_content, "single")

        assert len(parsers) == 1
        assert parsers[0].model_name == "single"

    def test_load_multi_document_skips_empty_documents(self):
        """Test that empty documents are skipped."""
        yaml_content = """
---
source:
  table_name: orders
  schema_name: public
model:
  table_name: output
  schema_name: public
---
"""
        loader = YAMLConfigLoader()
        parsers = loader.load_multi_document(yaml_content)

        # Only non-empty document should be included
        assert len(parsers) == 1

    def test_load_multi_document_invalid_yaml_raises_error(self):
        """Test that invalid YAML in multi-doc raises TransformationError."""
        yaml_content = """
source:
  table_name: orders
  schema_name: public
model:
  table_name: output
  schema_name: public
---
invalid: yaml: [[
"""
        loader = YAMLConfigLoader()

        with pytest.raises(TransformationError) as exc_info:
            loader.load_multi_document(yaml_content, "multi_error")

        error = exc_info.value
        assert "multi_error" in error.model_name


class TestYAMLErrorExtraction:
    """Tests for YAML error information extraction."""

    def setup_method(self):
        """Clear ConfigParser cache before each test."""
        ConfigParser._instances.clear()

    def test_error_line_number_is_1_based(self):
        """Test that error line numbers are converted to 1-based."""
        # Line 3 has the error (0-indexed: 2)
        yaml_content = """line_one: ok
line_two: ok
line_three: [invalid
line_four: ok
"""
        loader = YAMLConfigLoader()

        with pytest.raises(TransformationError) as exc_info:
            loader.load_from_string(yaml_content, "line_test")

        error = exc_info.value
        # Should be 1-based, and around line 3-4
        assert error.line_number >= 1

    def test_error_preserves_original_exception(self):
        """Test that original exception is chained."""
        invalid_yaml = "invalid: [["
        loader = YAMLConfigLoader()

        with pytest.raises(TransformationError) as exc_info:
            loader.load_from_string(invalid_yaml, "chain_test")

        error = exc_info.value
        assert error.__cause__ is not None
        assert isinstance(error.__cause__, yaml.YAMLError)


class TestLoadYamlConfigConvenience:
    """Tests for the load_yaml_config convenience function."""

    def setup_method(self):
        """Clear ConfigParser cache before each test."""
        ConfigParser._instances.clear()

    def test_load_from_string(self):
        """Test convenience function with YAML string."""
        yaml_content = """
source:
  table_name: orders
  schema_name: public
model:
  table_name: output
  schema_name: public
"""
        parser = load_yaml_config(yaml_content, model_name="string_test")

        assert parser.model_name == "string_test"
        assert parser.source_table_name == "orders"

    def test_load_from_file_path(self):
        """Test convenience function with file path."""
        yaml_content = """
source:
  table_name: customers
  schema_name: public
model:
  table_name: output
  schema_name: public
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(yaml_content)
            f.flush()
            file_path = Path(f.name)

        try:
            parser = load_yaml_config(file_path)
            assert parser.source_table_name == "customers"
        finally:
            file_path.unlink()

    def test_load_from_model_object(self):
        """Test convenience function with model-like object."""
        mock_model = MagicMock()
        mock_model.model_name = "convenience_model"
        mock_model.model_data = {
            "source": {"table_name": "products", "schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "public"},
        }

        parser = load_yaml_config(mock_model)

        assert parser.model_name == "convenience_model"
        assert parser.source_table_name == "products"

    def test_load_string_without_model_name_uses_default(self):
        """Test that string without model_name gets default name."""
        yaml_content = """
source:
  table_name: orders
  schema_name: public
model:
  table_name: output
  schema_name: public
"""
        parser = load_yaml_config(yaml_content)

        assert parser.model_name == "unnamed_model"


class TestYAMLParseError:
    """Tests for YAMLParseError exception class."""

    def test_yaml_parse_error_creation(self):
        """Test creating YAMLParseError with all attributes."""
        error = YAMLParseError(
            message="Test error",
            line=10,
            column=5,
            yaml_content="test: content",
        )

        assert error.message == "Test error"
        assert error.line == 10
        assert error.column == 5
        assert error.yaml_content == "test: content"
        assert str(error) == "Test error"

    def test_yaml_parse_error_minimal(self):
        """Test creating YAMLParseError with minimal attributes."""
        error = YAMLParseError("Simple error")

        assert error.message == "Simple error"
        assert error.line is None
        assert error.column is None
        assert error.yaml_content is None
