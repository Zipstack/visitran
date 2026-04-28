"""Unit tests for YAML validation layer."""

import pytest

from backend.application.config_parser.config_parser import ConfigParser
from backend.application.config_parser.yaml_validator import (
    ValidationError,
    ValidationResult,
    ValidationSeverity,
    YAMLConfigValidator,
    validate_config,
)
from visitran.errors import TransformationError


class TestValidationError:
    """Tests for ValidationError dataclass."""

    def test_validation_error_creation(self):
        """Test creating a ValidationError with all fields."""
        error = ValidationError(
            field_name="source.table_name",
            message="Missing required field",
            severity=ValidationSeverity.ERROR,
            line_number=10,
            column_number=5,
            suggested_fix="Add table_name field",
        )

        assert error.field_name == "source.table_name"
        assert error.message == "Missing required field"
        assert error.severity == ValidationSeverity.ERROR
        assert error.line_number == 10
        assert error.column_number == 5
        assert error.suggested_fix == "Add table_name field"

    def test_validation_error_defaults(self):
        """Test ValidationError with default values."""
        error = ValidationError(
            field_name="test_field",
            message="Test message",
        )

        assert error.severity == ValidationSeverity.ERROR
        assert error.line_number is None
        assert error.column_number is None
        assert error.suggested_fix is None

    def test_to_transformation_error(self):
        """Test conversion to TransformationError."""
        error = ValidationError(
            field_name="source",
            message="Invalid source configuration",
            line_number=5,
            column_number=3,
            suggested_fix="Fix the source section",
        )

        transform_error = error.to_transformation_error("test_model")

        assert isinstance(transform_error, TransformationError)
        assert transform_error.model_name == "test_model"
        assert transform_error.line_number == 5
        assert transform_error.column_number == 3
        assert "Invalid source" in transform_error.error_message


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_valid_result(self):
        """Test a valid validation result."""
        result = ValidationResult(is_valid=True)

        assert result.is_valid is True
        assert result.error_count == 0
        assert result.warning_count == 0
        assert result.has_errors() is False
        assert result.has_warnings() is False

    def test_result_with_errors(self):
        """Test result with errors."""
        errors = [
            ValidationError(field_name="field1", message="Error 1"),
            ValidationError(field_name="field2", message="Error 2"),
        ]
        result = ValidationResult(is_valid=False, errors=errors)

        assert result.is_valid is False
        assert result.error_count == 2
        assert result.has_errors() is True

    def test_result_with_warnings(self):
        """Test result with warnings."""
        warnings = [
            ValidationError(
                field_name="field1",
                message="Warning 1",
                severity=ValidationSeverity.WARNING,
            ),
        ]
        result = ValidationResult(is_valid=True, warnings=warnings)

        assert result.is_valid is True
        assert result.warning_count == 1
        assert result.has_warnings() is True


class TestYAMLConfigValidatorTopLevel:
    """Tests for top-level field validation."""

    def setup_method(self):
        """Clear ConfigParser cache before each test."""
        ConfigParser._instances.clear()

    def test_valid_minimal_config(self):
        """Test validation passes for minimal valid config."""
        config_data = {
            "source": {"table_name": "orders", "schema_name": "public"},
            "model": {"table_name": "processed_orders", "schema_name": "analytics"},
        }
        parser = ConfigParser(config_data, "valid_model")

        validator = YAMLConfigValidator()
        result = validator.validate(parser)

        assert result.is_valid is True
        assert result.error_count == 0

    def test_missing_source_field(self):
        """Test error when source field is missing."""
        config_data = {
            "model": {"table_name": "output", "schema_name": "public"},
        }
        parser = ConfigParser(config_data, "missing_source")

        validator = YAMLConfigValidator()
        result = validator.validate(parser)

        assert result.is_valid is False
        assert any("source" in e.field_name for e in result.errors)

    def test_missing_model_field(self):
        """Test error when model field is missing."""
        config_data = {
            "source": {"table_name": "orders", "schema_name": "public"},
        }
        parser = ConfigParser(config_data, "missing_model")

        validator = YAMLConfigValidator()
        result = validator.validate(parser)

        assert result.is_valid is False
        assert any("model" in e.field_name for e in result.errors)

    def test_unknown_top_level_field_warning(self):
        """Test warning for unknown top-level fields."""
        config_data = {
            "source": {"table_name": "orders", "schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "public"},
            "unknown_field": "some_value",
        }
        parser = ConfigParser(config_data, "unknown_field_model")

        validator = YAMLConfigValidator()
        result = validator.validate(parser)

        assert result.is_valid is True  # Warnings don't invalidate
        assert any("unknown_field" in w.field_name for w in result.warnings)


class TestYAMLConfigValidatorSourceSection:
    """Tests for source section validation."""

    def setup_method(self):
        """Clear ConfigParser cache before each test."""
        ConfigParser._instances.clear()

    def test_missing_source_table_name(self):
        """Test error when source.table_name is missing."""
        config_data = {
            "source": {"schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "public"},
        }
        parser = ConfigParser(config_data, "missing_table")

        validator = YAMLConfigValidator()
        result = validator.validate(parser)

        assert result.is_valid is False
        assert any("source.table_name" in e.field_name for e in result.errors)

    def test_empty_source_table_name(self):
        """Test error when source.table_name is empty."""
        config_data = {
            "source": {"table_name": "", "schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "public"},
        }
        parser = ConfigParser(config_data, "empty_table")

        validator = YAMLConfigValidator()
        result = validator.validate(parser)

        assert result.is_valid is False
        assert any("cannot be empty" in e.message for e in result.errors)

    def test_source_not_dict_error(self):
        """Test error when source is not a dictionary."""
        config_data = {
            "source": "invalid_string",
            "model": {"table_name": "output", "schema_name": "public"},
        }
        parser = ConfigParser(config_data, "source_not_dict")

        validator = YAMLConfigValidator()
        result = validator.validate(parser)

        assert result.is_valid is False
        assert any("must be a mapping" in e.message for e in result.errors)


class TestYAMLConfigValidatorModelSection:
    """Tests for model section validation."""

    def setup_method(self):
        """Clear ConfigParser cache before each test."""
        ConfigParser._instances.clear()

    def test_missing_model_table_name(self):
        """Test error when model.table_name is missing."""
        config_data = {
            "source": {"table_name": "orders", "schema_name": "public"},
            "model": {"schema_name": "analytics"},
        }
        parser = ConfigParser(config_data, "missing_model_table")

        validator = YAMLConfigValidator()
        result = validator.validate(parser)

        assert result.is_valid is False
        assert any("model.table_name" in e.field_name for e in result.errors)

    def test_empty_model_table_name(self):
        """Test error when model.table_name is empty."""
        config_data = {
            "source": {"table_name": "orders", "schema_name": "public"},
            "model": {"table_name": "   ", "schema_name": "analytics"},
        }
        parser = ConfigParser(config_data, "empty_model_table")

        validator = YAMLConfigValidator()
        result = validator.validate(parser)

        assert result.is_valid is False
        assert any("cannot be empty" in e.message for e in result.errors)


class TestYAMLConfigValidatorTransformations:
    """Tests for transformations validation."""

    def setup_method(self):
        """Clear ConfigParser cache before each test."""
        ConfigParser._instances.clear()

    def test_valid_transformation(self):
        """Test validation passes for valid transformation."""
        config_data = {
            "source": {"table_name": "orders", "schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "public"},
            "transform": {
                "filter_active": {
                    "type": "filter",
                    "filter": {"column": "status", "operator": "equals", "value": "active"},
                }
            },
            "transform_order": ["filter_active"],
        }
        parser = ConfigParser(config_data, "valid_transform")

        validator = YAMLConfigValidator()
        result = validator.validate(parser)

        assert result.is_valid is True

    def test_missing_transform_type(self):
        """Test error when transformation type is missing."""
        config_data = {
            "source": {"table_name": "orders", "schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "public"},
            "transform": {
                "my_transform": {
                    "filter": {"column": "status"},
                }
            },
            "transform_order": ["my_transform"],
        }
        parser = ConfigParser(config_data, "missing_type")

        validator = YAMLConfigValidator()
        result = validator.validate(parser)

        assert result.is_valid is False
        assert any("type" in e.field_name for e in result.errors)

    def test_invalid_transform_type(self):
        """Test error for invalid transformation type."""
        config_data = {
            "source": {"table_name": "orders", "schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "public"},
            "transform": {
                "my_transform": {
                    "type": "invalid_type",
                }
            },
            "transform_order": ["my_transform"],
        }
        parser = ConfigParser(config_data, "invalid_type")

        validator = YAMLConfigValidator()
        result = validator.validate(parser)

        assert result.is_valid is False
        assert any("Invalid transformation type" in e.message for e in result.errors)

    def test_transform_order_references_undefined(self):
        """Test error when transform_order references undefined transformation."""
        config_data = {
            "source": {"table_name": "orders", "schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "public"},
            "transform": {
                "existing_transform": {"type": "filter"},
            },
            "transform_order": ["existing_transform", "nonexistent_transform"],
        }
        parser = ConfigParser(config_data, "undefined_ref")

        validator = YAMLConfigValidator()
        result = validator.validate(parser)

        assert result.is_valid is False
        assert any("nonexistent_transform" in e.message for e in result.errors)

    def test_transform_not_in_order_warning(self):
        """Test warning when transformation is not in transform_order."""
        config_data = {
            "source": {"table_name": "orders", "schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "public"},
            "transform": {
                "transform_a": {"type": "filter"},
                "transform_b": {"type": "join"},
            },
            "transform_order": ["transform_a"],  # transform_b not included
        }
        parser = ConfigParser(config_data, "orphan_transform")

        validator = YAMLConfigValidator()
        result = validator.validate(parser)

        assert result.is_valid is True  # Warnings don't invalidate
        assert any("transform_b" in w.message for w in result.warnings)

    def test_transform_not_dict_error(self):
        """Test error when transform section is not a dict."""
        config_data = {
            "source": {"table_name": "orders", "schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "public"},
            "transform": "not_a_dict",
        }
        parser = ConfigParser(config_data, "transform_not_dict")

        validator = YAMLConfigValidator()
        result = validator.validate(parser)

        assert result.is_valid is False
        assert any("must be a mapping" in e.message for e in result.errors)

    def test_individual_transform_not_dict_error(self):
        """Test error when individual transformation is not a dict."""
        config_data = {
            "source": {"table_name": "orders", "schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "public"},
            "transform": {
                "bad_transform": "not_a_dict",
            },
        }
        parser = ConfigParser(config_data, "individual_not_dict")

        validator = YAMLConfigValidator()
        result = validator.validate(parser)

        assert result.is_valid is False
        assert any("must be a mapping" in e.message for e in result.errors)


class TestYAMLConfigValidatorMaterialization:
    """Tests for materialization validation."""

    def setup_method(self):
        """Clear ConfigParser cache before each test."""
        ConfigParser._instances.clear()

    def test_valid_materialization_types(self):
        """Test valid materialization types pass validation."""
        valid_types = ["TABLE", "VIEW", "EPHEMERAL", "INCREMENTAL"]

        for mat_type in valid_types:
            ConfigParser._instances.clear()
            config_data = {
                "source": {
                    "table_name": "orders",
                    "schema_name": "public",
                    "materialization": mat_type,
                },
                "model": {"table_name": "output", "schema_name": "public"},
            }
            parser = ConfigParser(config_data, f"mat_{mat_type}")

            validator = YAMLConfigValidator()
            result = validator.validate(parser)

            # INCREMENTAL may have a warning about incremental_key
            assert result.is_valid is True, f"Failed for {mat_type}"

    def test_invalid_materialization_type(self):
        """Test error for invalid materialization type."""
        config_data = {
            "source": {
                "table_name": "orders",
                "schema_name": "public",
                "materialization": "INVALID_TYPE",
            },
            "model": {"table_name": "output", "schema_name": "public"},
        }
        parser = ConfigParser(config_data, "invalid_mat")

        validator = YAMLConfigValidator()
        result = validator.validate(parser)

        assert result.is_valid is False
        assert any("Invalid materialization" in e.message for e in result.errors)

    def test_incremental_without_key_warning(self):
        """Test warning when INCREMENTAL materialization lacks incremental_key."""
        config_data = {
            "source": {
                "table_name": "orders",
                "schema_name": "public",
                "materialization": "INCREMENTAL",
            },
            "model": {"table_name": "output", "schema_name": "public"},
        }
        parser = ConfigParser(config_data, "incremental_no_key")

        validator = YAMLConfigValidator()
        result = validator.validate(parser)

        assert result.is_valid is True  # Warning doesn't invalidate
        assert any("incremental_key" in w.message for w in result.warnings)

    def test_incremental_with_key_no_warning(self):
        """Test no warning when INCREMENTAL has incremental_key."""
        config_data = {
            "source": {
                "table_name": "orders",
                "schema_name": "public",
                "materialization": "INCREMENTAL",
                "incremental_key": "updated_at",
            },
            "model": {"table_name": "output", "schema_name": "public"},
        }
        parser = ConfigParser(config_data, "incremental_with_key")

        validator = YAMLConfigValidator()
        result = validator.validate(parser)

        assert result.is_valid is True
        assert not any("incremental_key" in w.message for w in result.warnings)


class TestYAMLConfigValidatorSourceMapIntegration:
    """Tests for source map integration in validation errors."""

    def setup_method(self):
        """Clear ConfigParser cache before each test."""
        ConfigParser._instances.clear()

    def test_error_includes_line_number_from_source_map(self):
        """Test that errors include line numbers from source map."""
        config_data = {
            "source": {"table_name": "orders", "schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "public"},
            "transform": {
                "my_transform": {
                    "filter": {"column": "status"},  # Missing type
                }
            },
            "transform_order": ["my_transform"],
        }
        parser = ConfigParser(config_data, "source_map_test")

        # Set up source map with line numbers
        parser.set_source_map({
            "my_transform": (15, 3),
        })

        validator = YAMLConfigValidator()
        result = validator.validate(parser)

        assert result.is_valid is False
        # Find the error for my_transform
        transform_errors = [e for e in result.errors if "my_transform" in e.field_name]
        assert len(transform_errors) > 0
        assert transform_errors[0].line_number == 15


class TestYAMLConfigValidatorStrictMode:
    """Tests for strict validation mode."""

    def setup_method(self):
        """Clear ConfigParser cache before each test."""
        ConfigParser._instances.clear()

    def test_strict_mode_raises_on_error(self):
        """Test that strict mode raises TransformationError."""
        config_data = {
            "model": {"table_name": "output", "schema_name": "public"},
            # Missing source
        }
        parser = ConfigParser(config_data, "strict_test")

        validator = YAMLConfigValidator()

        with pytest.raises(TransformationError) as exc_info:
            validator.validate_strict(parser)

        assert "source" in exc_info.value.error_message

    def test_strict_mode_passes_for_valid(self):
        """Test that strict mode doesn't raise for valid config."""
        config_data = {
            "source": {"table_name": "orders", "schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "public"},
        }
        parser = ConfigParser(config_data, "strict_valid")

        validator = YAMLConfigValidator()
        # Should not raise
        validator.validate_strict(parser)


class TestYAMLConfigValidatorCustomValidators:
    """Tests for custom validator support."""

    def setup_method(self):
        """Clear ConfigParser cache before each test."""
        ConfigParser._instances.clear()

    def test_custom_validator_adds_errors(self):
        """Test that custom validators can add errors."""
        config_data = {
            "source": {"table_name": "orders", "schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "public"},
            "custom_field": "bad_value",
        }
        parser = ConfigParser(config_data, "custom_validator")

        def custom_validator(config, model_name):
            errors = []
            if config.get("custom_field") == "bad_value":
                errors.append(
                    ValidationError(
                        field_name="custom_field",
                        message="custom_field has invalid value",
                        severity=ValidationSeverity.ERROR,
                    )
                )
            return errors

        validator = YAMLConfigValidator()
        validator.add_custom_validator(custom_validator)
        result = validator.validate(parser)

        assert result.is_valid is False
        assert any("custom_field" in e.field_name for e in result.errors)

    def test_custom_validator_exception_handled(self):
        """Test that custom validator exceptions are handled gracefully."""
        config_data = {
            "source": {"table_name": "orders", "schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "public"},
        }
        parser = ConfigParser(config_data, "exception_validator")

        def bad_validator(config, model_name):
            raise RuntimeError("Validator crashed!")

        validator = YAMLConfigValidator()
        validator.add_custom_validator(bad_validator)

        # Should not raise, just log warning
        result = validator.validate(parser)
        assert result.is_valid is True


class TestValidateConfigConvenience:
    """Tests for the validate_config convenience function."""

    def setup_method(self):
        """Clear ConfigParser cache before each test."""
        ConfigParser._instances.clear()

    def test_validate_config_returns_result(self):
        """Test validate_config returns ValidationResult."""
        config_data = {
            "source": {"table_name": "orders", "schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "public"},
        }
        parser = ConfigParser(config_data, "convenience_test")

        result = validate_config(parser)

        assert isinstance(result, ValidationResult)
        assert result.is_valid is True

    def test_validate_config_strict_raises(self):
        """Test validate_config with strict=True raises on error."""
        config_data = {
            "model": {"table_name": "output", "schema_name": "public"},
        }
        parser = ConfigParser(config_data, "convenience_strict")

        with pytest.raises(TransformationError):
            validate_config(parser, strict=True)

    def test_validate_config_strict_valid_returns_result(self):
        """Test validate_config strict mode returns result when valid."""
        config_data = {
            "source": {"table_name": "orders", "schema_name": "public"},
            "model": {"table_name": "output", "schema_name": "public"},
        }
        parser = ConfigParser(config_data, "convenience_strict_valid")

        result = validate_config(parser, strict=True)

        assert isinstance(result, ValidationResult)
        assert result.is_valid is True


class TestValidTransformationTypes:
    """Tests for all valid transformation types."""

    def setup_method(self):
        """Clear ConfigParser cache before each test."""
        ConfigParser._instances.clear()

    def test_all_valid_transformation_types(self):
        """Test that all documented transformation types are valid."""
        valid_types = [
            "join",
            "union",
            "pivot",
            "filter",
            "rename_column",
            "combine_columns",
            "synthesize",
            "groups_and_aggregation",
            "find_and_replace",
            "distinct",
            "window",
            "sql",
            "python",
        ]

        for transform_type in valid_types:
            ConfigParser._instances.clear()
            config_data = {
                "source": {"table_name": "orders", "schema_name": "public"},
                "model": {"table_name": "output", "schema_name": "public"},
                "transform": {
                    "test_transform": {"type": transform_type},
                },
                "transform_order": ["test_transform"],
            }
            parser = ConfigParser(config_data, f"type_{transform_type}")

            validator = YAMLConfigValidator()
            result = validator.validate(parser)

            assert result.is_valid is True, f"Type '{transform_type}' should be valid"
