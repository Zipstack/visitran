"""
YAML Validation Layer

This module provides validation for transformation configuration YAML files,
ensuring all required fields are present and correctly structured before
execution begins.

Usage:
    validator = YAMLConfigValidator()

    # Validate a ConfigParser instance
    errors = validator.validate(config_parser)

    # Or use strict mode that raises on first error
    validator.validate_strict(config_parser)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional

from visitran.errors import TransformationError

if TYPE_CHECKING:
    from backend.application.config_parser.config_parser import ConfigParser

logger = logging.getLogger(__name__)


class ValidationSeverity(Enum):
    """Severity levels for validation errors."""

    ERROR = "error"  # Must fix - blocks execution
    WARNING = "warning"  # Should fix - may cause issues
    INFO = "info"  # Informational - best practice suggestions


@dataclass
class ValidationError:
    """Represents a validation error with context."""

    field_name: str
    message: str
    severity: ValidationSeverity = ValidationSeverity.ERROR
    line_number: Optional[int] = None
    column_number: Optional[int] = None
    suggested_fix: Optional[str] = None

    def to_transformation_error(
        self, model_name: str, yaml_content: Optional[str] = None
    ) -> TransformationError:
        """Convert to a TransformationError exception."""
        snippet = ""
        if yaml_content and self.line_number:
            snippet = TransformationError.extract_yaml_snippet(
                yaml_content, self.line_number, self.column_number or 1
            )

        return TransformationError(
            model_name=model_name,
            transformation_id=f"validation_{self.field_name}",
            line_number=self.line_number or 1,
            column_number=self.column_number or 1,
            error_message=self.message,
            yaml_snippet=snippet,
            suggested_fix=self.suggested_fix,
        )


@dataclass
class ValidationResult:
    """Result of configuration validation."""

    is_valid: bool
    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[ValidationError] = field(default_factory=list)

    @property
    def error_count(self) -> int:
        """Number of blocking errors."""
        return len(self.errors)

    @property
    def warning_count(self) -> int:
        """Number of warnings."""
        return len(self.warnings)

    def has_errors(self) -> bool:
        """Check if there are any blocking errors."""
        return len(self.errors) > 0

    def has_warnings(self) -> bool:
        """Check if there are any warnings."""
        return len(self.warnings) > 0


class YAMLConfigValidator:
    """
    Validates transformation configuration YAML files.

    Checks for:
    - Required top-level fields (source, model)
    - Transformation structure and required fields
    - Field type validation
    - Optional field validation when present

    The validator should be called early in the transformation pipeline,
    after YAML parsing but before execution planning.
    """

    # Required top-level fields
    REQUIRED_TOP_LEVEL_FIELDS = {"source", "model"}

    # Optional top-level fields (no error if missing)
    OPTIONAL_TOP_LEVEL_FIELDS = {
        "transform",
        "transform_order",
        "reference",
        "presentation",
        "materialization",
        "dependencies",
    }

    # Required fields within source section
    REQUIRED_SOURCE_FIELDS = {"table_name"}

    # Required fields within model section
    REQUIRED_MODEL_FIELDS = {"table_name"}

    # Required fields for each transformation
    REQUIRED_TRANSFORM_FIELDS = {"type"}

    # Valid transformation types
    VALID_TRANSFORMATION_TYPES = {
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
    }

    # Valid materialization types
    VALID_MATERIALIZATION_TYPES = {"TABLE", "VIEW", "EPHEMERAL", "INCREMENTAL"}

    def __init__(self) -> None:
        """Initialize the validator."""
        self._custom_validators: list[callable] = []

    def add_custom_validator(self, validator_func: callable) -> None:
        """
        Add a custom validation function.

        Args:
            validator_func: Function taking (config_data, model_name) and returning
                          list of ValidationError objects
        """
        self._custom_validators.append(validator_func)

    def validate(self, parser: ConfigParser) -> ValidationResult:
        """
        Validate a ConfigParser instance.

        Args:
            parser: The ConfigParser to validate

        Returns:
            ValidationResult with any errors and warnings found
        """
        errors: list[ValidationError] = []
        warnings: list[ValidationError] = []

        config_data = parser._config_data
        model_name = parser.model_name

        # Validate top-level structure
        self._validate_top_level(config_data, errors, warnings, parser)

        # Validate source section
        if "source" in config_data:
            self._validate_source_section(
                config_data["source"], errors, warnings, parser
            )

        # Validate model section
        if "model" in config_data:
            self._validate_model_section(
                config_data["model"], errors, warnings, parser
            )

        # Validate transformations
        if "transform" in config_data:
            self._validate_transformations(
                config_data["transform"],
                config_data.get("transform_order", []),
                errors,
                warnings,
                parser,
            )

        # Validate materialization if present
        if "source" in config_data and "materialization" in config_data["source"]:
            self._validate_materialization(
                config_data["source"]["materialization"], errors, warnings, parser
            )

        # Run custom validators
        for custom_validator in self._custom_validators:
            try:
                custom_errors = custom_validator(config_data, model_name)
                for error in custom_errors:
                    if error.severity == ValidationSeverity.ERROR:
                        errors.append(error)
                    else:
                        warnings.append(error)
            except Exception as e:
                logger.warning(f"Custom validator failed: {e}")

        is_valid = len(errors) == 0
        return ValidationResult(is_valid=is_valid, errors=errors, warnings=warnings)

    def validate_strict(self, parser: ConfigParser) -> None:
        """
        Validate and raise TransformationError on first error.

        Args:
            parser: The ConfigParser to validate

        Raises:
            TransformationError: If any validation errors are found
        """
        result = self.validate(parser)

        if result.has_errors():
            first_error = result.errors[0]
            raise first_error.to_transformation_error(
                parser.model_name, parser.yaml_content
            )

    def _validate_top_level(
        self,
        config_data: dict[str, Any],
        errors: list[ValidationError],
        warnings: list[ValidationError],
        parser: ConfigParser,
    ) -> None:
        """Validate top-level required fields."""
        for field_name in self.REQUIRED_TOP_LEVEL_FIELDS:
            if field_name not in config_data:
                errors.append(
                    ValidationError(
                        field_name=field_name,
                        message=f"Missing required top-level field: '{field_name}'",
                        severity=ValidationSeverity.ERROR,
                        line_number=parser.get_source_location(field_name)[0]
                        if parser.has_source_location(field_name)
                        else 1,
                        suggested_fix=f"Add '{field_name}:' section to your configuration",
                    )
                )

        # Check for unknown top-level fields
        all_known_fields = self.REQUIRED_TOP_LEVEL_FIELDS | self.OPTIONAL_TOP_LEVEL_FIELDS
        for field_name in config_data:
            if field_name not in all_known_fields:
                warnings.append(
                    ValidationError(
                        field_name=field_name,
                        message=f"Unknown top-level field: '{field_name}'",
                        severity=ValidationSeverity.WARNING,
                        suggested_fix=f"Remove '{field_name}' or check for typos",
                    )
                )

    def _validate_source_section(
        self,
        source_data: dict[str, Any],
        errors: list[ValidationError],
        warnings: list[ValidationError],
        parser: ConfigParser,
    ) -> None:
        """Validate the source section."""
        if not isinstance(source_data, dict):
            errors.append(
                ValidationError(
                    field_name="source",
                    message="'source' must be a mapping/dictionary",
                    severity=ValidationSeverity.ERROR,
                )
            )
            return

        for field_name in self.REQUIRED_SOURCE_FIELDS:
            if field_name not in source_data:
                errors.append(
                    ValidationError(
                        field_name=f"source.{field_name}",
                        message=f"Missing required field in source: '{field_name}'",
                        severity=ValidationSeverity.ERROR,
                        suggested_fix=f"Add '{field_name}:' under the 'source:' section",
                    )
                )

        # Validate table_name is not empty
        if "table_name" in source_data:
            table_name = source_data["table_name"]
            if not table_name or (isinstance(table_name, str) and not table_name.strip()):
                errors.append(
                    ValidationError(
                        field_name="source.table_name",
                        message="'source.table_name' cannot be empty",
                        severity=ValidationSeverity.ERROR,
                    )
                )

    def _validate_model_section(
        self,
        model_data: dict[str, Any],
        errors: list[ValidationError],
        warnings: list[ValidationError],
        parser: ConfigParser,
    ) -> None:
        """Validate the model section."""
        if not isinstance(model_data, dict):
            errors.append(
                ValidationError(
                    field_name="model",
                    message="'model' must be a mapping/dictionary",
                    severity=ValidationSeverity.ERROR,
                )
            )
            return

        for field_name in self.REQUIRED_MODEL_FIELDS:
            if field_name not in model_data:
                errors.append(
                    ValidationError(
                        field_name=f"model.{field_name}",
                        message=f"Missing required field in model: '{field_name}'",
                        severity=ValidationSeverity.ERROR,
                        suggested_fix=f"Add '{field_name}:' under the 'model:' section",
                    )
                )

        # Validate table_name is not empty
        if "table_name" in model_data:
            table_name = model_data["table_name"]
            if not table_name or (isinstance(table_name, str) and not table_name.strip()):
                errors.append(
                    ValidationError(
                        field_name="model.table_name",
                        message="'model.table_name' cannot be empty",
                        severity=ValidationSeverity.ERROR,
                    )
                )

    def _validate_transformations(
        self,
        transform_data: dict[str, Any],
        transform_order: list[str],
        errors: list[ValidationError],
        warnings: list[ValidationError],
        parser: ConfigParser,
    ) -> None:
        """Validate the transformations section."""
        if not isinstance(transform_data, dict):
            errors.append(
                ValidationError(
                    field_name="transform",
                    message="'transform' must be a mapping of transformation IDs to configs",
                    severity=ValidationSeverity.ERROR,
                )
            )
            return

        # Validate each transformation
        for transform_id, transform_config in transform_data.items():
            if not isinstance(transform_config, dict):
                errors.append(
                    ValidationError(
                        field_name=f"transform.{transform_id}",
                        message=f"Transformation '{transform_id}' must be a mapping",
                        severity=ValidationSeverity.ERROR,
                        line_number=parser.get_source_location(transform_id)[0]
                        if parser.has_source_location(transform_id)
                        else None,
                    )
                )
                continue

            # Check required fields
            for field_name in self.REQUIRED_TRANSFORM_FIELDS:
                if field_name not in transform_config:
                    errors.append(
                        ValidationError(
                            field_name=f"transform.{transform_id}.{field_name}",
                            message=f"Transformation '{transform_id}' missing required field: '{field_name}'",
                            severity=ValidationSeverity.ERROR,
                            line_number=parser.get_source_location(transform_id)[0]
                            if parser.has_source_location(transform_id)
                            else None,
                            suggested_fix=f"Add '{field_name}:' to transformation '{transform_id}'",
                        )
                    )

            # Validate transformation type
            if "type" in transform_config:
                transform_type = transform_config["type"]
                if transform_type not in self.VALID_TRANSFORMATION_TYPES:
                    errors.append(
                        ValidationError(
                            field_name=f"transform.{transform_id}.type",
                            message=f"Invalid transformation type '{transform_type}' in '{transform_id}'",
                            severity=ValidationSeverity.ERROR,
                            suggested_fix=f"Valid types: {', '.join(sorted(self.VALID_TRANSFORMATION_TYPES))}",
                        )
                    )

        # Check transform_order references valid transformations
        for transform_id in transform_order:
            if transform_id not in transform_data:
                errors.append(
                    ValidationError(
                        field_name="transform_order",
                        message=f"transform_order references undefined transformation: '{transform_id}'",
                        severity=ValidationSeverity.ERROR,
                        suggested_fix=f"Define '{transform_id}' in the 'transform:' section or remove from 'transform_order'",
                    )
                )

        # Warn about transformations not in transform_order
        for transform_id in transform_data:
            if transform_order and transform_id not in transform_order:
                warnings.append(
                    ValidationError(
                        field_name=f"transform.{transform_id}",
                        message=f"Transformation '{transform_id}' is defined but not in 'transform_order'",
                        severity=ValidationSeverity.WARNING,
                        suggested_fix=f"Add '{transform_id}' to 'transform_order' or remove the transformation",
                    )
                )

    def _validate_materialization(
        self,
        materialization: str,
        errors: list[ValidationError],
        warnings: list[ValidationError],
        parser: ConfigParser,
    ) -> None:
        """Validate materialization type."""
        if materialization not in self.VALID_MATERIALIZATION_TYPES:
            errors.append(
                ValidationError(
                    field_name="source.materialization",
                    message=f"Invalid materialization type: '{materialization}'",
                    severity=ValidationSeverity.ERROR,
                    suggested_fix=f"Valid types: {', '.join(sorted(self.VALID_MATERIALIZATION_TYPES))}",
                )
            )

        # Warn about incremental without incremental_key
        if materialization == "INCREMENTAL":
            source_data = parser.get("source", {})
            if "incremental_key" not in source_data:
                warnings.append(
                    ValidationError(
                        field_name="source.incremental_key",
                        message="INCREMENTAL materialization typically requires 'incremental_key'",
                        severity=ValidationSeverity.WARNING,
                        suggested_fix="Add 'incremental_key:' to specify the column for incremental updates",
                    )
                )


def validate_config(parser: ConfigParser, strict: bool = False) -> ValidationResult:
    """
    Convenience function to validate a ConfigParser.

    Args:
        parser: The ConfigParser to validate
        strict: If True, raise TransformationError on first error

    Returns:
        ValidationResult (only if strict=False)

    Raises:
        TransformationError: If strict=True and validation fails
    """
    validator = YAMLConfigValidator()

    if strict:
        validator.validate_strict(parser)
        return ValidationResult(is_valid=True)

    return validator.validate(parser)
