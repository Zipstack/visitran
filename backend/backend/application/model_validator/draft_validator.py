"""Draft-specific validation layer.

Provides YAML syntax checking with line-number error reporting and
structural schema validation for draft model_data.
"""

import logging
import time
from typing import Any

import yaml

logger = logging.getLogger(__name__)

# Required top-level keys in model_data
REQUIRED_TOP_LEVEL_KEYS = {"model", "source"}

# Keys expected in the 'model' section
REQUIRED_MODEL_KEYS = {"schema_name", "table_name"}

# Keys expected in the 'source' section
REQUIRED_SOURCE_KEYS = {"schema_name", "table_name"}

# Known transformation types
KNOWN_TRANSFORM_TYPES = {
    "filter",
    "join",
    "merge",
    "pivot",
    "unpivot",
    "rename_column",
    "synthesize",
    "distinct",
    "find_and_replace",
    "combine_columns",
    "groups_and_aggregation",
}


class DraftValidationError:
    """Structured error returned from draft validation."""

    def __init__(
        self,
        field: str,
        error_type: str,
        message: str,
        line: int | None = None,
    ):
        self.field = field
        self.error_type = error_type
        self.message = message
        self.line = line

    def to_dict(self) -> dict[str, Any]:
        result = {
            "field": self.field,
            "error_type": self.error_type,
            "message": self.message,
        }
        if self.line is not None:
            result["line"] = self.line
        return result


class DraftValidator:
    """Validates draft content before save operations.

    Usage::

        validator = DraftValidator(model_data_or_yaml)
        result = validator.validate()
        if not result["is_valid"]:
            # result["errors"] contains list of DraftValidationError dicts
    """

    def __init__(self, content: dict[str, Any] | str):
        """Accept either parsed dict (model_data) or raw YAML string."""
        self._raw_yaml: str | None = None
        self._parsed_data: dict[str, Any] | None = None

        if isinstance(content, str):
            self._raw_yaml = content
        elif isinstance(content, dict):
            self._parsed_data = content
        else:
            self._parsed_data = {}

    def validate(self) -> dict[str, Any]:
        """Run all validation checks and return result."""
        start = time.monotonic()
        errors: list[DraftValidationError] = []

        # Step 1: YAML syntax validation (if raw string provided)
        if self._raw_yaml is not None:
            yaml_errors = self._validate_yaml_syntax()
            if yaml_errors:
                errors.extend(yaml_errors)
                elapsed = (time.monotonic() - start) * 1000
                return self._build_result(errors, elapsed)

        # Step 2: Structural schema validation
        if self._parsed_data is not None:
            schema_errors = self._validate_schema()
            errors.extend(schema_errors)

            # Step 3: Transformation structure validation
            if not schema_errors:
                transform_errors = self._validate_transformations()
                errors.extend(transform_errors)

        elapsed = (time.monotonic() - start) * 1000
        return self._build_result(errors, elapsed)

    def _validate_yaml_syntax(self) -> list[DraftValidationError]:
        """Parse YAML and report syntax errors with line numbers."""
        errors = []
        try:
            self._parsed_data = yaml.safe_load(self._raw_yaml)
            if not isinstance(self._parsed_data, dict):
                errors.append(DraftValidationError(
                    field="root",
                    error_type="type_error",
                    message="YAML content must parse to a dictionary, "
                            f"got {type(self._parsed_data).__name__}.",
                ))
                self._parsed_data = None
        except yaml.YAMLError as exc:
            line = None
            if hasattr(exc, "problem_mark") and exc.problem_mark is not None:
                line = exc.problem_mark.line + 1
            errors.append(DraftValidationError(
                field="yaml",
                error_type="syntax_error",
                message=str(exc),
                line=line,
            ))
        return errors

    def _validate_schema(self) -> list[DraftValidationError]:
        """Validate top-level structure and required fields."""
        errors = []
        data = self._parsed_data or {}

        for key in REQUIRED_TOP_LEVEL_KEYS:
            if key not in data:
                errors.append(DraftValidationError(
                    field=key,
                    error_type="missing_field",
                    message=f"Required field '{key}' is missing.",
                ))

        model_section = data.get("model")
        if isinstance(model_section, dict):
            for key in REQUIRED_MODEL_KEYS:
                if key not in model_section:
                    errors.append(DraftValidationError(
                        field=f"model.{key}",
                        error_type="missing_field",
                        message=f"Required field 'model.{key}' is missing.",
                    ))
        elif model_section is not None:
            errors.append(DraftValidationError(
                field="model",
                error_type="type_error",
                message="'model' must be a dictionary.",
            ))

        source_section = data.get("source")
        if isinstance(source_section, dict):
            for key in REQUIRED_SOURCE_KEYS:
                if key not in source_section:
                    errors.append(DraftValidationError(
                        field=f"source.{key}",
                        error_type="missing_field",
                        message=f"Required field 'source.{key}' is missing.",
                    ))
        elif source_section is not None:
            errors.append(DraftValidationError(
                field="source",
                error_type="type_error",
                message="'source' must be a dictionary.",
            ))

        transform = data.get("transform", {})
        transform_order = data.get("transform_order", [])
        if isinstance(transform, dict) and isinstance(transform_order, list):
            for step_id in transform_order:
                if step_id not in transform:
                    errors.append(DraftValidationError(
                        field=f"transform_order.{step_id}",
                        error_type="reference_error",
                        message=f"Step '{step_id}' in transform_order "
                                f"not found in transform dictionary.",
                    ))

        return errors

    def _validate_transformations(self) -> list[DraftValidationError]:
        """Validate individual transformation entries."""
        errors = []
        data = self._parsed_data or {}
        transform = data.get("transform", {})

        if not isinstance(transform, dict):
            return errors

        for step_id, step_config in transform.items():
            if not isinstance(step_config, dict):
                errors.append(DraftValidationError(
                    field=f"transform.{step_id}",
                    error_type="type_error",
                    message=f"Transformation '{step_id}' must be a dictionary.",
                ))
                continue

            step_type = step_config.get("type")
            if not step_type:
                errors.append(DraftValidationError(
                    field=f"transform.{step_id}.type",
                    error_type="missing_field",
                    message=f"Transformation '{step_id}' is missing "
                            f"required 'type' field.",
                ))
            elif step_type not in KNOWN_TRANSFORM_TYPES:
                errors.append(DraftValidationError(
                    field=f"transform.{step_id}.type",
                    error_type="invalid_value",
                    message=f"Unknown transformation type '{step_type}'. "
                            f"Must be one of: {', '.join(sorted(KNOWN_TRANSFORM_TYPES))}.",
                ))

        return errors

    @staticmethod
    def _build_result(
        errors: list[DraftValidationError],
        elapsed_ms: float,
    ) -> dict[str, Any]:
        return {
            "is_valid": len(errors) == 0,
            "errors": [e.to_dict() for e in errors],
            "validation_time_ms": round(elapsed_ms, 2),
        }
