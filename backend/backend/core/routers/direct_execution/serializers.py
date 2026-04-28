"""
Serializers and Schema Validators for Direct Execution API.

Provides JSON schemas for request/response validation as per VTR-064.

These serializers define the structure of API requests and responses,
enabling validation and documentation generation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from rest_framework import serializers


# =============================================================================
# Enums
# =============================================================================


class ExecutionModeEnum(str, Enum):
    """Execution mode options."""
    LEGACY = "legacy"
    DIRECT = "direct"
    PARALLEL = "parallel"


class ValidationStatusEnum(str, Enum):
    """Validation status options."""
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"


class DAGExecutionStatusEnum(str, Enum):
    """DAG execution status options."""
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


# =============================================================================
# VTR-060: Model Execution Serializers
# =============================================================================


class ExecuteModelRequestSerializer(serializers.Serializer):
    """Request serializer for POST /api/v1/models/{schema}/{model}/execute"""

    incremental = serializers.BooleanField(
        required=False,
        default=False,
        help_text="Trigger incremental execution mode",
    )
    override_feature_flag = serializers.BooleanField(
        required=False,
        default=False,
        help_text="Bypass ENABLE_DIRECT_EXECUTION flag for testing",
    )


class YAMLLocationSerializer(serializers.Serializer):
    """YAML source location details."""

    file = serializers.CharField(
        required=False,
        allow_null=True,
        help_text="Path to the YAML file",
    )
    line = serializers.IntegerField(
        required=False,
        allow_null=True,
        help_text="Line number (1-based)",
    )
    column = serializers.IntegerField(
        required=False,
        allow_null=True,
        help_text="Column number (1-based)",
    )


class ValidationDetailsSerializer(serializers.Serializer):
    """Validation comparison details."""

    dbt_sql = serializers.CharField(help_text="SQL from legacy/dbt path")
    ibis_sql = serializers.CharField(help_text="SQL from Ibis direct path")
    match_status = serializers.BooleanField(help_text="Whether SQL outputs match")
    discrepancy_details = serializers.CharField(
        required=False,
        allow_null=True,
        help_text="Details about mismatches",
    )


class ExecuteModelResponseSerializer(serializers.Serializer):
    """Response serializer for model execution success."""

    execution_id = serializers.UUIDField(help_text="Unique execution identifier")
    schema = serializers.CharField(help_text="Schema name")
    model = serializers.CharField(help_text="Model name")
    generated_sql = serializers.CharField(help_text="Generated SQL from Ibis")
    validation_status = serializers.ChoiceField(
        choices=[s.value for s in ValidationStatusEnum],
        help_text="Validation result status",
    )
    validation_details = ValidationDetailsSerializer(
        required=False,
        allow_null=True,
        help_text="Validation comparison details (if validation ran)",
    )
    execution_time_ms = serializers.FloatField(help_text="Execution time in ms")
    rows_affected = serializers.IntegerField(
        required=False,
        allow_null=True,
        help_text="Number of rows affected",
    )


class ExecuteModelErrorSerializer(serializers.Serializer):
    """Error response for model execution failures."""

    error = serializers.CharField(help_text="Error type")
    error_code = serializers.CharField(help_text="Unique error code")
    model = serializers.CharField(help_text="Model that failed")
    message = serializers.CharField(help_text="Human-readable error message")
    yaml_location = YAMLLocationSerializer(
        required=False,
        allow_null=True,
        help_text="YAML source location",
    )
    yaml_snippet = serializers.CharField(
        required=False,
        allow_null=True,
        help_text="3-line YAML context with error marker",
    )
    suggested_fix = serializers.CharField(
        required=False,
        allow_null=True,
        help_text="Actionable fix recommendation",
    )
    execution_id = serializers.UUIDField(
        required=False,
        allow_null=True,
        help_text="Execution ID for tracking",
    )
    execution_time_ms = serializers.FloatField(
        required=False,
        help_text="Time before failure in ms",
    )


# =============================================================================
# VTR-061: Validation Results Serializers
# =============================================================================


class PaginationSerializer(serializers.Serializer):
    """Pagination metadata."""

    total_count = serializers.IntegerField(help_text="Total number of results")
    page = serializers.IntegerField(help_text="Current page number")
    page_size = serializers.IntegerField(help_text="Results per page")
    has_next = serializers.BooleanField(help_text="Whether more pages exist")
    has_previous = serializers.BooleanField(help_text="Whether previous pages exist")


class ValidationResultSerializer(serializers.Serializer):
    """Single validation result."""

    model_name = serializers.CharField(help_text="Model name (schema.model)")
    legacy_sql = serializers.CharField(help_text="SQL from legacy path")
    direct_sql = serializers.CharField(help_text="SQL from direct path")
    match_status = serializers.BooleanField(help_text="Whether SQL matched")
    diff = serializers.CharField(
        required=False,
        allow_null=True,
        help_text="Diff output for mismatches",
    )
    timestamp = serializers.DateTimeField(help_text="When validation was performed")
    execution_id = serializers.CharField(
        required=False,
        help_text="Execution ID if part of DAG run",
    )


class ValidationResultsListSerializer(serializers.Serializer):
    """Response for validation results list."""

    results = ValidationResultSerializer(many=True)
    pagination = PaginationSerializer()


# =============================================================================
# VTR-062: DAG Execution Serializers
# =============================================================================


class ExecuteDAGRequestSerializer(serializers.Serializer):
    """Request for DAG execution."""

    schema = serializers.CharField(
        required=False,
        allow_null=True,
        help_text="Optional schema filter",
    )
    execution_mode = serializers.ChoiceField(
        choices=[m.value for m in ExecutionModeEnum],
        default=ExecutionModeEnum.DIRECT.value,
        help_text="Execution mode to use",
    )


class ModelExecutionResultSerializer(serializers.Serializer):
    """Result for a single model execution within DAG."""

    model_name = serializers.CharField(help_text="Schema.model name")
    execution_order = serializers.IntegerField(help_text="Position in topological order")
    sql_output = serializers.CharField(
        required=False,
        allow_null=True,
        help_text="Generated SQL",
    )
    validation_status = serializers.ChoiceField(
        choices=[s.value for s in ValidationStatusEnum],
        help_text="Validation result",
    )
    validation_message = serializers.CharField(
        required=False,
        allow_null=True,
        help_text="Validation failure details",
    )
    execution_time_ms = serializers.FloatField(help_text="Model execution time")
    status = serializers.CharField(help_text="Execution status")
    error = serializers.CharField(
        required=False,
        allow_null=True,
        help_text="Error message if failed",
    )


class ValidationSummarySerializer(serializers.Serializer):
    """Validation summary for DAG execution."""

    total_validated = serializers.IntegerField()
    matched = serializers.IntegerField()
    mismatched = serializers.IntegerField()
    match_rate = serializers.FloatField()


class ExecuteDAGResponseSerializer(serializers.Serializer):
    """Response for DAG execution."""

    execution_id = serializers.UUIDField()
    execution_mode = serializers.ChoiceField(
        choices=[m.value for m in ExecutionModeEnum],
    )
    schema_filter = serializers.CharField(required=False, allow_null=True)
    total_models = serializers.IntegerField()
    successful_count = serializers.IntegerField()
    failed_count = serializers.IntegerField()
    skipped_count = serializers.IntegerField()
    total_time_ms = serializers.FloatField()
    validation_summary = ValidationSummarySerializer(required=False, allow_null=True)
    per_model_results = ModelExecutionResultSerializer(many=True)


class DAGStatusResponseSerializer(serializers.Serializer):
    """Response for DAG status check."""

    execution_id = serializers.UUIDField()
    status = serializers.ChoiceField(
        choices=[s.value for s in DAGExecutionStatusEnum],
    )
    models_completed = serializers.IntegerField()
    models_total = serializers.IntegerField()
    current_model = serializers.CharField(required=False, allow_null=True)
    started_at = serializers.DateTimeField(required=False, allow_null=True)


# =============================================================================
# VTR-063: Registry Inspection Serializers
# =============================================================================


class ModelMetadataSerializer(serializers.Serializer):
    """Model metadata from registry."""

    table_name = serializers.CharField()
    schema_name = serializers.CharField()
    materialization_type = serializers.CharField()
    execution_status = serializers.CharField()
    config_loaded = serializers.BooleanField()


class RegistryModelSerializer(serializers.Serializer):
    """Model entry in registry list."""

    key = serializers.CharField()
    schema = serializers.CharField()
    model = serializers.CharField()
    table_name = serializers.CharField(required=False)
    materialization_type = serializers.CharField(required=False)
    execution_status = serializers.CharField(required=False)
    config_loaded = serializers.BooleanField(required=False)
    error = serializers.CharField(required=False, allow_null=True)


class CacheStatusSerializer(serializers.Serializer):
    """Cache status metadata."""

    cached_tables = serializers.IntegerField()
    is_locked = serializers.BooleanField()


class RegistryModelsListSerializer(serializers.Serializer):
    """Response for registry models list."""

    models = RegistryModelSerializer(many=True)
    total_count = serializers.IntegerField()
    cache_status = CacheStatusSerializer()
    last_modified = serializers.DateTimeField()


class DAGNodeSerializer(serializers.Serializer):
    """Node in DAG visualization."""

    id = serializers.CharField()
    schema = serializers.CharField()
    model = serializers.CharField()


class DAGEdgeSerializer(serializers.Serializer):
    """Edge in DAG visualization."""

    source = serializers.CharField()
    target = serializers.CharField()


class RegistryDAGSerializer(serializers.Serializer):
    """Response for DAG visualization."""

    schema = serializers.CharField()
    nodes = DAGNodeSerializer(many=True)
    edges = DAGEdgeSerializer(many=True)
    node_count = serializers.IntegerField()
    edge_count = serializers.IntegerField()
    cache_status = serializers.CharField()
    last_modified = serializers.DateTimeField()


class ConfigDetailsSerializer(serializers.Serializer):
    """ConfigParser configuration details."""

    destination_schema = serializers.CharField()
    destination_table = serializers.CharField()
    source_schema = serializers.CharField(required=False, allow_null=True)
    source_table = serializers.CharField(required=False, allow_null=True)
    materialization = serializers.CharField()


class RegistryModelDetailSerializer(serializers.Serializer):
    """Response for model detail view."""

    schema = serializers.CharField()
    model = serializers.CharField()
    qualified_name = serializers.CharField()
    metadata = ModelMetadataSerializer()
    config = ConfigDetailsSerializer()
    yaml_location = YAMLLocationSerializer(required=False, allow_null=True)
    cache_status = serializers.CharField()
    last_modified = serializers.DateTimeField()


# =============================================================================
# VTR-064: Error Response Serializers
# =============================================================================


class StandardErrorSerializer(serializers.Serializer):
    """Standard error response format."""

    error_code = serializers.CharField(help_text="Unique error identifier")
    message = serializers.CharField(help_text="Human-readable error description")
    yaml_location = serializers.CharField(
        required=False,
        help_text="file:line:column format",
    )
    suggested_fix = serializers.CharField(
        required=False,
        help_text="Actionable remediation guidance",
    )
    timestamp = serializers.DateTimeField(default=datetime.utcnow)


# =============================================================================
# Schema Definitions for OpenAPI
# =============================================================================

# These are used for generating OpenAPI documentation

OPENAPI_SCHEMAS = {
    "ExecuteModelRequest": {
        "type": "object",
        "properties": {
            "incremental": {
                "type": "boolean",
                "default": False,
                "description": "Trigger incremental execution mode",
            },
            "override_feature_flag": {
                "type": "boolean",
                "default": False,
                "description": "Bypass ENABLE_DIRECT_EXECUTION flag",
            },
        },
    },
    "ExecuteModelResponse": {
        "type": "object",
        "required": ["execution_id", "schema", "model", "generated_sql", "validation_status", "execution_time_ms"],
        "properties": {
            "execution_id": {"type": "string", "format": "uuid"},
            "schema": {"type": "string"},
            "model": {"type": "string"},
            "generated_sql": {"type": "string"},
            "validation_status": {"type": "string", "enum": ["passed", "failed", "skipped"]},
            "validation_details": {"$ref": "#/components/schemas/ValidationDetails"},
            "execution_time_ms": {"type": "number"},
            "rows_affected": {"type": "integer", "nullable": True},
        },
    },
    "ValidationDetails": {
        "type": "object",
        "properties": {
            "dbt_sql": {"type": "string"},
            "ibis_sql": {"type": "string"},
            "match_status": {"type": "boolean"},
            "discrepancy_details": {"type": "string", "nullable": True},
        },
    },
    "StandardError": {
        "type": "object",
        "required": ["error_code", "message"],
        "properties": {
            "error_code": {"type": "string"},
            "message": {"type": "string"},
            "yaml_location": {"type": "string"},
            "suggested_fix": {"type": "string"},
            "timestamp": {"type": "string", "format": "date-time"},
        },
    },
}
