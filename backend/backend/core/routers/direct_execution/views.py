"""
Views for Direct Execution API endpoints.

Provides REST API endpoints for:
- VTR-060: Model Execution API with Error Handling
- VTR-061: SQL Validation Results API
- VTR-062: DAG Execution API
- VTR-063: Model Registry Inspection Endpoints
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime
from typing import Any, Optional

from django.utils import timezone
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response

from backend.application.config_parser.dag_builder import DAGBuilder
from backend.application.config_parser.dag_executor import (
    DAGExecutor,
    DAGExecutionResult,
    ExecutionStatus,
)
from backend.application.config_parser.feature_flags import (
    ExecutionMode,
    ExecutionRouter,
    FeatureFlags,
)
from backend.application.config_parser.ibis_builder import IbisBuildError
from backend.application.config_parser.model_registry import ModelRegistry
from backend.application.config_parser.sql_validator import (
    ValidationResult,
    validate_sql_equivalence,
)
try:
    from backend.application.config_parser.validation_storage_service import (
        ValidationStorageService,
        get_validation_storage_service,
    )
except ImportError:
    ValidationStorageService = None  # type: ignore
    get_validation_storage_service = None  # type: ignore
from backend.core.utils import handle_http_request
from backend.utils.constants import HTTPMethods
from visitran.errors import TransformationError

logger = logging.getLogger(__name__)


def _inject_join_table_columns(config_parser, project_id: str) -> None:
    """
    Inject column metadata for joined tables into the config parser.

    This enables the legacy-compatible column aliasing (rname pattern) where
    joined table columns are prefixed with table name to avoid column name
    conflicts when both tables have columns with the same name (e.g., 'id').

    Args:
        config_parser: The ConfigParser instance to inject columns into
        project_id: The project ID for database connection
    """
    from backend.application.config_parser.transformation_parsers.join_parser import JoinParsers
    from backend.application.context.application import ApplicationContext

    # Get transforms to check for joins
    transforms = config_parser.transform_parser.get_transforms()

    # Find join transforms
    join_tables = []
    for transform in transforms:
        if isinstance(transform, JoinParsers):
            for join_parser in transform.get_joins():
                rhs_schema = join_parser.rhs_schema_name
                rhs_table = join_parser.rhs_table_name
                if rhs_table:
                    join_tables.append((rhs_schema, rhs_table))

    if not join_tables:
        return  # No joins, nothing to inject

    # Create ApplicationContext to query database
    try:
        app = ApplicationContext(project_id=project_id)

        for schema, table in join_tables:
            # Query information_schema for column names
            if schema:
                query = f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = '{schema}' AND table_name = '{table}'
                    ORDER BY ordinal_position
                """
            else:
                query = f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{table}'
                    ORDER BY ordinal_position
                """

            try:
                result = app.execute_sql_command(sql_command=query, limit=1000)

                columns = []
                if result:
                    if isinstance(result, dict) and 'rows' in result:
                        # Result format: {'columns': [...], 'rows': [(...), ...]}
                        columns = [row[0] for row in result['rows'] if row and row[0]]
                    elif isinstance(result, list):
                        # Result format: [{'column_name': 'col1'}, ...]
                        columns = [row.get("column_name") for row in result if row.get("column_name")]

                if columns:
                    config_parser.set_join_table_columns(schema or "", table, columns)
                    logger.debug(f"Injected {len(columns)} columns for {schema}.{table}")
            except Exception as e:
                logger.warning(f"Failed to get columns for {schema}.{table}: {e}")

        # Close connection after querying
        app.visitran_context.close_db_connection()

    except Exception as e:
        logger.warning(f"Failed to create ApplicationContext for column injection: {e}")


def _inject_dialect(config_parser, project_id: str) -> None:
    """
    Inject the database dialect into the config parser.

    This enables database-specific SQL generation (e.g., BigQuery supports
    SELECT * REPLACE, PostgreSQL needs explicit column lists).

    Args:
        config_parser: The ConfigParser instance to inject dialect into
        project_id: The project ID for database connection
    """
    from backend.application.context.application import ApplicationContext

    # Skip if already set
    if config_parser.get_dialect():
        return

    try:
        app = ApplicationContext(project_id=project_id)
        # Get database type from project configuration
        db_type = getattr(app.visitran_context, 'database_type', None)
        if not db_type and hasattr(app, 'session') and hasattr(app.session, 'project_instance'):
            db_type = getattr(app.session.project_instance, 'database_type', None)

        if db_type:
            config_parser.set_dialect(db_type)
            logger.debug(f"Injected dialect '{db_type}' for project {project_id}")

        app.visitran_context.close_db_connection()
    except Exception as e:
        logger.warning(f"Failed to inject dialect: {e}")


def _inject_source_table_columns(config_parser, project_id: str) -> None:
    """
    Inject column metadata for the source table into the config parser.

    This enables PostgreSQL-compatible SQL generation for transformations like
    find_and_replace that need explicit column lists for in-place replacement.

    Args:
        config_parser: The ConfigParser instance to inject columns into
        project_id: The project ID for database connection
    """
    from backend.application.context.application import ApplicationContext

    # Skip if already populated
    if config_parser.get_source_table_columns():
        return

    source_schema = config_parser.source_schema_name
    source_table = config_parser.source_table_name

    if not source_table:
        return

    try:
        app = ApplicationContext(project_id=project_id)

        # Query information_schema for column names
        if source_schema:
            query = f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{source_schema}' AND table_name = '{source_table}'
                ORDER BY ordinal_position
            """
        else:
            query = f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{source_table}'
                ORDER BY ordinal_position
            """

        try:
            result = app.execute_sql_command(sql_command=query, limit=1000)

            columns = []
            if result:
                if isinstance(result, dict) and 'rows' in result:
                    columns = [row[0] for row in result['rows'] if row and row[0]]
                elif isinstance(result, list):
                    columns = [row.get("column_name") for row in result if row.get("column_name")]

            if columns:
                config_parser.set_source_table_columns(columns)
                logger.debug(f"Injected {len(columns)} source columns for {source_schema}.{source_table}")
        except Exception as e:
            logger.warning(f"Failed to get source columns for {source_schema}.{source_table}: {e}")

        app.visitran_context.close_db_connection()

    except Exception as e:
        logger.warning(f"Failed to create ApplicationContext for source column injection: {e}")


# =============================================================================
# VTR-060: Model Execution API with Error Handling
# =============================================================================


@api_view([HTTPMethods.POST])
@handle_http_request
def execute_model(request: Request, project_id: str) -> Response:
    """
    Execute a single model via the direct execution path.

    POST /api/v1/visitran/{org_id}/project/{project_id}/execute/direct/

    Request body:
        {
            "file_name": "model_name",  # Model name (required)
            "yaml_content": "...",  # YAML content to parse (required for ad-hoc execution)
            "schema": "schema_name",  # Optional, derived from yaml_content if not provided
            "environment": {"id": "..."},  # Optional environment configuration
            "incremental": false,
            "dry_run": false,  # If true, only generate SQL without executing
            "override_feature_flag": false
        }

    Returns:
        200: Execution successful with results
        400: Invalid model reference or validation error
        403: Direct execution disabled
        500: Internal execution error
    """
    from backend.application.config_parser.config_parser import ConfigParser
    from backend.application.context.application import ApplicationContext

    execution_id = str(uuid.uuid4())
    start_time = time.time()

    # Parse request body
    payload = request.data or {}
    file_name = payload.get("file_name", "")
    yaml_content = payload.get("yaml_content")
    schema = payload.get("schema", "")
    environment = payload.get("environment", {})
    environment_id = environment.get("id", "") if isinstance(environment, dict) else ""
    incremental = payload.get("incremental", False)
    dry_run = payload.get("dry_run", False)
    override_feature_flag = payload.get("override_feature_flag", False)

    # Validate required fields
    if not file_name:
        return Response(
            {
                "error": "Missing required field",
                "error_code": "MISSING_FILE_NAME",
                "message": "The 'file_name' field is required.",
            },
            status=status.HTTP_400_BAD_REQUEST,
        )

    model = file_name

    # Check feature flag
    if not override_feature_flag and not FeatureFlags.is_direct_execution_enabled():
        return Response(
            {
                "error": "Direct execution is disabled",
                "error_code": "DIRECT_EXECUTION_DISABLED",
                "message": "The ENABLE_DIRECT_EXECUTION feature flag is not enabled. "
                "Use override_feature_flag=true to bypass this check.",
            },
            status=status.HTTP_403_FORBIDDEN,
        )

    registry = ModelRegistry()
    qualified_name = f"{schema}.{model}" if schema else model

    try:
        # If yaml_content is provided, parse it directly using ConfigParser
        if yaml_content:
            import yaml as yaml_parser
            # Parse YAML string to dictionary
            model_data = yaml_parser.safe_load(yaml_content)
            # Clear cached instance to allow fresh parsing with new data
            if model in ConfigParser._instances:
                del ConfigParser._instances[model]
            config_parser = ConfigParser(model_data, file_name=model)

            # Inject database dialect and column metadata for SQL generation
            # This must happen before get_compiled_sql() to enable:
            # - Database-specific SQL syntax (dialect)
            # - Legacy rname pattern for JOIN column aliasing
            # - PostgreSQL-compatible find_and_replace with in-place replacement
            _inject_dialect(config_parser, project_id)
            _inject_join_table_columns(config_parser, project_id)
            _inject_source_table_columns(config_parser, project_id)

            transformation_sql = config_parser.get_compiled_sql()
            generated_sql = transformation_sql
        else:
            # Validate model exists in registry
            if not registry.contains(schema, model):
                # Try to get source location for error context
                yaml_location = _get_model_yaml_location(schema, model)
                return Response(
                    {
                        "error": "Model not found",
                        "error_code": "MODEL_NOT_FOUND",
                        "model": qualified_name,
                        "message": f"Model '{qualified_name}' is not registered in the ModelRegistry.",
                        "yaml_location": yaml_location,
                        "suggested_fix": f"Ensure the model '{model}' is defined in a YAML file under schema '{schema}'.",
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Get model configuration from registry
            config = registry.get(schema, model)

            # Inject dialect and column metadata for database-specific SQL generation
            _inject_dialect(config, project_id)
            _inject_join_table_columns(config, project_id)
            _inject_source_table_columns(config, project_id)

            # Get transformation SQL from config
            if hasattr(config, "get_compiled_sql"):
                transformation_sql = config.get_compiled_sql()
            else:
                source_schema = getattr(config, "source_schema_name", schema)
                source_table = getattr(config, "source_table_name", model)
                transformation_sql = f"SELECT * FROM {source_schema}.{source_table}"

            generated_sql = transformation_sql

        # Validate SQL if in parallel mode (only when we have a config from registry)
        validation_status = "skipped"
        validation_details = None

        if not yaml_content and ExecutionRouter.is_parallel_validation_mode():
            legacy_sql = _get_legacy_sql(config)  # type: ignore[possibly-undefined]
            if legacy_sql:
                validation_result = validate_sql_equivalence(
                    legacy_sql=legacy_sql,
                    direct_sql=generated_sql,
                    model_name=qualified_name,
                    store_result=True,
                )
                validation_status = "passed" if validation_result.match_status else "failed"
                validation_details = {
                    "dbt_sql": legacy_sql,
                    "ibis_sql": generated_sql,
                    "match_status": validation_result.match_status,
                    "discrepancy_details": validation_result.discrepancy_details if not validation_result.match_status else None,
                }

        # Execute SQL if not dry_run
        rows_affected = None
        execution_status = "dry_run" if dry_run else "pending"

        if not dry_run:
            try:
                # Create ApplicationContext for database execution
                app = ApplicationContext(project_id=project_id)

                # Get destination table info for CREATE TABLE
                if yaml_content:
                    dest_schema = config_parser.destination_schema_name
                    dest_table = config_parser.destination_table_name
                else:
                    dest_schema = getattr(config, "destination_schema_name", schema)
                    dest_table = getattr(config, "destination_table_name", model)

                # Build table reference
                if dest_schema:
                    full_table_name = f'"{dest_schema}"."{dest_table}"'
                else:
                    full_table_name = f'"{dest_table}"'

                # PostgreSQL doesn't support CREATE OR REPLACE TABLE
                # First drop the existing table, then create new one
                drop_sql = f"DROP TABLE IF EXISTS {full_table_name} CASCADE"
                create_sql = f"CREATE TABLE {full_table_name} AS {generated_sql}"

                logger.info(f"Executing SQL for model {qualified_name}")
                logger.info(f"  Drop: {drop_sql}")
                logger.info(f"  Create: {create_sql[:200]}...")

                # Execute DROP first
                app.execute_sql_command(sql_command=drop_sql)

                # Execute CREATE TABLE AS
                result = app.execute_sql_command(sql_command=create_sql)

                # Close connection
                app.visitran_context.close_db_connection()

                if isinstance(result, dict) and result.get("status") == "failed":
                    execution_status = "failed"
                    return Response(
                        {
                            "execution_id": execution_id,
                            "schema": schema,
                            "model": model,
                            "generated_sql": generated_sql,
                            "execution_status": execution_status,
                            "error_message": result.get("error_message"),
                            "execution_time_ms": round((time.time() - start_time) * 1000, 2),
                        },
                        status=status.HTTP_400_BAD_REQUEST,
                    )

                execution_status = "success"

            except Exception as exec_error:
                logger.exception(f"Error executing SQL for model {qualified_name}")
                execution_status = "failed"
                return Response(
                    {
                        "execution_id": execution_id,
                        "schema": schema,
                        "model": model,
                        "generated_sql": generated_sql,
                        "execution_status": execution_status,
                        "error_message": str(exec_error),
                        "execution_time_ms": round((time.time() - start_time) * 1000, 2),
                    },
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )

        execution_time_ms = (time.time() - start_time) * 1000

        return Response(
            {
                "execution_id": execution_id,
                "project_id": project_id,
                "schema": schema,
                "model": model,
                "generated_sql": generated_sql,
                "execution_status": execution_status,
                "validation_status": validation_status,
                "validation_details": validation_details,
                "execution_time_ms": round(execution_time_ms, 2),
                "rows_affected": rows_affected,
                "incremental": incremental,
            },
            status=status.HTTP_200_OK,
        )

    except IbisBuildError as e:
        execution_time_ms = (time.time() - start_time) * 1000
        return Response(
            {
                "error": "Ibis build error",
                "error_code": "IBIS_BUILD_ERROR",
                "model": qualified_name,
                "message": str(e),
                "yaml_location": {
                    "line": e.line_number,
                    "column": getattr(e, "column_number", None),
                },
                "execution_id": execution_id,
                "execution_time_ms": round(execution_time_ms, 2),
            },
            status=status.HTTP_400_BAD_REQUEST,
        )

    except TransformationError as e:
        execution_time_ms = (time.time() - start_time) * 1000
        return Response(
            {
                "error": "Transformation error",
                "error_code": "TRANSFORMATION_ERROR",
                "model": e.model_name,
                "message": e.error_message,
                "yaml_location": {
                    "file": None,
                    "line": e.line_number,
                    "column": e.column_number,
                },
                "yaml_snippet": e.yaml_snippet,
                "suggested_fix": e.suggested_fix,
                "execution_id": execution_id,
                "execution_time_ms": round(execution_time_ms, 2),
            },
            status=status.HTTP_400_BAD_REQUEST,
        )

    except Exception as e:
        logger.exception(f"Unexpected error executing model {qualified_name}")
        execution_time_ms = (time.time() - start_time) * 1000
        return Response(
            {
                "error": "Internal error",
                "error_code": "INTERNAL_ERROR",
                "model": qualified_name,
                "message": str(e),
                "execution_id": execution_id,
                "execution_time_ms": round(execution_time_ms, 2),
            },
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


# =============================================================================
# VTR-061: SQL Validation Results API
# =============================================================================


@api_view([HTTPMethods.GET])
@handle_http_request
def list_validation_results(request: Request) -> Response:
    """
    Get paginated list of SQL validation results.

    GET /api/v1/validation/results

    Query parameters:
        page: Page number (default: 1)
        page_size: Results per page (default: 20, max: 100)
        start_date: Filter results after this date (ISO 8601)
        end_date: Filter results before this date (ISO 8601)
        status: Filter by match status ('match' or 'mismatch')

    Returns:
        200: Paginated list of validation results
        400: Invalid query parameters
    """
    from backend.core.models.validation_models import SQLValidationResult

    # Parse pagination parameters
    try:
        page = int(request.query_params.get("page", 1))
        page_size = min(int(request.query_params.get("page_size", 20)), 100)
    except ValueError:
        return Response(
            {
                "error": "Invalid pagination parameters",
                "error_code": "INVALID_PARAMS",
                "message": "page and page_size must be integers",
            },
            status=status.HTTP_400_BAD_REQUEST,
        )

    # Build queryset with filters
    queryset = SQLValidationResult.objects.all()

    # Date range filter
    start_date = request.query_params.get("start_date")
    end_date = request.query_params.get("end_date")

    if start_date:
        try:
            queryset = queryset.filter(validated_at__gte=start_date)
        except ValueError:
            return Response(
                {
                    "error": "Invalid start_date format",
                    "error_code": "INVALID_DATE",
                    "message": "start_date must be in ISO 8601 format",
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

    if end_date:
        try:
            queryset = queryset.filter(validated_at__lte=end_date)
        except ValueError:
            return Response(
                {
                    "error": "Invalid end_date format",
                    "error_code": "INVALID_DATE",
                    "message": "end_date must be in ISO 8601 format",
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

    # Status filter
    match_status = request.query_params.get("status")
    if match_status == "match":
        queryset = queryset.filter(match_status=True)
    elif match_status == "mismatch":
        queryset = queryset.filter(match_status=False)
    elif match_status is not None:
        return Response(
            {
                "error": "Invalid status filter",
                "error_code": "INVALID_STATUS",
                "message": "status must be 'match' or 'mismatch'",
            },
            status=status.HTTP_400_BAD_REQUEST,
        )

    # Get total count
    total_count = queryset.count()

    # Apply pagination
    offset = (page - 1) * page_size
    results = queryset.order_by("-validated_at")[offset : offset + page_size]

    # Format results
    results_data = [
        {
            "model_name": r.model_name,
            "legacy_sql": r.legacy_sql,
            "direct_sql": r.direct_sql,
            "match_status": r.match_status,
            "diff": r.diff_output if not r.match_status else None,
            "timestamp": r.validated_at.isoformat(),
            "execution_id": r.execution_id,
        }
        for r in results
    ]

    return Response(
        {
            "results": results_data,
            "pagination": {
                "total_count": total_count,
                "page": page,
                "page_size": page_size,
                "has_next": offset + page_size < total_count,
                "has_previous": page > 1,
            },
        },
        status=status.HTTP_200_OK,
    )


@api_view([HTTPMethods.GET])
@handle_http_request
def get_model_validation_results(request: Request, model_name: str) -> Response:
    """
    Get validation results for a specific model.

    GET /api/v1/validation/results/{model_name}

    Query parameters:
        page: Page number (default: 1)
        page_size: Results per page (default: 20, max: 100)
        start_date: Filter results after this date (ISO 8601)
        end_date: Filter results before this date (ISO 8601)

    Returns:
        200: Paginated list of validation results for the model
        404: Model has no validation results
    """
    from backend.core.models.validation_models import SQLValidationResult

    # Parse pagination parameters
    try:
        page = int(request.query_params.get("page", 1))
        page_size = min(int(request.query_params.get("page_size", 20)), 100)
    except ValueError:
        return Response(
            {
                "error": "Invalid pagination parameters",
                "error_code": "INVALID_PARAMS",
                "message": "page and page_size must be integers",
            },
            status=status.HTTP_400_BAD_REQUEST,
        )

    # Build queryset
    queryset = SQLValidationResult.objects.filter(model_name__icontains=model_name)

    # Date range filter
    start_date = request.query_params.get("start_date")
    end_date = request.query_params.get("end_date")

    if start_date:
        queryset = queryset.filter(validated_at__gte=start_date)
    if end_date:
        queryset = queryset.filter(validated_at__lte=end_date)

    # Get total count
    total_count = queryset.count()

    if total_count == 0:
        return Response(
            {
                "error": "No validation results found",
                "error_code": "NOT_FOUND",
                "model_name": model_name,
                "message": f"No validation results found for model matching '{model_name}'",
            },
            status=status.HTTP_404_NOT_FOUND,
        )

    # Apply pagination
    offset = (page - 1) * page_size
    results = queryset.order_by("-validated_at")[offset : offset + page_size]

    # Format results
    results_data = [
        {
            "model_name": r.model_name,
            "legacy_sql": r.legacy_sql,
            "direct_sql": r.direct_sql,
            "match_status": r.match_status,
            "diff": r.diff_output if not r.match_status else None,
            "timestamp": r.validated_at.isoformat(),
            "execution_id": r.execution_id,
        }
        for r in results
    ]

    return Response(
        {
            "model_name": model_name,
            "results": results_data,
            "pagination": {
                "total_count": total_count,
                "page": page,
                "page_size": page_size,
                "has_next": offset + page_size < total_count,
                "has_previous": page > 1,
            },
        },
        status=status.HTTP_200_OK,
    )


# =============================================================================
# VTR-062: DAG Execution API
# =============================================================================


# In-memory storage for async execution state (would use Redis/DB in production)
_dag_execution_state: dict[str, dict] = {}


@api_view([HTTPMethods.POST])
@handle_http_request
def execute_dag(request: Request, project_id: str) -> Response:
    """
    Execute a complete DAG of model transformations.

    POST /api/v1/visitran/{org_id}/project/{project_id}/execute/direct/dag

    Request body:
        {
            "schema": "optional_schema_filter",
            "execution_mode": "legacy" | "direct" | "parallel"
        }

    Returns:
        200: Execution results (synchronous)
        202: Execution started (asynchronous, returns execution_id)
        400: Invalid request
    """
    _ = project_id  # Available for future use with ApplicationContext
    execution_id = str(uuid.uuid4())
    start_time = time.time()
    started_at = datetime.utcnow()

    # Parse request body
    payload = request.data or {}
    schema_filter = payload.get("schema")
    execution_mode_str = payload.get("execution_mode", "direct")

    # Validate execution mode
    try:
        execution_mode = ExecutionMode(execution_mode_str.lower())
    except ValueError:
        return Response(
            {
                "error": "Invalid execution_mode",
                "error_code": "INVALID_EXECUTION_MODE",
                "message": f"execution_mode must be one of: legacy, direct, parallel. Got: {execution_mode_str}",
            },
            status=status.HTTP_400_BAD_REQUEST,
        )

    try:
        # Get the DAG from DAGBuilder
        dag_builder = DAGBuilder()
        dag = dag_builder.dag

        if dag is None or dag.number_of_nodes() == 0:
            return Response(
                {
                    "execution_id": execution_id,
                    "execution_mode": execution_mode.value,
                    "total_models": 0,
                    "successful_count": 0,
                    "failed_count": 0,
                    "total_time_ms": 0,
                    "per_model_results": [],
                    "message": "No models in DAG to execute",
                },
                status=status.HTTP_200_OK,
            )

        # Filter by schema if specified
        if schema_filter:
            nodes_to_remove = [
                node for node in dag.nodes()
                if not node[0].startswith(schema_filter)
            ]
            for node in nodes_to_remove:
                dag.remove_node(node)

        # Execute DAG
        registry = ModelRegistry()

        # Set execution mode
        with FeatureFlags.override(execution_mode=execution_mode):
            executor = DAGExecutor(
                dag=dag,
                connection=None,  # Dry-run mode
                registry=registry,
                fail_fast=False,
            )
            result = executor.execute()

        # Format per-model results
        per_model_results = [
            {
                "model_name": f"{mr.schema}.{mr.model}",
                "execution_order": idx + 1,
                "sql_output": mr.sql_generated,
                "validation_status": (
                    "passed" if mr.validation_result and mr.validation_result.match_status
                    else "failed" if mr.validation_result
                    else "skipped"
                ),
                "validation_message": (
                    mr.validation_result.discrepancy_details
                    if mr.validation_result and not mr.validation_result.match_status
                    else None
                ),
                "execution_time_ms": round(mr.execution_time_ms, 2),
                "status": mr.status.value,
                "error": mr.error,
            }
            for idx, mr in enumerate(result.model_results)
        ]

        total_time_ms = (time.time() - start_time) * 1000

        # Store validation summary if in parallel mode
        if execution_mode == ExecutionMode.PARALLEL:
            service = get_validation_storage_service()
            service.create_summary(
                execution_id=execution_id,
                execution_mode=execution_mode.value,
                started_at=started_at,
                completed_at=datetime.utcnow(),
            )

        return Response(
            {
                "execution_id": execution_id,
                "execution_mode": execution_mode.value,
                "schema_filter": schema_filter,
                "total_models": len(result.model_results),
                "successful_count": result.models_executed,
                "failed_count": result.models_failed,
                "skipped_count": result.models_skipped,
                "total_time_ms": round(total_time_ms, 2),
                "validation_summary": result.validation_summary,
                "per_model_results": per_model_results,
            },
            status=status.HTTP_200_OK,
        )

    except Exception as e:
        logger.exception("Error executing DAG")
        total_time_ms = (time.time() - start_time) * 1000
        return Response(
            {
                "error": "DAG execution failed",
                "error_code": "DAG_EXECUTION_ERROR",
                "message": str(e),
                "execution_id": execution_id,
                "total_time_ms": round(total_time_ms, 2),
            },
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view([HTTPMethods.GET])
@handle_http_request
def get_dag_status(request: Request, project_id: str, execution_id: str) -> Response:
    """
    Get the status of an asynchronous DAG execution.

    GET /api/v1/visitran/{org_id}/project/{project_id}/execute/direct/dag/status/{execution_id}

    Returns:
        200: Execution status
        404: Execution not found
    """
    _ = project_id  # Available for future use
    if execution_id not in _dag_execution_state:
        return Response(
            {
                "error": "Execution not found",
                "error_code": "NOT_FOUND",
                "execution_id": execution_id,
                "message": f"No execution found with ID '{execution_id}'",
            },
            status=status.HTTP_404_NOT_FOUND,
        )

    state = _dag_execution_state[execution_id]
    return Response(
        {
            "execution_id": execution_id,
            "status": state.get("status", "unknown"),
            "models_completed": state.get("models_completed", 0),
            "models_total": state.get("models_total", 0),
            "current_model": state.get("current_model"),
            "started_at": state.get("started_at"),
        },
        status=status.HTTP_200_OK,
    )


@api_view([HTTPMethods.GET])
@handle_http_request
def get_dag_results(request: Request, project_id: str, execution_id: str) -> Response:
    """
    Get the full results of a completed DAG execution.

    GET /api/v1/visitran/{org_id}/project/{project_id}/execute/direct/dag/results/{execution_id}

    Returns:
        200: Full execution results
        404: Execution not found
        409: Execution not yet complete
    """
    _ = project_id  # Available for future use
    if execution_id not in _dag_execution_state:
        return Response(
            {
                "error": "Execution not found",
                "error_code": "NOT_FOUND",
                "execution_id": execution_id,
                "message": f"No execution found with ID '{execution_id}'",
            },
            status=status.HTTP_404_NOT_FOUND,
        )

    state = _dag_execution_state[execution_id]

    if state.get("status") != "completed":
        return Response(
            {
                "error": "Execution not complete",
                "error_code": "NOT_COMPLETE",
                "execution_id": execution_id,
                "status": state.get("status"),
                "message": "Execution is still in progress. Check /status endpoint for updates.",
            },
            status=status.HTTP_409_CONFLICT,
        )

    return Response(state.get("results", {}), status=status.HTTP_200_OK)


# =============================================================================
# VTR-063: Model Registry Inspection Endpoints
# =============================================================================


@api_view([HTTPMethods.GET])
@handle_http_request
def list_registry_models(request: Request) -> Response:
    """
    List all models registered in the ModelRegistry.

    GET /api/v1/registry/models

    Returns:
        200: List of all registered models with metadata
    """
    registry = ModelRegistry()
    models = registry.list_models()

    models_data = []
    for model_key in models:
        # Parse schema.model from key
        parts = model_key.split(".", 1)
        schema = parts[0] if len(parts) > 1 else ""
        model = parts[1] if len(parts) > 1 else parts[0]

        try:
            metadata = registry.get_metadata(schema, model)
            models_data.append({
                "key": model_key,
                "schema": schema,
                "model": model,
                "table_name": metadata.table_name,
                "materialization_type": metadata.materialization_type,
                "execution_status": metadata.execution_status.value,
                "config_loaded": metadata.config_loaded,
            })
        except KeyError:
            models_data.append({
                "key": model_key,
                "schema": schema,
                "model": model,
                "error": "Metadata not found",
            })

    return Response(
        {
            "models": models_data,
            "total_count": len(models_data),
            "cache_status": {
                "cached_tables": registry.cached_table_count,
                "is_locked": registry.is_locked,
            },
            "last_modified": datetime.utcnow().isoformat(),
        },
        status=status.HTTP_200_OK,
    )


@api_view([HTTPMethods.GET])
@handle_http_request
def get_registry_dag(request: Request, schema: str) -> Response:
    """
    Get the DAG visualization for a schema.

    GET /api/v1/registry/dag/{schema}

    Returns:
        200: DAG as JSON with nodes and edges
        404: Schema not found
    """
    try:
        dag_builder = DAGBuilder()
        dag = dag_builder.dag

        if dag is None:
            return Response(
                {
                    "error": "DAG not available",
                    "error_code": "DAG_NOT_FOUND",
                    "message": "No DAG has been built yet",
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        # Filter nodes by schema
        schema_nodes = [
            node for node in dag.nodes()
            if node[0] == schema
        ]

        if not schema_nodes:
            return Response(
                {
                    "error": "Schema not found in DAG",
                    "error_code": "SCHEMA_NOT_FOUND",
                    "schema": schema,
                    "message": f"No models found for schema '{schema}'",
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        # Build nodes list
        nodes = [
            {
                "id": f"{node[0]}.{node[1]}",
                "schema": node[0],
                "model": node[1],
            }
            for node in schema_nodes
        ]

        # Build edges list (only edges within the schema)
        edges = []
        for source, target in dag.edges():
            if source[0] == schema and target[0] == schema:
                edges.append({
                    "source": f"{source[0]}.{source[1]}",
                    "target": f"{target[0]}.{target[1]}",
                })

        return Response(
            {
                "schema": schema,
                "nodes": nodes,
                "edges": edges,
                "node_count": len(nodes),
                "edge_count": len(edges),
                "cache_status": "current",
                "last_modified": datetime.utcnow().isoformat(),
            },
            status=status.HTTP_200_OK,
        )

    except Exception as e:
        logger.exception(f"Error getting DAG for schema {schema}")
        return Response(
            {
                "error": "Internal error",
                "error_code": "INTERNAL_ERROR",
                "message": str(e),
            },
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view([HTTPMethods.GET])
@handle_http_request
def get_registry_model_detail(request: Request, schema: str, model: str) -> Response:
    """
    Get detailed information about a specific model.

    GET /api/v1/registry/models/{schema}/{model}

    Returns:
        200: Model details including ConfigParser metadata
        404: Model not found
    """
    registry = ModelRegistry()
    qualified_name = f"{schema}.{model}"

    if not registry.contains(schema, model):
        return Response(
            {
                "error": "Model not found",
                "error_code": "NOT_FOUND",
                "model": qualified_name,
                "message": f"Model '{qualified_name}' is not registered",
            },
            status=status.HTTP_404_NOT_FOUND,
        )

    try:
        config = registry.get(schema, model)
        metadata = registry.get_metadata(schema, model)

        # Extract ConfigParser details
        config_details = {
            "destination_schema": getattr(config, "destination_schema_name", schema),
            "destination_table": getattr(config, "destination_table_name", model),
            "source_schema": getattr(config, "source_schema_name", None),
            "source_table": getattr(config, "source_table_name", None),
            "materialization": getattr(config, "materialization", "TABLE"),
        }

        # Get source location if available
        yaml_location = None
        if hasattr(config, "get_source_location"):
            location = config.get_source_location()
            if location:
                yaml_location = {
                    "file": location.get("file"),
                    "line": location.get("line"),
                    "column": location.get("column"),
                }

        # Check Ibis cache
        is_cache_valid = registry.is_cache_valid(schema, model)

        return Response(
            {
                "schema": schema,
                "model": model,
                "qualified_name": qualified_name,
                "metadata": metadata.to_dict(),
                "config": config_details,
                "yaml_location": yaml_location,
                "cache_status": "valid" if is_cache_valid else "invalid",
                "last_modified": datetime.utcnow().isoformat(),
            },
            status=status.HTTP_200_OK,
        )

    except Exception as e:
        logger.exception(f"Error getting model details for {qualified_name}")
        return Response(
            {
                "error": "Internal error",
                "error_code": "INTERNAL_ERROR",
                "model": qualified_name,
                "message": str(e),
            },
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )




# =============================================================================
# Feature Flags API
# =============================================================================


@api_view([HTTPMethods.GET])
@handle_http_request
def get_feature_flags(request: Request) -> Response:
    """
    Get current feature flag settings for direct execution.

    GET /api/v1/visitran/{org_id}/feature-flags

    Returns:
        200: Current feature flag values
    """
    from backend.utils.tenant_context import get_current_tenant

    # org_id from tenant context (set by middleware) - available for future per-org flags
    _ = get_current_tenant()

    return Response(
        {
            "enable_direct_execution": FeatureFlags.is_direct_execution_enabled(),
            "execution_mode": FeatureFlags.get_execution_mode().value,
            "suppress_python_files": FeatureFlags.should_suppress_python_files(),
            "rollout_phase": FeatureFlags.get_rollout_phase().value,
            "sql_validation_enabled": FeatureFlags.is_sql_validation_enabled(),
        },
        status=status.HTTP_200_OK,
    )


# =============================================================================
# Helper Functions
# =============================================================================


def _get_model_yaml_location(schema: str, model: str) -> Optional[dict]:
    """Get YAML source location for a model (if available)."""
    try:
        registry = ModelRegistry()
        if registry.contains(schema, model):
            config = registry.get(schema, model)
            if hasattr(config, "get_source_location"):
                location = config.get_source_location()
                if location:
                    return {
                        "file": location.get("file"),
                        "line": location.get("line"),
                        "column": location.get("column"),
                    }
    except Exception:
        pass
    return None


def _get_legacy_sql(config: Any) -> Optional[str]:
    """Get legacy SQL from config if available."""
    if hasattr(config, "get_legacy_sql"):
        return config.get_legacy_sql()
    return None
