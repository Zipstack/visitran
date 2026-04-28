"""
DAG Executor for Topological Model Execution.

This module executes data transformation models in topological dependency order,
integrating IbisBuilder for SQL compilation and MaterializationHandler for
database operations.

Usage:
    executor = DAGExecutor(dag, connection)
    result = executor.execute()

    if result.success:
        print(f"Executed {result.models_executed} models")
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Optional

import networkx as nx

from visitran.errors import TransformationError, VisitranBaseExceptions
from visitran.errors.error_codes import ErrorCodeConstants

from backend.application.config_parser.execution_planner import (
    ExecutionPlan,
    ExecutionPlanner,
    ExecutionStep,
    ExecutionPlanError,
)
from backend.application.config_parser.feature_flags import (
    ExecutionRouter,
    FeatureFlags,
)
from backend.application.config_parser.ibis_builder import (
    IbisBuilder,
    IbisBuildError,
)
from backend.application.config_parser.materialization_handler import (
    DatabaseConnection,
    MaterializationConfig,
    MaterializationHandler,
    MaterializationMode,
    MaterializationResult,
)
from backend.application.config_parser.model_registry import ModelRegistry
from backend.application.config_parser.sql_validator import (
    ValidationResult,
    validate_sql_equivalence,
)

if TYPE_CHECKING:
    from backend.application.config_parser.config_parser import ConfigParser

logger = logging.getLogger(__name__)


class ExecutionStatus(Enum):
    """Status of model execution."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class ModelExecutionResult:
    """
    Result of executing a single model.

    Attributes:
        schema: Schema name
        model: Model name
        status: Execution status
        execution_time_ms: Time to execute in milliseconds
        sql_generated: The SQL that was generated/executed
        materialization_result: Result from materialization handler
        error: Error information if failed
        validation_result: SQL validation result (if parallel mode)
    """

    schema: str
    model: str
    status: ExecutionStatus
    execution_time_ms: float = 0.0
    sql_generated: Optional[str] = None
    materialization_result: Optional[MaterializationResult] = None
    error: Optional[str] = None
    error_line: Optional[int] = None
    error_column: Optional[int] = None
    error_file: Optional[str] = None
    validation_result: Optional[ValidationResult] = None

    @property
    def qualified_name(self) -> str:
        """Return schema.model qualified name."""
        return f"{self.schema}.{self.model}"

    @property
    def success(self) -> bool:
        """Check if execution was successful."""
        return self.status == ExecutionStatus.COMPLETED

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        result = {
            "schema": self.schema,
            "model": self.model,
            "qualified_name": self.qualified_name,
            "status": self.status.value,
            "execution_time_ms": self.execution_time_ms,
            "success": self.success,
        }
        if self.error:
            result["error"] = {
                "message": self.error,
                "line": self.error_line,
                "column": self.error_column,
                "file": self.error_file,
            }
        return result


@dataclass
class DAGExecutionResult:
    """
    Result of executing the entire DAG.

    Attributes:
        success: Whether all models executed successfully
        models_executed: Number of models executed
        models_failed: Number of models that failed
        models_skipped: Number of models skipped
        total_time_ms: Total execution time in milliseconds
        model_results: Results for each model
        validation_summary: Summary of SQL validation (if parallel mode)
    """

    success: bool
    models_executed: int = 0
    models_failed: int = 0
    models_skipped: int = 0
    total_time_ms: float = 0.0
    model_results: list[ModelExecutionResult] = field(default_factory=list)
    validation_summary: Optional[dict[str, Any]] = None
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "models_executed": self.models_executed,
            "models_failed": self.models_failed,
            "models_skipped": self.models_skipped,
            "total_time_ms": self.total_time_ms,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "model_results": [r.to_dict() for r in self.model_results],
            "validation_summary": self.validation_summary,
        }


class DAGExecutionError(VisitranBaseExceptions):
    """
    Raised when DAG execution fails.

    Enriched with YAML source location when available.
    """

    def __init__(
        self,
        message: str,
        model_name: Optional[str] = None,
        line_number: Optional[int] = None,
        column_number: Optional[int] = None,
        file_path: Optional[str] = None,
        cause: Optional[Exception] = None,
    ):
        self.message = message
        self.model_name = model_name
        self.line_number = line_number
        self.column_number = column_number
        self.file_path = file_path
        self.cause = cause
        super().__init__(
            error_code=ErrorCodeConstants.DAG_EXECUTION_ERROR,
            model_name=model_name or "unknown",
            message=message,
        )

    def _format_message(self) -> str:
        """Format error message with source location."""
        parts = [self.message]

        if self.model_name:
            parts.append(f"Model: {self.model_name}")

        if self.file_path:
            location = self.file_path
            if self.line_number is not None:
                location += f":{self.line_number}"
                if self.column_number is not None:
                    location += f":{self.column_number}"
            parts.append(f"Location: {location}")

        return " | ".join(parts)

    def to_transformation_error(self) -> TransformationError:
        """Convert to TransformationError."""
        return TransformationError(
            model_name=self.model_name or "unknown",
            transformation_id=None,
            error_message=self.message,
            line_number=self.line_number,
            column_number=self.column_number,
            yaml_snippet=None,
        )

    def error_response(self) -> dict:
        """Return structured error response."""
        response = super().error_response()
        if self.model_name:
            response["model_name"] = self.model_name
        if self.line_number:
            response["line_number"] = self.line_number
        if self.column_number:
            response["column_number"] = self.column_number
        if self.file_path:
            response["file_path"] = self.file_path
        return response


class DAGExecutor:
    """
    Executes DAG of data transformation models in topological order.

    The executor:
    1. Creates an execution plan from the DAG
    2. For each model in order:
       - Builds Ibis expression using IbisBuilder
       - Compiles to SQL
       - Applies materialization strategy
       - Registers result in ModelRegistry
    3. Validates SQL equivalence if in parallel mode

    Usage:
        executor = DAGExecutor(dag, connection)
        result = executor.execute()

        # With hooks
        executor = DAGExecutor(dag, connection)
        executor.on_model_start = lambda step: print(f"Starting {step.model}")
        executor.on_model_complete = lambda step, result: print(f"Done: {result.success}")
        result = executor.execute()
    """

    def __init__(
        self,
        dag: nx.DiGraph,
        connection: Optional[DatabaseConnection] = None,
        registry: Optional[ModelRegistry] = None,
        fail_fast: bool = True,
    ) -> None:
        """
        Initialize the executor.

        Args:
            dag: NetworkX DiGraph with model dependencies
            connection: Database connection (None for dry-run)
            registry: ModelRegistry for storing results (default: global)
            fail_fast: Stop on first failure (default: True)
        """
        self._dag = dag
        self._connection = connection
        self._registry = registry or ModelRegistry()
        self._fail_fast = fail_fast

        # Execution components
        self._planner = ExecutionPlanner(dag)
        # Get the Ibis backend connection if available
        ibis_connection = connection.connection if connection and hasattr(connection, 'connection') else None
        self._ibis_builder = IbisBuilder(self._registry, connection=ibis_connection)
        self._materialization_handler = MaterializationHandler(connection)

        # Execution hooks
        self.on_model_start: Optional[Callable[[ExecutionStep], None]] = None
        self.on_model_complete: Optional[Callable[[ExecutionStep, ModelExecutionResult], None]] = None
        self.on_execution_start: Optional[Callable[[ExecutionPlan], None]] = None
        self.on_execution_complete: Optional[Callable[[DAGExecutionResult], None]] = None

        # Execution state
        self._execution_status: dict[tuple[str, str], ExecutionStatus] = {}
        self._model_results: dict[tuple[str, str], ModelExecutionResult] = {}

    @property
    def dag(self) -> nx.DiGraph:
        """Return the DAG being executed."""
        return self._dag

    @property
    def registry(self) -> ModelRegistry:
        """Return the model registry."""
        return self._registry

    def execute(self) -> DAGExecutionResult:
        """
        Execute the DAG.

        Uses feature flags to determine execution path:
        - DIRECT mode: Execute via Ibis/direct path only
        - LEGACY mode: Skip (let legacy path handle)
        - PARALLEL mode: Execute both and validate

        Returns:
            DAGExecutionResult with execution details
        """
        start_time = time.time()
        result = DAGExecutionResult(success=True)

        # Check feature flags
        if not ExecutionRouter.should_execute_direct():
            logger.info("Direct execution disabled, skipping DAG executor")
            result.success = True
            result.completed_at = datetime.utcnow()
            return result

        try:
            # Create execution plan
            plan = self._planner.create_plan()

            # Notify execution start
            if self.on_execution_start:
                self.on_execution_start(plan)

            if plan.is_empty:
                logger.info("Empty execution plan, nothing to execute")
                result.completed_at = datetime.utcnow()
                # Notify execution complete
                if self.on_execution_complete:
                    self.on_execution_complete(result)
                return result

            logger.info(f"Executing DAG with {len(plan)} models")

            # Execute each model in topological order
            for step in plan:
                model_result = self._execute_step(step)
                result.model_results.append(model_result)

                if model_result.status == ExecutionStatus.COMPLETED:
                    result.models_executed += 1
                elif model_result.status == ExecutionStatus.FAILED:
                    result.models_failed += 1
                    result.success = False

                    if self._fail_fast:
                        logger.error(
                            f"Execution failed at {model_result.qualified_name}, "
                            f"stopping (fail_fast=True)"
                        )
                        # Mark remaining models as skipped
                        remaining_count = len(plan) - (step.order + 1)
                        result.models_skipped = remaining_count
                        break
                elif model_result.status == ExecutionStatus.SKIPPED:
                    result.models_skipped += 1

            # Add validation summary if in parallel mode
            if ExecutionRouter.is_parallel_validation_mode():
                result.validation_summary = self._get_validation_summary()

        except ExecutionPlanError as e:
            logger.error(f"Failed to create execution plan: {e}")
            result.success = False
            result.model_results.append(
                ModelExecutionResult(
                    schema="",
                    model="",
                    status=ExecutionStatus.FAILED,
                    error=str(e),
                )
            )

        result.total_time_ms = (time.time() - start_time) * 1000
        result.completed_at = datetime.utcnow()

        # Notify execution complete
        if self.on_execution_complete:
            self.on_execution_complete(result)

        logger.info(
            f"DAG execution complete: {result.models_executed} executed, "
            f"{result.models_failed} failed, {result.models_skipped} skipped "
            f"in {result.total_time_ms:.2f}ms"
        )

        return result

    def _execute_step(self, step: ExecutionStep) -> ModelExecutionResult:
        """
        Execute a single step in the plan.

        Args:
            step: The execution step to process

        Returns:
            ModelExecutionResult with execution details
        """
        start_time = time.time()

        # Update status
        self._execution_status[step.key] = ExecutionStatus.RUNNING

        # Notify model start
        if self.on_model_start:
            self.on_model_start(step)

        try:
            # Check if dependencies are satisfied
            if not self._check_dependencies(step):
                result = self._create_skipped_result(step, "Dependencies not satisfied")
                self._execution_status[step.key] = ExecutionStatus.SKIPPED
                self._model_results[step.key] = result
                # Notify model complete for skipped models too
                if self.on_model_complete:
                    self.on_model_complete(step, result)
                return result

            # Get model configuration
            config = step.config
            if config is None:
                raise DAGExecutionError(
                    "Model configuration not found",
                    model_name=step.qualified_name,
                )

            # Build Ibis expression and compile to SQL
            sql = self._build_sql(step, config)

            # Apply materialization
            mat_result = self._materialize(step, config, sql)

            # Register in ModelRegistry
            self._register_result(step, config, mat_result)

            # Validate SQL if in parallel mode
            validation_result = None
            if ExecutionRouter.is_parallel_validation_mode():
                validation_result = self._validate_sql(step, config, sql)

            # Create success result
            execution_time = (time.time() - start_time) * 1000
            result = ModelExecutionResult(
                schema=step.schema,
                model=step.model,
                status=ExecutionStatus.COMPLETED,
                execution_time_ms=execution_time,
                sql_generated=sql,
                materialization_result=mat_result,
                validation_result=validation_result,
            )

            self._execution_status[step.key] = ExecutionStatus.COMPLETED
            self._model_results[step.key] = result

            logger.info(f"Executed {step.qualified_name} in {execution_time:.2f}ms")

        except DAGExecutionError as e:
            result = self._create_error_result(step, e, start_time)
            self._execution_status[step.key] = ExecutionStatus.FAILED
            self._model_results[step.key] = result

        except IbisBuildError as e:
            execution_error = DAGExecutionError(
                message=str(e),
                model_name=step.qualified_name,
                line_number=e.line_number,
                cause=e,
            )
            result = self._create_error_result(step, execution_error, start_time)
            self._execution_status[step.key] = ExecutionStatus.FAILED
            self._model_results[step.key] = result

        except Exception as e:
            logger.exception(f"Unexpected error executing {step.qualified_name}")
            execution_error = DAGExecutionError(
                message=f"Unexpected error: {str(e)}",
                model_name=step.qualified_name,
                cause=e,
            )
            result = self._create_error_result(step, execution_error, start_time)
            self._execution_status[step.key] = ExecutionStatus.FAILED
            self._model_results[step.key] = result

        # Notify model complete
        if self.on_model_complete:
            self.on_model_complete(step, result)

        return result

    def _check_dependencies(self, step: ExecutionStep) -> bool:
        """
        Check if all dependencies have been successfully executed.

        In our DAG, edge direction is: dependent -> dependency.
        So to find dependencies of a node, we check its successors.

        Args:
            step: The step to check dependencies for

        Returns:
            True if all dependencies are satisfied
        """
        # Edge direction: dependent -> dependency
        # So successors are the dependencies
        dependencies = list(self._dag.successors(step.key))

        for dep_key in dependencies:
            status = self._execution_status.get(dep_key)
            if status != ExecutionStatus.COMPLETED:
                logger.warning(
                    f"Dependency {dep_key[0]}.{dep_key[1]} not completed "
                    f"for {step.qualified_name}"
                )
                return False

        return True

    def _build_sql(self, step: ExecutionStep, config: ConfigParser) -> str:
        """
        Build SQL from model configuration.

        Args:
            step: The execution step
            config: The model configuration

        Returns:
            Compiled SQL string

        Raises:
            DAGExecutionError: If SQL compilation fails
        """
        try:
            # Get transformation SQL from config
            transformation_sql = self._get_transformation_sql(config)

            # Compile using IbisBuilder
            compilation_result = self._ibis_builder.compile_transformation(
                sql=transformation_sql,
                line_number=self._get_source_line(config),
            )

            return compilation_result.sql_resolved

        except IbisBuildError:
            raise  # Let caller handle

        except Exception as e:
            line_number = self._get_source_line(config)
            raise DAGExecutionError(
                message=f"Failed to build SQL: {str(e)}",
                model_name=step.qualified_name,
                line_number=line_number,
                file_path=self._get_source_file(config),
                cause=e,
            )

    def _get_transformation_sql(self, config: ConfigParser) -> str:
        """
        Extract transformation SQL from config.

        Args:
            config: The model configuration

        Returns:
            SQL string
        """
        # Populate table column metadata before SQL generation
        # This enables proper column aliasing for JOINs and PostgreSQL-compatible
        # SQL generation for find_and_replace
        self._populate_join_table_columns(config)
        self._populate_source_table_columns(config)

        # Try to get SQL from various sources
        if hasattr(config, 'get_compiled_sql'):
            return config.get_compiled_sql()

        # Fallback: construct from source
        source_schema = config.source_schema_name
        source_table = config.source_table_name

        return f"SELECT * FROM {source_schema}.{source_table}"

    def _populate_join_table_columns(self, config: ConfigParser) -> None:
        """
        Populate column metadata for all joined tables in the configuration.

        This enables the SQL builder to generate proper column aliases for
        JOIN operations, matching the legacy Ibis rname pattern where columns
        from joined tables are prefixed with the table name (e.g., table_col).

        Args:
            config: The model configuration with JOIN transformations
        """
        if not self._ibis_builder or not self._ibis_builder._connection:
            # No database connection available - columns will fall back to table.*
            return

        try:
            # Get all joined tables from configuration
            joined_tables = config.get_joined_tables()

            for schema, table in joined_tables:
                # Skip if columns already set
                if config.get_join_table_columns(schema, table):
                    continue

                try:
                    # Use Ibis connection to get table object and extract columns
                    ibis_table = self._ibis_builder._connection.table(
                        table,
                        database=schema if schema else None
                    )
                    columns = list(ibis_table.columns)
                    config.set_join_table_columns(schema, table, columns)
                    logger.debug(
                        f"Populated {len(columns)} columns for joined table "
                        f"{schema}.{table}" if schema else table
                    )
                except Exception as e:
                    # Log warning but continue - SQL builder will fall back to table.*
                    logger.warning(
                        f"Could not get columns for joined table "
                        f"{schema}.{table if schema else table}: {e}"
                    )
        except Exception as e:
            # Don't fail SQL generation due to column population issues
            logger.warning(f"Failed to populate join table columns: {e}")

    def _populate_source_table_columns(self, config: ConfigParser) -> None:
        """
        Populate column metadata for the source table.

        This enables PostgreSQL-compatible SQL generation for transformations
        like find_and_replace that need explicit column lists.

        Args:
            config: The model configuration
        """
        if not self._ibis_builder or not self._ibis_builder._connection:
            return

        # Skip if already set
        if config.get_source_table_columns():
            return

        try:
            schema = config.source_schema_name
            table = config.source_table_name

            ibis_table = self._ibis_builder._connection.table(
                table,
                database=schema if schema else None
            )
            columns = list(ibis_table.columns)
            config.set_source_table_columns(columns)
            logger.debug(
                f"Populated {len(columns)} columns for source table "
                f"{schema}.{table}" if schema else table
            )
        except Exception as e:
            logger.warning(f"Could not get columns for source table: {e}")

    def _get_source_line(self, config: ConfigParser) -> Optional[int]:
        """Get source line number from config."""
        # ConfigParser.get_source_location requires a transformation_id
        # For model-level errors, we don't have a specific transformation
        # Return None as we don't have line info at model level
        return None

    def _get_source_file(self, config: ConfigParser) -> Optional[str]:
        """Get source file path from config."""
        # ConfigParser doesn't store file path directly
        # Return None as we don't have file info at model level
        return None

    def _materialize(
        self,
        step: ExecutionStep,
        config: ConfigParser,
        sql: str,
    ) -> MaterializationResult:
        """
        Apply materialization strategy.

        Args:
            step: The execution step
            config: The model configuration
            sql: The compiled SQL

        Returns:
            MaterializationResult
        """
        # Get materialization mode
        mode_str = config.materialization if hasattr(config, 'materialization') else "TABLE"
        mode = MaterializationMode.from_string(mode_str)

        # Get incremental configuration if applicable
        incremental_key = None
        unique_key = None
        if mode == MaterializationMode.INCREMENTAL:
            if hasattr(config, 'incremental_key'):
                incremental_key = config.incremental_key
            if hasattr(config, 'unique_key'):
                unique_key = config.unique_key

        # Create materialization config
        mat_config = MaterializationConfig(
            schema_name=config.destination_schema_name,
            table_name=config.destination_table_name,
            sql=sql,
            mode=mode,
            incremental_key=incremental_key,
            unique_key=unique_key,
        )

        # Execute materialization
        return self._materialization_handler.execute(mat_config)

    def _register_result(
        self,
        step: ExecutionStep,
        config: ConfigParser,
        mat_result: MaterializationResult,
    ) -> None:
        """
        Register executed model in ModelRegistry.

        Args:
            step: The execution step
            config: The model configuration
            mat_result: The materialization result
        """
        # For ephemeral models, we store the CTE SQL
        # For persistent models, we store the table/view reference

        if mat_result.config.mode == MaterializationMode.EPHEMERAL:
            # Ephemeral models don't get registered as tables
            # Their SQL is stored for inline substitution
            logger.debug(f"Ephemeral model {step.qualified_name} not registered in DB")
        else:
            # Persistent models - register in ModelRegistry
            if not self._registry.contains(step.schema, step.model):
                self._registry.register(
                    schema=step.schema,
                    model=step.model,
                    config=config,
                )

    def _validate_sql(
        self,
        step: ExecutionStep,
        config: ConfigParser,
        direct_sql: str,
    ) -> Optional[ValidationResult]:
        """
        Validate SQL equivalence against legacy path.

        Args:
            step: The execution step
            config: The model configuration
            direct_sql: SQL from direct path

        Returns:
            ValidationResult if validation was performed
        """
        # Get legacy SQL if available
        legacy_sql = self._get_legacy_sql(config)
        if legacy_sql is None:
            return None

        # Perform validation (warnings only, don't fail)
        result = validate_sql_equivalence(
            legacy_sql=legacy_sql,
            direct_sql=direct_sql,
            model_name=step.qualified_name,
            store_result=True,
            log_discrepancy=True,
        )

        if not result.match_status:
            logger.warning(
                f"SQL discrepancy detected for {step.qualified_name}. "
                "See validation log for details."
            )

        return result

    def _get_legacy_sql(self, config: ConfigParser) -> Optional[str]:
        """
        Get SQL from legacy Python generation path.

        Args:
            config: The model configuration

        Returns:
            Legacy SQL if available, None otherwise
        """
        # This would typically come from the Python file generation system
        if hasattr(config, 'get_legacy_sql'):
            return config.get_legacy_sql()
        return None

    def _get_validation_summary(self) -> dict[str, Any]:
        """Get summary of SQL validation results."""
        from backend.application.config_parser.sql_validator import get_validation_store

        store = get_validation_store()
        return store.get_summary()

    def _create_skipped_result(
        self,
        step: ExecutionStep,
        reason: str,
    ) -> ModelExecutionResult:
        """Create a result for a skipped model."""
        return ModelExecutionResult(
            schema=step.schema,
            model=step.model,
            status=ExecutionStatus.SKIPPED,
            error=reason,
        )

    def _create_error_result(
        self,
        step: ExecutionStep,
        error: DAGExecutionError,
        start_time: float,
    ) -> ModelExecutionResult:
        """Create a result for a failed model."""
        execution_time = (time.time() - start_time) * 1000

        logger.error(
            f"Model {step.qualified_name} failed: {error.message}"
            + (f" at line {error.line_number}" if error.line_number else "")
        )

        return ModelExecutionResult(
            schema=step.schema,
            model=step.model,
            status=ExecutionStatus.FAILED,
            execution_time_ms=execution_time,
            error=error.message,
            error_line=error.line_number,
            error_column=error.column_number,
            error_file=error.file_path,
        )

    def get_model_status(self, schema: str, model: str) -> ExecutionStatus:
        """
        Get the execution status of a model.

        Args:
            schema: Schema name
            model: Model name

        Returns:
            ExecutionStatus (PENDING if not yet processed)
        """
        return self._execution_status.get((schema, model), ExecutionStatus.PENDING)

    def get_model_result(
        self,
        schema: str,
        model: str,
    ) -> Optional[ModelExecutionResult]:
        """
        Get the execution result for a model.

        Args:
            schema: Schema name
            model: Model name

        Returns:
            ModelExecutionResult if executed, None otherwise
        """
        return self._model_results.get((schema, model))


def execute_dag(
    dag: nx.DiGraph,
    connection: Optional[DatabaseConnection] = None,
    fail_fast: bool = True,
) -> DAGExecutionResult:
    """
    Convenience function to execute a DAG.

    Args:
        dag: NetworkX DiGraph with model dependencies
        connection: Database connection (None for dry-run)
        fail_fast: Stop on first failure

    Returns:
        DAGExecutionResult with execution details
    """
    executor = DAGExecutor(dag, connection, fail_fast=fail_fast)
    return executor.execute()
