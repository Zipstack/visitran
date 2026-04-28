"""
Materialization Strategy Handler.

This module provides materialization execution for different modes:
- TABLE: CREATE TABLE AS SELECT
- VIEW: CREATE OR REPLACE VIEW
- INCREMENTAL: INSERT/MERGE based on incremental_key
- EPHEMERAL: Return CTE definition without DDL

Usage:
    handler = MaterializationHandler(connection)
    result = handler.execute(config)
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional, Protocol, Union

logger = logging.getLogger(__name__)


class MaterializationMode(Enum):
    """Supported materialization modes."""

    TABLE = "TABLE"
    VIEW = "VIEW"
    INCREMENTAL = "INCREMENTAL"
    EPHEMERAL = "EPHEMERAL"

    @classmethod
    def from_string(cls, value: str) -> MaterializationMode:
        """Create MaterializationMode from string value."""
        upper_value = value.upper()
        try:
            return cls(upper_value)
        except ValueError:
            raise ValueError(
                f"Invalid materialization mode: '{value}'. "
                f"Supported modes: {[m.value for m in cls]}"
            )


class IncrementalStrategy(Enum):
    """Strategy for incremental materialization."""

    APPEND = "append"      # INSERT new records only
    MERGE = "merge"        # MERGE/UPSERT based on unique_key


@dataclass
class MaterializationConfig:
    """
    Configuration for materialization execution.

    Attributes:
        schema_name: Target schema name
        table_name: Target table/view name
        sql: The transformation SQL to execute
        mode: Materialization mode
        incremental_key: Column for incremental filtering (for INCREMENTAL mode)
        unique_key: Column(s) for MERGE deduplication (for INCREMENTAL mode)
        incremental_strategy: Strategy for incremental updates
        replace_existing: Whether to drop existing objects before creating
    """

    schema_name: str
    table_name: str
    sql: str
    mode: MaterializationMode = MaterializationMode.TABLE
    incremental_key: Optional[str] = None
    unique_key: Optional[Union[str, list[str]]] = None
    incremental_strategy: IncrementalStrategy = IncrementalStrategy.APPEND
    replace_existing: bool = True

    @property
    def full_table_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.schema_name}.{self.table_name}"

    def validate(self) -> list[str]:
        """
        Validate configuration.

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        if not self.schema_name:
            errors.append("schema_name is required")
        if not self.table_name:
            errors.append("table_name is required")
        if not self.sql:
            errors.append("sql is required")

        if self.mode == MaterializationMode.INCREMENTAL:
            if not self.incremental_key:
                errors.append("incremental_key is required for INCREMENTAL mode")
            if self.incremental_strategy == IncrementalStrategy.MERGE:
                if not self.unique_key:
                    errors.append("unique_key is required for MERGE strategy")

        return errors


@dataclass
class MaterializationResult:
    """
    Result of materialization execution.

    Attributes:
        config: The materialization config used
        success: Whether execution succeeded
        execution_time_ms: Execution time in milliseconds
        rows_affected: Number of rows affected (if applicable)
        sql_executed: The actual SQL that was executed
        error_message: Error message if failed
        cte_sql: CTE definition (for EPHEMERAL mode only)
        max_incremental_value: Max value of incremental_key after execution
    """

    config: MaterializationConfig
    success: bool
    execution_time_ms: float = 0.0
    rows_affected: Optional[int] = None
    sql_executed: Optional[str] = None
    error_message: Optional[str] = None
    cte_sql: Optional[str] = None
    max_incremental_value: Optional[Any] = None
    executed_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "schema_name": self.config.schema_name,
            "table_name": self.config.table_name,
            "mode": self.config.mode.value,
            "success": self.success,
            "execution_time_ms": self.execution_time_ms,
            "rows_affected": self.rows_affected,
            "error_message": self.error_message,
            "executed_at": self.executed_at.isoformat(),
        }


class DatabaseConnection(Protocol):
    """Protocol for database connection interface."""

    def execute(self, sql: str) -> Any:
        """Execute SQL statement."""
        ...

    def fetch_one(self, sql: str) -> Optional[tuple]:
        """Fetch single row."""
        ...

    def table_exists(self, schema: str, table: str) -> bool:
        """Check if table exists."""
        ...


class BaseMaterializer(ABC):
    """Base class for materialization executors."""

    def __init__(self, connection: Optional[DatabaseConnection] = None) -> None:
        """
        Initialize materializer.

        Args:
            connection: Database connection (None for dry-run mode)
        """
        self.connection = connection

    @abstractmethod
    def execute(self, config: MaterializationConfig) -> MaterializationResult:
        """
        Execute materialization.

        Args:
            config: Materialization configuration

        Returns:
            MaterializationResult with execution details
        """
        pass

    @abstractmethod
    def generate_sql(self, config: MaterializationConfig) -> str:
        """
        Generate SQL for this materialization type.

        Args:
            config: Materialization configuration

        Returns:
            SQL string to execute
        """
        pass

    def _execute_sql(self, sql: str, config: MaterializationConfig) -> MaterializationResult:
        """
        Execute SQL and return result.

        Args:
            sql: SQL to execute
            config: Materialization configuration

        Returns:
            MaterializationResult
        """
        import time

        start_time = time.time()

        if self.connection is None:
            # Dry-run mode - just return the SQL
            return MaterializationResult(
                config=config,
                success=True,
                sql_executed=sql,
                execution_time_ms=0.0,
            )

        try:
            # Try different execution methods based on connection type
            result = None
            rows_affected = None

            if hasattr(self.connection, 'execute'):
                # Standard DatabaseConnection protocol
                result = self.connection.execute(sql)
            elif hasattr(self.connection, 'connection') and hasattr(self.connection.connection, 'raw_sql'):
                # Visitran BaseConnection with ibis backend
                result = self.connection.connection.raw_sql(sql)
            elif hasattr(self.connection, 'raw_sql'):
                # Direct ibis backend
                result = self.connection.raw_sql(sql)
            else:
                raise AttributeError(
                    f"Connection {type(self.connection).__name__} does not have a supported "
                    "execution method (execute, raw_sql)"
                )

            execution_time = (time.time() - start_time) * 1000

            if hasattr(result, "rowcount"):
                rows_affected = result.rowcount

            return MaterializationResult(
                config=config,
                success=True,
                sql_executed=sql,
                execution_time_ms=execution_time,
                rows_affected=rows_affected,
            )
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Materialization failed for {config.full_table_name}: {e}")
            return MaterializationResult(
                config=config,
                success=False,
                sql_executed=sql,
                execution_time_ms=execution_time,
                error_message=str(e),
            )


class TableMaterializer(BaseMaterializer):
    """Materializer for TABLE mode - CREATE TABLE AS SELECT."""

    def generate_sql(self, config: MaterializationConfig) -> str:
        """Generate CREATE TABLE AS SELECT statement."""
        drop_sql = ""
        if config.replace_existing:
            drop_sql = f"DROP TABLE IF EXISTS {config.full_table_name};\n"

        return f"{drop_sql}CREATE TABLE {config.full_table_name} AS (\n{config.sql}\n)"

    def execute(self, config: MaterializationConfig) -> MaterializationResult:
        """Execute table materialization."""
        sql = self.generate_sql(config)
        return self._execute_sql(sql, config)


class ViewMaterializer(BaseMaterializer):
    """Materializer for VIEW mode - CREATE OR REPLACE VIEW."""

    def generate_sql(self, config: MaterializationConfig) -> str:
        """Generate CREATE OR REPLACE VIEW statement."""
        return f"CREATE OR REPLACE VIEW {config.full_table_name} AS (\n{config.sql}\n)"

    def execute(self, config: MaterializationConfig) -> MaterializationResult:
        """Execute view materialization."""
        sql = self.generate_sql(config)
        return self._execute_sql(sql, config)


class IncrementalMaterializer(BaseMaterializer):
    """
    Materializer for INCREMENTAL mode.

    Supports two strategies:
    - APPEND: INSERT new records where incremental_key > max(existing)
    - MERGE: UPSERT based on unique_key
    """

    def _get_max_incremental_value(self, config: MaterializationConfig) -> Optional[Any]:
        """
        Get maximum value of incremental_key from target table.

        Args:
            config: Materialization configuration

        Returns:
            Maximum value or None if table doesn't exist
        """
        if self.connection is None:
            return None

        if not self.connection.table_exists(config.schema_name, config.table_name):
            return None

        sql = f"SELECT MAX({config.incremental_key}) FROM {config.full_table_name}"
        result = self.connection.fetch_one(sql)

        if result and result[0] is not None:
            return result[0]
        return None

    def generate_sql(
        self,
        config: MaterializationConfig,
        max_value: Optional[Any] = None,
    ) -> str:
        """
        Generate incremental SQL.

        For APPEND: INSERT INTO ... SELECT ... WHERE incremental_key > max_value
        For MERGE: MERGE statement (database-specific)
        """
        if max_value is None:
            # Full refresh - create table
            return f"CREATE TABLE IF NOT EXISTS {config.full_table_name} AS (\n{config.sql}\n)"

        # Filter source data to new records only
        filtered_sql = (
            f"SELECT * FROM (\n{config.sql}\n) AS __source "
            f"WHERE {config.incremental_key} > '{max_value}'"
        )

        if config.incremental_strategy == IncrementalStrategy.APPEND:
            return f"INSERT INTO {config.full_table_name}\n{filtered_sql}"

        # MERGE strategy
        if config.unique_key is None:
            raise ValueError("unique_key required for MERGE strategy")

        unique_keys = (
            config.unique_key
            if isinstance(config.unique_key, list)
            else [config.unique_key]
        )
        join_condition = " AND ".join(
            f"target.{k} = source.{k}" for k in unique_keys
        )

        # Standard SQL MERGE syntax
        return f"""
MERGE INTO {config.full_table_name} AS target
USING ({filtered_sql}) AS source
ON {join_condition}
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""".strip()

    def execute(self, config: MaterializationConfig) -> MaterializationResult:
        """Execute incremental materialization."""
        import time

        start_time = time.time()

        # Get current max value
        max_value = self._get_max_incremental_value(config)

        sql = self.generate_sql(config, max_value)
        result = self._execute_sql(sql, config)

        # Get new max value after execution
        if result.success and self.connection is not None:
            result.max_incremental_value = self._get_max_incremental_value(config)

        return result


class EphemeralMaterializer(BaseMaterializer):
    """
    Materializer for EPHEMERAL mode - CTE definition only.

    Ephemeral transformations are not persisted to the database.
    They return a CTE definition for inline substitution in downstream queries.
    """

    def generate_sql(self, config: MaterializationConfig) -> str:
        """Generate CTE definition."""
        return f"{config.table_name} AS (\n{config.sql}\n)"

    def execute(self, config: MaterializationConfig) -> MaterializationResult:
        """
        Generate CTE definition without executing DDL.

        Returns result with cte_sql populated for downstream use.
        """
        cte_sql = self.generate_sql(config)

        return MaterializationResult(
            config=config,
            success=True,
            cte_sql=cte_sql,
            sql_executed=None,  # No DDL executed
            rows_affected=None,
        )


class MaterializationHandler:
    """
    Main handler for materialization strategy execution.

    Routes transformations to appropriate materializers based on mode.

    Usage:
        handler = MaterializationHandler(connection)

        # Execute with config
        result = handler.execute(config)

        # Execute with individual parameters
        result = handler.materialize(
            schema_name="dev",
            table_name="my_model",
            sql="SELECT * FROM source",
            mode="TABLE",
        )
    """

    def __init__(self, connection: Optional[DatabaseConnection] = None) -> None:
        """
        Initialize handler with database connection.

        Args:
            connection: Database connection (None for dry-run/SQL generation only)
        """
        self.connection = connection
        self._materializers: dict[MaterializationMode, BaseMaterializer] = {
            MaterializationMode.TABLE: TableMaterializer(connection),
            MaterializationMode.VIEW: ViewMaterializer(connection),
            MaterializationMode.INCREMENTAL: IncrementalMaterializer(connection),
            MaterializationMode.EPHEMERAL: EphemeralMaterializer(connection),
        }

    def execute(self, config: MaterializationConfig) -> MaterializationResult:
        """
        Execute materialization based on config.

        Args:
            config: Materialization configuration

        Returns:
            MaterializationResult with execution details

        Raises:
            ValueError: If configuration is invalid
        """
        # Validate configuration
        errors = config.validate()
        if errors:
            return MaterializationResult(
                config=config,
                success=False,
                error_message=f"Invalid configuration: {'; '.join(errors)}",
            )

        # Get appropriate materializer
        materializer = self._materializers.get(config.mode)
        if materializer is None:
            return MaterializationResult(
                config=config,
                success=False,
                error_message=f"Unsupported materialization mode: {config.mode}",
            )

        logger.info(
            f"Executing {config.mode.value} materialization for {config.full_table_name}"
        )

        # Execute materialization
        result = materializer.execute(config)

        if result.success:
            logger.info(
                f"Materialization complete for {config.full_table_name} "
                f"in {result.execution_time_ms:.2f}ms"
            )
        else:
            logger.error(
                f"Materialization failed for {config.full_table_name}: "
                f"{result.error_message}"
            )

        return result

    def materialize(
        self,
        schema_name: str,
        table_name: str,
        sql: str,
        mode: Union[str, MaterializationMode] = MaterializationMode.TABLE,
        incremental_key: Optional[str] = None,
        unique_key: Optional[Union[str, list[str]]] = None,
        incremental_strategy: Union[str, IncrementalStrategy] = IncrementalStrategy.APPEND,
        replace_existing: bool = True,
    ) -> MaterializationResult:
        """
        Convenience method for materialization with individual parameters.

        Args:
            schema_name: Target schema
            table_name: Target table/view name
            sql: Transformation SQL
            mode: Materialization mode
            incremental_key: Column for incremental filtering
            unique_key: Column(s) for MERGE deduplication
            incremental_strategy: Strategy for incremental updates
            replace_existing: Whether to replace existing objects

        Returns:
            MaterializationResult
        """
        # Convert string to enum if needed
        if isinstance(mode, str):
            mode = MaterializationMode.from_string(mode)
        if isinstance(incremental_strategy, str):
            incremental_strategy = IncrementalStrategy(incremental_strategy.lower())

        config = MaterializationConfig(
            schema_name=schema_name,
            table_name=table_name,
            sql=sql,
            mode=mode,
            incremental_key=incremental_key,
            unique_key=unique_key,
            incremental_strategy=incremental_strategy,
            replace_existing=replace_existing,
        )

        return self.execute(config)

    def generate_sql(
        self,
        config: MaterializationConfig,
    ) -> str:
        """
        Generate SQL without executing.

        Args:
            config: Materialization configuration

        Returns:
            SQL string that would be executed
        """
        materializer = self._materializers.get(config.mode)
        if materializer is None:
            raise ValueError(f"Unsupported materialization mode: {config.mode}")

        if config.mode == MaterializationMode.INCREMENTAL:
            # For incremental, we need to potentially get max value
            inc_materializer = materializer
            if isinstance(inc_materializer, IncrementalMaterializer):
                max_value = inc_materializer._get_max_incremental_value(config)
                return inc_materializer.generate_sql(config, max_value)

        return materializer.generate_sql(config)

    def get_cte_definitions(
        self,
        ephemeral_configs: list[MaterializationConfig],
    ) -> str:
        """
        Generate combined CTE definitions for multiple ephemeral transformations.

        Args:
            ephemeral_configs: List of ephemeral materialization configs

        Returns:
            Combined CTE definition with WITH clause
        """
        if not ephemeral_configs:
            return ""

        cte_parts = []
        for config in ephemeral_configs:
            if config.mode != MaterializationMode.EPHEMERAL:
                continue
            materializer = self._materializers[MaterializationMode.EPHEMERAL]
            cte_parts.append(materializer.generate_sql(config))

        if not cte_parts:
            return ""

        return "WITH " + ",\n".join(cte_parts)


# Convenience function
def create_materialization_handler(
    connection: Optional[DatabaseConnection] = None,
) -> MaterializationHandler:
    """
    Create a MaterializationHandler instance.

    Args:
        connection: Database connection (None for dry-run mode)

    Returns:
        MaterializationHandler instance
    """
    return MaterializationHandler(connection)
