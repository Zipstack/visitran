"""Unit tests for Materialization Strategy Handler."""

from unittest.mock import MagicMock, patch

import pytest

from backend.application.config_parser.materialization_handler import (
    MaterializationMode,
    IncrementalStrategy,
    MaterializationConfig,
    MaterializationResult,
    BaseMaterializer,
    TableMaterializer,
    ViewMaterializer,
    IncrementalMaterializer,
    EphemeralMaterializer,
    MaterializationHandler,
    create_materialization_handler,
)


class TestMaterializationMode:
    """Tests for MaterializationMode enum."""

    def test_values(self):
        """Test enum values."""
        assert MaterializationMode.TABLE.value == "TABLE"
        assert MaterializationMode.VIEW.value == "VIEW"
        assert MaterializationMode.INCREMENTAL.value == "INCREMENTAL"
        assert MaterializationMode.EPHEMERAL.value == "EPHEMERAL"

    def test_from_string_valid(self):
        """Test creating from valid strings."""
        assert MaterializationMode.from_string("TABLE") == MaterializationMode.TABLE
        assert MaterializationMode.from_string("table") == MaterializationMode.TABLE
        assert MaterializationMode.from_string("View") == MaterializationMode.VIEW
        assert MaterializationMode.from_string("INCREMENTAL") == MaterializationMode.INCREMENTAL
        assert MaterializationMode.from_string("ephemeral") == MaterializationMode.EPHEMERAL

    def test_from_string_invalid(self):
        """Test creating from invalid string."""
        with pytest.raises(ValueError) as exc_info:
            MaterializationMode.from_string("invalid")

        assert "Invalid materialization mode" in str(exc_info.value)


class TestIncrementalStrategy:
    """Tests for IncrementalStrategy enum."""

    def test_values(self):
        """Test enum values."""
        assert IncrementalStrategy.APPEND.value == "append"
        assert IncrementalStrategy.MERGE.value == "merge"


class TestMaterializationConfig:
    """Tests for MaterializationConfig dataclass."""

    def test_creation_minimal(self):
        """Test creating config with minimal fields."""
        config = MaterializationConfig(
            schema_name="dev",
            table_name="my_model",
            sql="SELECT * FROM source",
        )

        assert config.schema_name == "dev"
        assert config.table_name == "my_model"
        assert config.sql == "SELECT * FROM source"
        assert config.mode == MaterializationMode.TABLE
        assert config.incremental_key is None
        assert config.replace_existing is True

    def test_creation_full(self):
        """Test creating config with all fields."""
        config = MaterializationConfig(
            schema_name="prod",
            table_name="incremental_model",
            sql="SELECT * FROM events",
            mode=MaterializationMode.INCREMENTAL,
            incremental_key="updated_at",
            unique_key="id",
            incremental_strategy=IncrementalStrategy.MERGE,
            replace_existing=False,
        )

        assert config.mode == MaterializationMode.INCREMENTAL
        assert config.incremental_key == "updated_at"
        assert config.unique_key == "id"
        assert config.incremental_strategy == IncrementalStrategy.MERGE

    def test_full_table_name(self):
        """Test full_table_name property."""
        config = MaterializationConfig(
            schema_name="dev",
            table_name="my_model",
            sql="SELECT 1",
        )

        assert config.full_table_name == "dev.my_model"

    def test_validate_valid_config(self):
        """Test validation with valid config."""
        config = MaterializationConfig(
            schema_name="dev",
            table_name="my_model",
            sql="SELECT * FROM source",
        )

        errors = config.validate()
        assert errors == []

    def test_validate_missing_schema(self):
        """Test validation with missing schema."""
        config = MaterializationConfig(
            schema_name="",
            table_name="my_model",
            sql="SELECT 1",
        )

        errors = config.validate()
        assert "schema_name is required" in errors

    def test_validate_missing_table(self):
        """Test validation with missing table."""
        config = MaterializationConfig(
            schema_name="dev",
            table_name="",
            sql="SELECT 1",
        )

        errors = config.validate()
        assert "table_name is required" in errors

    def test_validate_missing_sql(self):
        """Test validation with missing SQL."""
        config = MaterializationConfig(
            schema_name="dev",
            table_name="my_model",
            sql="",
        )

        errors = config.validate()
        assert "sql is required" in errors

    def test_validate_incremental_without_key(self):
        """Test validation of incremental mode without incremental_key."""
        config = MaterializationConfig(
            schema_name="dev",
            table_name="my_model",
            sql="SELECT 1",
            mode=MaterializationMode.INCREMENTAL,
        )

        errors = config.validate()
        assert "incremental_key is required for INCREMENTAL mode" in errors

    def test_validate_merge_without_unique_key(self):
        """Test validation of merge strategy without unique_key."""
        config = MaterializationConfig(
            schema_name="dev",
            table_name="my_model",
            sql="SELECT 1",
            mode=MaterializationMode.INCREMENTAL,
            incremental_key="updated_at",
            incremental_strategy=IncrementalStrategy.MERGE,
        )

        errors = config.validate()
        assert "unique_key is required for MERGE strategy" in errors


class TestMaterializationResult:
    """Tests for MaterializationResult dataclass."""

    def test_creation(self):
        """Test creating result."""
        config = MaterializationConfig(
            schema_name="dev",
            table_name="my_model",
            sql="SELECT 1",
        )
        result = MaterializationResult(
            config=config,
            success=True,
            execution_time_ms=100.5,
            rows_affected=10,
        )

        assert result.success is True
        assert result.execution_time_ms == 100.5
        assert result.rows_affected == 10

    def test_to_dict(self):
        """Test conversion to dictionary."""
        config = MaterializationConfig(
            schema_name="dev",
            table_name="my_model",
            sql="SELECT 1",
        )
        result = MaterializationResult(
            config=config,
            success=True,
            execution_time_ms=50.0,
        )

        d = result.to_dict()

        assert d["schema_name"] == "dev"
        assert d["table_name"] == "my_model"
        assert d["mode"] == "TABLE"
        assert d["success"] is True
        assert d["execution_time_ms"] == 50.0
        assert "executed_at" in d


class TestTableMaterializer:
    """Tests for TableMaterializer."""

    def test_generate_sql_with_replace(self):
        """Test SQL generation with replace enabled."""
        materializer = TableMaterializer()
        config = MaterializationConfig(
            schema_name="dev",
            table_name="my_model",
            sql="SELECT * FROM source",
            replace_existing=True,
        )

        sql = materializer.generate_sql(config)

        assert "DROP TABLE IF EXISTS dev.my_model" in sql
        assert "CREATE TABLE dev.my_model AS" in sql
        assert "SELECT * FROM source" in sql

    def test_generate_sql_without_replace(self):
        """Test SQL generation without replace."""
        materializer = TableMaterializer()
        config = MaterializationConfig(
            schema_name="dev",
            table_name="my_model",
            sql="SELECT * FROM source",
            replace_existing=False,
        )

        sql = materializer.generate_sql(config)

        assert "DROP TABLE" not in sql
        assert "CREATE TABLE dev.my_model AS" in sql

    def test_execute_dry_run(self):
        """Test execution in dry-run mode (no connection)."""
        materializer = TableMaterializer(connection=None)
        config = MaterializationConfig(
            schema_name="dev",
            table_name="my_model",
            sql="SELECT * FROM source",
        )

        result = materializer.execute(config)

        assert result.success is True
        assert result.sql_executed is not None
        assert "CREATE TABLE" in result.sql_executed

    def test_execute_with_connection(self):
        """Test execution with mock connection."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 100
        mock_conn.execute.return_value = mock_result

        materializer = TableMaterializer(connection=mock_conn)
        config = MaterializationConfig(
            schema_name="dev",
            table_name="my_model",
            sql="SELECT * FROM source",
        )

        result = materializer.execute(config)

        assert result.success is True
        assert result.rows_affected == 100
        mock_conn.execute.assert_called_once()


class TestViewMaterializer:
    """Tests for ViewMaterializer."""

    def test_generate_sql(self):
        """Test SQL generation for view."""
        materializer = ViewMaterializer()
        config = MaterializationConfig(
            schema_name="dev",
            table_name="my_view",
            sql="SELECT * FROM source",
            mode=MaterializationMode.VIEW,
        )

        sql = materializer.generate_sql(config)

        assert "CREATE OR REPLACE VIEW dev.my_view AS" in sql
        assert "SELECT * FROM source" in sql

    def test_execute_dry_run(self):
        """Test view execution in dry-run mode."""
        materializer = ViewMaterializer(connection=None)
        config = MaterializationConfig(
            schema_name="dev",
            table_name="my_view",
            sql="SELECT * FROM source",
            mode=MaterializationMode.VIEW,
        )

        result = materializer.execute(config)

        assert result.success is True
        assert "CREATE OR REPLACE VIEW" in result.sql_executed


class TestIncrementalMaterializer:
    """Tests for IncrementalMaterializer."""

    def test_generate_sql_full_refresh(self):
        """Test SQL for full refresh (no existing data)."""
        materializer = IncrementalMaterializer()
        config = MaterializationConfig(
            schema_name="dev",
            table_name="incremental_model",
            sql="SELECT * FROM events",
            mode=MaterializationMode.INCREMENTAL,
            incremental_key="updated_at",
        )

        sql = materializer.generate_sql(config, max_value=None)

        assert "CREATE TABLE IF NOT EXISTS dev.incremental_model AS" in sql

    def test_generate_sql_append_strategy(self):
        """Test SQL for append strategy."""
        materializer = IncrementalMaterializer()
        config = MaterializationConfig(
            schema_name="dev",
            table_name="incremental_model",
            sql="SELECT * FROM events",
            mode=MaterializationMode.INCREMENTAL,
            incremental_key="updated_at",
            incremental_strategy=IncrementalStrategy.APPEND,
        )

        sql = materializer.generate_sql(config, max_value="2024-01-01")

        assert "INSERT INTO dev.incremental_model" in sql
        assert "WHERE updated_at > '2024-01-01'" in sql

    def test_generate_sql_merge_strategy(self):
        """Test SQL for merge strategy."""
        materializer = IncrementalMaterializer()
        config = MaterializationConfig(
            schema_name="dev",
            table_name="incremental_model",
            sql="SELECT * FROM events",
            mode=MaterializationMode.INCREMENTAL,
            incremental_key="updated_at",
            unique_key="id",
            incremental_strategy=IncrementalStrategy.MERGE,
        )

        sql = materializer.generate_sql(config, max_value="2024-01-01")

        assert "MERGE INTO dev.incremental_model" in sql
        assert "target.id = source.id" in sql
        assert "WHEN MATCHED THEN UPDATE" in sql
        assert "WHEN NOT MATCHED THEN INSERT" in sql

    def test_generate_sql_merge_multiple_keys(self):
        """Test SQL for merge with multiple unique keys."""
        materializer = IncrementalMaterializer()
        config = MaterializationConfig(
            schema_name="dev",
            table_name="incremental_model",
            sql="SELECT * FROM events",
            mode=MaterializationMode.INCREMENTAL,
            incremental_key="updated_at",
            unique_key=["id", "source_system"],
            incremental_strategy=IncrementalStrategy.MERGE,
        )

        sql = materializer.generate_sql(config, max_value="2024-01-01")

        assert "target.id = source.id" in sql
        assert "target.source_system = source.source_system" in sql

    def test_get_max_incremental_value_no_connection(self):
        """Test getting max value without connection."""
        materializer = IncrementalMaterializer(connection=None)
        config = MaterializationConfig(
            schema_name="dev",
            table_name="model",
            sql="SELECT 1",
            mode=MaterializationMode.INCREMENTAL,
            incremental_key="updated_at",
        )

        max_val = materializer._get_max_incremental_value(config)

        assert max_val is None

    def test_get_max_incremental_value_table_not_exists(self):
        """Test getting max value when table doesn't exist."""
        mock_conn = MagicMock()
        mock_conn.table_exists.return_value = False

        materializer = IncrementalMaterializer(connection=mock_conn)
        config = MaterializationConfig(
            schema_name="dev",
            table_name="model",
            sql="SELECT 1",
            mode=MaterializationMode.INCREMENTAL,
            incremental_key="updated_at",
        )

        max_val = materializer._get_max_incremental_value(config)

        assert max_val is None
        mock_conn.table_exists.assert_called_once_with("dev", "model")

    def test_get_max_incremental_value_with_data(self):
        """Test getting max value from existing table."""
        mock_conn = MagicMock()
        mock_conn.table_exists.return_value = True
        mock_conn.fetch_one.return_value = ("2024-06-15",)

        materializer = IncrementalMaterializer(connection=mock_conn)
        config = MaterializationConfig(
            schema_name="dev",
            table_name="model",
            sql="SELECT 1",
            mode=MaterializationMode.INCREMENTAL,
            incremental_key="updated_at",
        )

        max_val = materializer._get_max_incremental_value(config)

        assert max_val == "2024-06-15"


class TestEphemeralMaterializer:
    """Tests for EphemeralMaterializer."""

    def test_generate_sql(self):
        """Test CTE generation."""
        materializer = EphemeralMaterializer()
        config = MaterializationConfig(
            schema_name="dev",
            table_name="ephemeral_cte",
            sql="SELECT * FROM source",
            mode=MaterializationMode.EPHEMERAL,
        )

        sql = materializer.generate_sql(config)

        assert sql == "ephemeral_cte AS (\nSELECT * FROM source\n)"

    def test_execute_no_ddl(self):
        """Test that execute doesn't run any DDL."""
        mock_conn = MagicMock()
        materializer = EphemeralMaterializer(connection=mock_conn)
        config = MaterializationConfig(
            schema_name="dev",
            table_name="ephemeral_cte",
            sql="SELECT * FROM source",
            mode=MaterializationMode.EPHEMERAL,
        )

        result = materializer.execute(config)

        assert result.success is True
        assert result.sql_executed is None  # No DDL executed
        assert result.cte_sql is not None
        assert "ephemeral_cte AS" in result.cte_sql
        mock_conn.execute.assert_not_called()


class TestMaterializationHandler:
    """Tests for MaterializationHandler."""

    def test_init_creates_materializers(self):
        """Test that handler creates all materializers."""
        handler = MaterializationHandler()

        assert MaterializationMode.TABLE in handler._materializers
        assert MaterializationMode.VIEW in handler._materializers
        assert MaterializationMode.INCREMENTAL in handler._materializers
        assert MaterializationMode.EPHEMERAL in handler._materializers

    def test_execute_table(self):
        """Test executing TABLE materialization."""
        handler = MaterializationHandler(connection=None)
        config = MaterializationConfig(
            schema_name="dev",
            table_name="my_table",
            sql="SELECT * FROM source",
            mode=MaterializationMode.TABLE,
        )

        result = handler.execute(config)

        assert result.success is True
        assert "CREATE TABLE" in result.sql_executed

    def test_execute_view(self):
        """Test executing VIEW materialization."""
        handler = MaterializationHandler(connection=None)
        config = MaterializationConfig(
            schema_name="dev",
            table_name="my_view",
            sql="SELECT * FROM source",
            mode=MaterializationMode.VIEW,
        )

        result = handler.execute(config)

        assert result.success is True
        assert "CREATE OR REPLACE VIEW" in result.sql_executed

    def test_execute_ephemeral(self):
        """Test executing EPHEMERAL materialization."""
        handler = MaterializationHandler(connection=None)
        config = MaterializationConfig(
            schema_name="dev",
            table_name="my_cte",
            sql="SELECT * FROM source",
            mode=MaterializationMode.EPHEMERAL,
        )

        result = handler.execute(config)

        assert result.success is True
        assert result.cte_sql is not None
        assert result.sql_executed is None

    def test_execute_invalid_config(self):
        """Test execution with invalid config."""
        handler = MaterializationHandler(connection=None)
        config = MaterializationConfig(
            schema_name="",
            table_name="",
            sql="",
        )

        result = handler.execute(config)

        assert result.success is False
        assert "Invalid configuration" in result.error_message

    def test_materialize_convenience_method(self):
        """Test materialize convenience method."""
        handler = MaterializationHandler(connection=None)

        result = handler.materialize(
            schema_name="dev",
            table_name="my_model",
            sql="SELECT * FROM source",
            mode="TABLE",
        )

        assert result.success is True
        assert result.config.mode == MaterializationMode.TABLE

    def test_materialize_with_string_mode(self):
        """Test materialize with string mode."""
        handler = MaterializationHandler(connection=None)

        result = handler.materialize(
            schema_name="dev",
            table_name="my_view",
            sql="SELECT 1",
            mode="view",  # lowercase string
        )

        assert result.success is True
        assert result.config.mode == MaterializationMode.VIEW

    def test_generate_sql_table(self):
        """Test generate_sql for table."""
        handler = MaterializationHandler(connection=None)
        config = MaterializationConfig(
            schema_name="dev",
            table_name="my_table",
            sql="SELECT * FROM source",
            mode=MaterializationMode.TABLE,
        )

        sql = handler.generate_sql(config)

        assert "CREATE TABLE" in sql
        assert "SELECT * FROM source" in sql

    def test_generate_sql_view(self):
        """Test generate_sql for view."""
        handler = MaterializationHandler(connection=None)
        config = MaterializationConfig(
            schema_name="dev",
            table_name="my_view",
            sql="SELECT * FROM source",
            mode=MaterializationMode.VIEW,
        )

        sql = handler.generate_sql(config)

        assert "CREATE OR REPLACE VIEW" in sql

    def test_get_cte_definitions_empty(self):
        """Test get_cte_definitions with empty list."""
        handler = MaterializationHandler()

        result = handler.get_cte_definitions([])

        assert result == ""

    def test_get_cte_definitions_single(self):
        """Test get_cte_definitions with single ephemeral."""
        handler = MaterializationHandler()
        configs = [
            MaterializationConfig(
                schema_name="dev",
                table_name="cte1",
                sql="SELECT * FROM source",
                mode=MaterializationMode.EPHEMERAL,
            ),
        ]

        result = handler.get_cte_definitions(configs)

        assert result.startswith("WITH")
        assert "cte1 AS" in result

    def test_get_cte_definitions_multiple(self):
        """Test get_cte_definitions with multiple ephemerals."""
        handler = MaterializationHandler()
        configs = [
            MaterializationConfig(
                schema_name="dev",
                table_name="cte1",
                sql="SELECT 1",
                mode=MaterializationMode.EPHEMERAL,
            ),
            MaterializationConfig(
                schema_name="dev",
                table_name="cte2",
                sql="SELECT 2",
                mode=MaterializationMode.EPHEMERAL,
            ),
        ]

        result = handler.get_cte_definitions(configs)

        assert "WITH" in result
        assert "cte1 AS" in result
        assert "cte2 AS" in result

    def test_get_cte_definitions_filters_non_ephemeral(self):
        """Test that get_cte_definitions filters out non-ephemeral configs."""
        handler = MaterializationHandler()
        configs = [
            MaterializationConfig(
                schema_name="dev",
                table_name="table1",
                sql="SELECT 1",
                mode=MaterializationMode.TABLE,
            ),
            MaterializationConfig(
                schema_name="dev",
                table_name="cte1",
                sql="SELECT 2",
                mode=MaterializationMode.EPHEMERAL,
            ),
        ]

        result = handler.get_cte_definitions(configs)

        assert "cte1 AS" in result
        assert "table1" not in result


class TestConvenienceFunction:
    """Tests for convenience functions."""

    def test_create_materialization_handler(self):
        """Test creating handler with factory function."""
        handler = create_materialization_handler()

        assert handler is not None
        assert handler.connection is None

    def test_create_materialization_handler_with_connection(self):
        """Test creating handler with connection."""
        mock_conn = MagicMock()
        handler = create_materialization_handler(connection=mock_conn)

        assert handler.connection is mock_conn


class TestMaterializationHandlerWithConnection:
    """Tests for MaterializationHandler with mock database connection."""

    def test_table_execution_success(self):
        """Test successful table materialization."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 50
        mock_conn.execute.return_value = mock_result

        handler = MaterializationHandler(connection=mock_conn)
        config = MaterializationConfig(
            schema_name="dev",
            table_name="my_table",
            sql="SELECT * FROM source",
            mode=MaterializationMode.TABLE,
        )

        result = handler.execute(config)

        assert result.success is True
        assert result.rows_affected == 50
        mock_conn.execute.assert_called_once()

    def test_table_execution_failure(self):
        """Test table materialization failure."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Database error")

        handler = MaterializationHandler(connection=mock_conn)
        config = MaterializationConfig(
            schema_name="dev",
            table_name="my_table",
            sql="SELECT * FROM source",
            mode=MaterializationMode.TABLE,
        )

        result = handler.execute(config)

        assert result.success is False
        assert "Database error" in result.error_message

    def test_incremental_full_refresh(self):
        """Test incremental materialization with full refresh."""
        mock_conn = MagicMock()
        mock_conn.table_exists.return_value = False
        mock_result = MagicMock()
        mock_result.rowcount = 100
        mock_conn.execute.return_value = mock_result

        handler = MaterializationHandler(connection=mock_conn)
        config = MaterializationConfig(
            schema_name="dev",
            table_name="incr_table",
            sql="SELECT * FROM events",
            mode=MaterializationMode.INCREMENTAL,
            incremental_key="updated_at",
        )

        result = handler.execute(config)

        assert result.success is True
        assert "CREATE TABLE IF NOT EXISTS" in result.sql_executed

    def test_incremental_append(self):
        """Test incremental materialization with append."""
        mock_conn = MagicMock()
        mock_conn.table_exists.return_value = True
        mock_conn.fetch_one.return_value = ("2024-01-01",)
        mock_result = MagicMock()
        mock_result.rowcount = 10
        mock_conn.execute.return_value = mock_result

        handler = MaterializationHandler(connection=mock_conn)
        config = MaterializationConfig(
            schema_name="dev",
            table_name="incr_table",
            sql="SELECT * FROM events",
            mode=MaterializationMode.INCREMENTAL,
            incremental_key="updated_at",
        )

        result = handler.execute(config)

        assert result.success is True
        assert "INSERT INTO" in result.sql_executed
        assert "WHERE updated_at > '2024-01-01'" in result.sql_executed
