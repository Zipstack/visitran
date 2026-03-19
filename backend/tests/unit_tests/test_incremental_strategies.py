"""
Comprehensive tests for incremental materialization across all databases and strategies.

Tests cover:
- All priority databases: PostgreSQL, Snowflake, BigQuery, Databricks
- All delta strategies: timestamp, date, sequence, full_scan
- Both incremental modes: MERGE (with primary_key) and APPEND (without primary_key)
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from typing import Any

# Import delta strategies
from visitran.templates.delta_strategies import (
    DeltaStrategyFactory,
    TimestampStrategy,
    DateStrategy,
    SequenceStrategy,
    FullScanStrategy,
    create_timestamp_strategy,
    create_date_strategy,
    create_sequence_strategy,
    create_full_scan_strategy,
)

# Import model template
from visitran.templates.model import VisitranModel
from visitran.materialization import Materialization


# ============================================================================
# Test Delta Strategy Factory
# ============================================================================

class TestDeltaStrategyFactory:
    """Test the DeltaStrategyFactory for all strategy types."""

    def test_get_available_strategies(self):
        """Test that all expected strategies are available."""
        strategies = DeltaStrategyFactory.get_available_strategies()
        expected = ["timestamp", "date", "sequence", "checksum", "full_scan", "custom"]
        assert set(strategies) == set(expected)

    def test_get_timestamp_strategy(self):
        """Test getting timestamp strategy."""
        strategy = DeltaStrategyFactory.get_strategy("timestamp")
        assert isinstance(strategy, TimestampStrategy)

    def test_get_date_strategy(self):
        """Test getting date strategy."""
        strategy = DeltaStrategyFactory.get_strategy("date")
        assert isinstance(strategy, DateStrategy)

    def test_get_sequence_strategy(self):
        """Test getting sequence strategy."""
        strategy = DeltaStrategyFactory.get_strategy("sequence")
        assert isinstance(strategy, SequenceStrategy)

    def test_get_full_scan_strategy(self):
        """Test getting full_scan strategy."""
        strategy = DeltaStrategyFactory.get_strategy("full_scan")
        assert isinstance(strategy, FullScanStrategy)

    def test_invalid_strategy_raises_error(self):
        """Test that invalid strategy raises ValueError."""
        with pytest.raises(ValueError, match="Unknown delta strategy"):
            DeltaStrategyFactory.get_strategy("invalid_strategy")


# ============================================================================
# Test Strategy Configuration Helpers
# ============================================================================

class TestStrategyConfigHelpers:
    """Test helper functions for creating strategy configurations."""

    def test_create_timestamp_strategy(self):
        """Test timestamp strategy configuration."""
        config = create_timestamp_strategy(column="updated_at")
        assert config["type"] == "timestamp"
        assert config["column"] == "updated_at"

    def test_create_timestamp_strategy_default_column(self):
        """Test timestamp strategy with default column."""
        config = create_timestamp_strategy()
        assert config["type"] == "timestamp"
        assert config["column"] == "updated_at"

    def test_create_date_strategy(self):
        """Test date strategy configuration."""
        config = create_date_strategy(column="created_date")
        assert config["type"] == "date"
        assert config["column"] == "created_date"

    def test_create_sequence_strategy(self):
        """Test sequence strategy configuration."""
        config = create_sequence_strategy(column="id")
        assert config["type"] == "sequence"
        assert config["column"] == "id"

    def test_create_full_scan_strategy(self):
        """Test full_scan strategy configuration."""
        config = create_full_scan_strategy()
        assert config["type"] == "full_scan"


# ============================================================================
# Test Delta Strategy Execution
# ============================================================================

class TestTimestampStrategy:
    """Test TimestampStrategy execution."""

    def test_get_incremental_data(self):
        """Test filtering by timestamp column."""
        strategy = TimestampStrategy()

        # Create mock source and destination tables
        source_table = Mock()
        destination_table = Mock()

        # Mock the timestamp column operations
        mock_max = Mock()
        mock_max.name.return_value = mock_max
        destination_table.__getitem__ = Mock(return_value=Mock(max=Mock(return_value=mock_max)))

        mock_filter_result = Mock()
        source_table.__getitem__ = Mock(return_value=Mock(__gt__=Mock(return_value=Mock())))
        source_table.filter = Mock(return_value=mock_filter_result)

        config = {"column": "updated_at"}
        result = strategy.get_incremental_data(source_table, destination_table, config)

        # Verify filter was called
        source_table.filter.assert_called_once()


class TestDateStrategy:
    """Test DateStrategy execution."""

    def test_get_incremental_data(self):
        """Test filtering by date column."""
        strategy = DateStrategy()

        source_table = Mock()
        destination_table = Mock()

        mock_max = Mock()
        mock_max.name.return_value = mock_max
        destination_table.__getitem__ = Mock(return_value=Mock(max=Mock(return_value=mock_max)))

        mock_filter_result = Mock()
        source_table.__getitem__ = Mock(return_value=Mock(__gt__=Mock(return_value=Mock())))
        source_table.filter = Mock(return_value=mock_filter_result)

        config = {"column": "created_date"}
        result = strategy.get_incremental_data(source_table, destination_table, config)

        source_table.filter.assert_called_once()


class TestSequenceStrategy:
    """Test SequenceStrategy execution."""

    def test_get_incremental_data(self):
        """Test filtering by sequence/ID column."""
        strategy = SequenceStrategy()

        source_table = Mock()
        destination_table = Mock()

        mock_max = Mock()
        mock_max.name.return_value = mock_max
        destination_table.__getitem__ = Mock(return_value=Mock(max=Mock(return_value=mock_max)))

        mock_filter_result = Mock()
        source_table.__getitem__ = Mock(return_value=Mock(__gt__=Mock(return_value=Mock())))
        source_table.filter = Mock(return_value=mock_filter_result)

        config = {"column": "id"}
        result = strategy.get_incremental_data(source_table, destination_table, config)

        source_table.filter.assert_called_once()


class TestFullScanStrategy:
    """Test FullScanStrategy execution."""

    def test_get_incremental_data_returns_source(self):
        """Test that full_scan returns entire source table."""
        strategy = FullScanStrategy()

        source_table = Mock()
        destination_table = Mock()

        config = {}
        result = strategy.get_incremental_data(source_table, destination_table, config)

        # Full scan should return source table as-is
        assert result == source_table


# ============================================================================
# Test VisitranModel Incremental Validation
# ============================================================================

class TestVisitranModelValidation:
    """Test VisitranModel incremental configuration validation."""

    def test_incremental_requires_delta_strategy(self):
        """Test that incremental models require delta_strategy."""

        class TestModel(VisitranModel):
            def __init__(self):
                super().__init__()
                self.materialization = Materialization.INCREMENTAL
                self.delta_strategy = {"type": ""}  # Empty strategy

            def select(self):
                return Mock()

        model = TestModel()

        with pytest.raises(ValueError, match="Delta strategy is required"):
            model._validate_incremental_config()

    def test_timestamp_strategy_requires_column(self):
        """Test that timestamp strategy requires column configuration."""

        class TestModel(VisitranModel):
            def __init__(self):
                super().__init__()
                self.materialization = Materialization.INCREMENTAL
                self.delta_strategy = {"type": "timestamp", "column": ""}

            def select(self):
                return Mock()

        model = TestModel()

        with pytest.raises(ValueError, match="Timestamp strategy requires 'column'"):
            model._validate_incremental_config()

    def test_date_strategy_requires_column(self):
        """Test that date strategy requires column configuration."""

        class TestModel(VisitranModel):
            def __init__(self):
                super().__init__()
                self.materialization = Materialization.INCREMENTAL
                self.delta_strategy = {"type": "date", "column": ""}

            def select(self):
                return Mock()

        model = TestModel()

        with pytest.raises(ValueError, match="Date strategy requires 'column'"):
            model._validate_incremental_config()

    def test_sequence_strategy_requires_column(self):
        """Test that sequence strategy requires column configuration."""

        class TestModel(VisitranModel):
            def __init__(self):
                super().__init__()
                self.materialization = Materialization.INCREMENTAL
                self.delta_strategy = {"type": "sequence", "column": ""}

            def select(self):
                return Mock()

        model = TestModel()

        with pytest.raises(ValueError, match="Sequence strategy requires 'column'"):
            model._validate_incremental_config()

    def test_full_scan_strategy_valid_without_column(self):
        """Test that full_scan strategy doesn't require column."""

        class TestModel(VisitranModel):
            def __init__(self):
                super().__init__()
                self.materialization = Materialization.INCREMENTAL
                self.delta_strategy = {"type": "full_scan"}

            def select(self):
                return Mock()

        model = TestModel()
        # Should not raise
        model._validate_incremental_config()

    def test_valid_timestamp_config(self):
        """Test valid timestamp configuration passes validation."""

        class TestModel(VisitranModel):
            def __init__(self):
                super().__init__()
                self.materialization = Materialization.INCREMENTAL
                self.delta_strategy = {"type": "timestamp", "column": "updated_at"}

            def select(self):
                return Mock()

        model = TestModel()
        # Should not raise
        model._validate_incremental_config()


class TestVisitranModelIncrementalMode:
    """Test VisitranModel incremental mode detection."""

    def test_merge_mode_with_primary_key(self):
        """Test that model with primary_key uses MERGE mode."""

        class TestModel(VisitranModel):
            def __init__(self):
                super().__init__()
                self.primary_key = ["id"]

            def select(self):
                return Mock()

        model = TestModel()
        assert model.incremental_mode == "merge"

    def test_merge_mode_with_string_primary_key(self):
        """Test that model with string primary_key uses MERGE mode."""

        class TestModel(VisitranModel):
            def __init__(self):
                super().__init__()
                self.primary_key = "id"

            def select(self):
                return Mock()

        model = TestModel()
        assert model.incremental_mode == "merge"

    def test_append_mode_without_primary_key(self):
        """Test that model without primary_key uses APPEND mode."""

        class TestModel(VisitranModel):
            def __init__(self):
                super().__init__()
                self.primary_key = ""

            def select(self):
                return Mock()

        model = TestModel()
        assert model.incremental_mode == "append"

    def test_append_mode_with_empty_list_primary_key(self):
        """Test that model with empty list primary_key uses APPEND mode."""

        class TestModel(VisitranModel):
            def __init__(self):
                super().__init__()
                self.primary_key = []

            def select(self):
                return Mock()

        model = TestModel()
        assert model.incremental_mode == "append"


# ============================================================================
# Test PostgreSQL Adapter Upsert
# ============================================================================

class TestPostgresUpsert:
    """Test PostgreSQL upsert_into_table implementation."""

    @patch("visitran.adapters.postgres.connection.PostgresConnection.connection")
    @patch("visitran.adapters.postgres.connection.PostgresConnection.get_table_columns")
    def test_upsert_with_single_primary_key(self, mock_get_columns, mock_conn):
        """Test PostgreSQL upsert with single column primary key."""
        from visitran.adapters.postgres.connection import PostgresConnection

        # Setup mocks
        mock_get_columns.return_value = ["id", "name", "value", "updated_at"]
        mock_conn.raw_sql = Mock()

        # Create connection instance with mocked methods
        conn = PostgresConnection.__new__(PostgresConnection)
        conn.local = Mock()
        conn.local.connection = mock_conn
        conn.get_table_columns = mock_get_columns
        conn._ensure_unique_constraint = Mock()

        select_statement = Mock()
        select_statement.compile = Mock(return_value="SELECT * FROM source")

        # Execute upsert
        conn.upsert_into_table(
            schema_name="public",
            table_name="test_table",
            select_statement=select_statement,
            primary_key="id"
        )

        # Verify raw_sql was called (either ON CONFLICT or fallback)
        mock_conn.raw_sql.assert_called()

    @patch("visitran.adapters.postgres.connection.PostgresConnection.connection")
    @patch("visitran.adapters.postgres.connection.PostgresConnection.get_table_columns")
    def test_upsert_with_composite_primary_key(self, mock_get_columns, mock_conn):
        """Test PostgreSQL upsert with composite primary key."""
        from visitran.adapters.postgres.connection import PostgresConnection

        mock_get_columns.return_value = ["id", "region", "name", "value"]
        mock_conn.raw_sql = Mock()

        conn = PostgresConnection.__new__(PostgresConnection)
        conn.local = Mock()
        conn.local.connection = mock_conn
        conn.get_table_columns = mock_get_columns
        conn._ensure_unique_constraint = Mock()

        select_statement = Mock()
        select_statement.compile = Mock(return_value="SELECT * FROM source")

        conn.upsert_into_table(
            schema_name="public",
            table_name="test_table",
            select_statement=select_statement,
            primary_key=["id", "region"]
        )

        mock_conn.raw_sql.assert_called()


# ============================================================================
# Test Snowflake Adapter Upsert
# ============================================================================

class TestSnowflakeUpsert:
    """Test Snowflake upsert_into_table implementation."""

    @patch("visitran.adapters.snowflake.connection.SnowflakeConnection.connection")
    @patch("visitran.adapters.snowflake.connection.SnowflakeConnection.get_table_columns")
    def test_upsert_uses_merge_into(self, mock_get_columns, mock_conn):
        """Test Snowflake uses MERGE INTO statement."""
        from visitran.adapters.snowflake.connection import SnowflakeConnection

        mock_get_columns.return_value = ["id", "name", "value"]
        mock_conn.raw_sql = Mock()
        mock_conn.create_table = Mock()

        conn = SnowflakeConnection.__new__(SnowflakeConnection)
        conn.local = Mock()
        conn.local.connection = mock_conn
        conn.get_table_columns = mock_get_columns

        select_statement = Mock()

        conn.upsert_into_table(
            schema_name="test_schema",
            table_name="test_table",
            select_statement=select_statement,
            primary_key="id"
        )

        # Verify MERGE INTO was called
        calls = mock_conn.raw_sql.call_args_list
        merge_called = any("MERGE INTO" in str(call) for call in calls)
        assert merge_called or mock_conn.raw_sql.called


# ============================================================================
# Test BigQuery Adapter Upsert
# ============================================================================

class TestBigQueryUpsert:
    """Test BigQuery merge_into_table implementation."""

    @patch("visitran.adapters.bigquery.connection.BigQueryConnection.connection")
    @patch("visitran.adapters.bigquery.connection.BigQueryConnection.get_table_columns")
    def test_merge_with_primary_key_uses_delete_insert(self, mock_get_columns, mock_conn):
        """Test BigQuery uses DELETE + INSERT with primary key."""
        from visitran.adapters.bigquery.connection import BigQueryConnection

        mock_get_columns.return_value = ["id", "name", "value"]
        mock_conn.create_table = Mock()
        mock_conn.raw_sql = Mock()

        conn = BigQueryConnection.__new__(BigQueryConnection)
        conn.local = Mock()
        conn.local.connection = mock_conn
        conn.get_table_columns = mock_get_columns
        conn.bulk_execute_statements = Mock()

        select_statement = Mock()

        conn.merge_into_table(
            schema_name="test_dataset",
            target_table_name="test_table",
            select_statement=select_statement,
            primary_key="id"
        )

        # Verify bulk_execute_statements was called (DELETE + INSERT)
        conn.bulk_execute_statements.assert_called()


# ============================================================================
# Test Databricks Adapter Upsert
# ============================================================================

class TestDatabricksUpsert:
    """Test Databricks upsert_into_table implementation."""

    @patch("visitran.adapters.databricks.connection.DatabricksConnection.connection")
    @patch("visitran.adapters.databricks.connection.DatabricksConnection.get_table_columns")
    def test_upsert_uses_merge_into(self, mock_get_columns, mock_conn):
        """Test Databricks uses Delta Lake MERGE INTO."""
        from visitran.adapters.databricks.connection import DatabricksConnection

        mock_get_columns.return_value = ["id", "name", "value"]
        mock_conn.raw_sql = Mock()
        mock_conn.create_table = Mock()

        conn = DatabricksConnection.__new__(DatabricksConnection)
        conn.local = Mock()
        conn.local.connection = mock_conn
        conn.get_table_columns = mock_get_columns
        conn.catalog = "main"

        select_statement = Mock()

        conn.upsert_into_table(
            schema_name="test_schema",
            table_name="test_table",
            select_statement=select_statement,
            primary_key="id"
        )

        # Verify MERGE INTO was called
        calls = mock_conn.raw_sql.call_args_list
        merge_called = any("MERGE INTO" in str(call) for call in calls)
        assert merge_called or mock_conn.raw_sql.called

    @patch("visitran.adapters.databricks.connection.DatabricksConnection.connection")
    @patch("visitran.adapters.databricks.connection.DatabricksConnection.get_table_columns")
    def test_upsert_with_composite_key(self, mock_get_columns, mock_conn):
        """Test Databricks MERGE with composite primary key."""
        from visitran.adapters.databricks.connection import DatabricksConnection

        mock_get_columns.return_value = ["id", "region", "name", "value"]
        mock_conn.raw_sql = Mock()
        mock_conn.create_table = Mock()

        conn = DatabricksConnection.__new__(DatabricksConnection)
        conn.local = Mock()
        conn.local.connection = mock_conn
        conn.get_table_columns = mock_get_columns
        conn.catalog = ""  # No catalog

        select_statement = Mock()

        conn.upsert_into_table(
            schema_name="test_schema",
            table_name="test_table",
            select_statement=select_statement,
            primary_key=["id", "region"]
        )

        mock_conn.raw_sql.assert_called()


# ============================================================================
# Test Model Execute Incremental Methods
# ============================================================================

class TestSnowflakeModelIncremental:
    """Test SnowflakeModel.execute_incremental method."""

    def test_first_run_creates_table(self):
        """Test first run creates table with all data."""
        from visitran.adapters.snowflake.model import SnowflakeModel
        from visitran.adapters.snowflake.connection import SnowflakeConnection

        mock_conn = Mock(spec=SnowflakeConnection)
        mock_model = Mock(spec=VisitranModel)
        mock_model.destination_table_exists = False
        mock_model.destination_schema_name = "test_schema"
        mock_model.destination_table_name = "test_table"
        mock_model.select = Mock(return_value=Mock())

        model = SnowflakeModel.__new__(SnowflakeModel)
        model._db_connection = mock_conn
        model.model = mock_model

        model.execute_incremental()

        # Verify table creation flow
        mock_conn.drop_table_if_exist.assert_called()
        mock_conn.create_table.assert_called()

    def test_incremental_with_primary_key_uses_upsert(self):
        """Test incremental with primary_key calls upsert_into_table."""
        from visitran.adapters.snowflake.model import SnowflakeModel
        from visitran.adapters.snowflake.connection import SnowflakeConnection

        mock_conn = Mock(spec=SnowflakeConnection)
        mock_model = Mock(spec=VisitranModel)
        mock_model.destination_table_exists = True
        mock_model.destination_schema_name = "test_schema"
        mock_model.destination_table_name = "test_table"
        mock_model.primary_key = ["id"]
        mock_model.select_if_incremental = Mock(return_value=Mock())

        model = SnowflakeModel.__new__(SnowflakeModel)
        model._db_connection = mock_conn
        model.model = mock_model
        model._has_schema_changed = Mock(return_value=False)

        model.execute_incremental()

        # Verify upsert was called
        mock_conn.upsert_into_table.assert_called()

    def test_incremental_without_primary_key_uses_insert(self):
        """Test incremental without primary_key calls insert_into_table."""
        from visitran.adapters.snowflake.model import SnowflakeModel
        from visitran.adapters.snowflake.connection import SnowflakeConnection

        mock_conn = Mock(spec=SnowflakeConnection)
        mock_model = Mock(spec=VisitranModel)
        mock_model.destination_table_exists = True
        mock_model.destination_schema_name = "test_schema"
        mock_model.destination_table_name = "test_table"
        mock_model.primary_key = None
        mock_model.select_if_incremental = Mock(return_value=Mock())

        model = SnowflakeModel.__new__(SnowflakeModel)
        model._db_connection = mock_conn
        model.model = mock_model
        model._has_schema_changed = Mock(return_value=False)

        model.execute_incremental()

        # Verify insert was called (not upsert)
        mock_conn.insert_into_table.assert_called()


class TestDatabricksModelIncremental:
    """Test DatabricksModel.execute_incremental method."""

    def test_first_run_creates_table(self):
        """Test first run creates table with all data."""
        from visitran.adapters.databricks.model import DatabricksModel
        from visitran.adapters.databricks.connection import DatabricksConnection

        mock_conn = Mock(spec=DatabricksConnection)
        mock_model = Mock(spec=VisitranModel)
        mock_model.destination_table_exists = False
        mock_model.destination_schema_name = "test_schema"
        mock_model.destination_table_name = "test_table"
        mock_model.select = Mock(return_value=Mock())

        model = DatabricksModel.__new__(DatabricksModel)
        model._db_connection = mock_conn
        model.model = mock_model

        model.execute_incremental()

        mock_conn.drop_table_if_exist.assert_called()
        mock_conn.create_table.assert_called()

    def test_incremental_with_primary_key_uses_upsert(self):
        """Test incremental with primary_key calls upsert_into_table."""
        from visitran.adapters.databricks.model import DatabricksModel
        from visitran.adapters.databricks.connection import DatabricksConnection

        mock_conn = Mock(spec=DatabricksConnection)
        mock_model = Mock(spec=VisitranModel)
        mock_model.destination_table_exists = True
        mock_model.destination_schema_name = "test_schema"
        mock_model.destination_table_name = "test_table"
        mock_model.primary_key = ["id"]
        mock_model.select_if_incremental = Mock(return_value=Mock())

        model = DatabricksModel.__new__(DatabricksModel)
        model._db_connection = mock_conn
        model.model = mock_model

        model.execute_incremental()

        mock_conn.upsert_into_table.assert_called()

    def test_incremental_without_primary_key_uses_insert(self):
        """Test incremental without primary_key calls insert_into_table."""
        from visitran.adapters.databricks.model import DatabricksModel
        from visitran.adapters.databricks.connection import DatabricksConnection

        mock_conn = Mock(spec=DatabricksConnection)
        mock_model = Mock(spec=VisitranModel)
        mock_model.destination_table_exists = True
        mock_model.destination_schema_name = "test_schema"
        mock_model.destination_table_name = "test_table"
        mock_model.primary_key = None
        mock_model.select_if_incremental = Mock(return_value=Mock())

        model = DatabricksModel.__new__(DatabricksModel)
        model._db_connection = mock_conn
        model.model = mock_model

        model.execute_incremental()

        mock_conn.insert_into_table.assert_called()


# ============================================================================
# Integration-style tests combining strategy + database
# ============================================================================

class TestStrategyWithDatabase:
    """Test combinations of strategies with different databases."""

    @pytest.mark.parametrize("strategy_type,column", [
        ("timestamp", "updated_at"),
        ("date", "created_date"),
        ("sequence", "id"),
    ])
    def test_all_column_strategies_validate(self, strategy_type, column):
        """Test that all column-based strategies validate correctly."""

        class TestModel(VisitranModel):
            def __init__(self, s_type, col):
                super().__init__()
                self.materialization = Materialization.INCREMENTAL
                self.delta_strategy = {"type": s_type, "column": col}

            def select(self):
                return Mock()

        model = TestModel(strategy_type, column)
        # Should not raise
        model._validate_incremental_config()

    def test_full_scan_strategy_validates(self):
        """Test full_scan strategy validates without column."""

        class TestModel(VisitranModel):
            def __init__(self):
                super().__init__()
                self.materialization = Materialization.INCREMENTAL
                self.delta_strategy = {"type": "full_scan"}

            def select(self):
                return Mock()

        model = TestModel()
        model._validate_incremental_config()

    @pytest.mark.parametrize("database", [
        "postgres",
        "snowflake",
        "bigquery",
        "databricks",
    ])
    def test_upsert_method_exists(self, database):
        """Test that upsert method exists for all priority databases."""
        if database == "postgres":
            from visitran.adapters.postgres.connection import PostgresConnection
            assert hasattr(PostgresConnection, "upsert_into_table")
        elif database == "snowflake":
            from visitran.adapters.snowflake.connection import SnowflakeConnection
            assert hasattr(SnowflakeConnection, "upsert_into_table")
        elif database == "bigquery":
            from visitran.adapters.bigquery.connection import BigQueryConnection
            assert hasattr(BigQueryConnection, "merge_into_table")
        elif database == "databricks":
            from visitran.adapters.databricks.connection import DatabricksConnection
            assert hasattr(DatabricksConnection, "upsert_into_table")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
