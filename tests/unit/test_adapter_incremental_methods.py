"""
Tests for verifying incremental materialization methods exist in all adapters.
"""

import pytest


class TestAdapterIncrementalMethods:
    """Verify all adapters have required incremental materialization methods."""

    def test_base_model_has_schema_changed(self):
        """Test BaseModel has _has_schema_changed method."""
        from visitran.adapters.model import BaseModel
        assert hasattr(BaseModel, '_has_schema_changed'), "BaseModel missing _has_schema_changed"

    def test_postgres_model_execute_incremental(self):
        """Test PostgresModel has execute_incremental method."""
        from visitran.adapters.postgres.model import PostgresModel
        assert hasattr(PostgresModel, 'execute_incremental'), "PostgresModel missing execute_incremental"

    def test_postgres_model_full_refresh(self):
        """Test PostgresModel has _full_refresh_table method."""
        from visitran.adapters.postgres.model import PostgresModel
        assert hasattr(PostgresModel, '_full_refresh_table'), "PostgresModel missing _full_refresh_table"

    def test_postgres_connection_upsert(self):
        """Test PostgresConnection has upsert_into_table method."""
        from visitran.adapters.postgres.connection import PostgresConnection
        assert hasattr(PostgresConnection, 'upsert_into_table'), "PostgresConnection missing upsert_into_table"

    def test_bigquery_model_execute_incremental(self):
        """Test BigQueryModel has execute_incremental method."""
        from visitran.adapters.bigquery.model import BigQueryModel
        assert hasattr(BigQueryModel, 'execute_incremental'), "BigQueryModel missing execute_incremental"

    def test_bigquery_model_full_refresh(self):
        """Test BigQueryModel has _full_refresh_table method."""
        from visitran.adapters.bigquery.model import BigQueryModel
        assert hasattr(BigQueryModel, '_full_refresh_table'), "BigQueryModel missing _full_refresh_table"

    def test_bigquery_connection_merge(self):
        """Test BigQueryConnection has merge_into_table method."""
        from visitran.adapters.bigquery.connection import BigQueryConnection
        assert hasattr(BigQueryConnection, 'merge_into_table'), "BigQueryConnection missing merge_into_table"

    def test_snowflake_model_execute_incremental(self):
        """Test SnowflakeModel has execute_incremental method."""
        from visitran.adapters.snowflake.model import SnowflakeModel
        assert hasattr(SnowflakeModel, 'execute_incremental'), "SnowflakeModel missing execute_incremental"

    def test_snowflake_model_full_refresh(self):
        """Test SnowflakeModel has _full_refresh_table method."""
        from visitran.adapters.snowflake.model import SnowflakeModel
        assert hasattr(SnowflakeModel, '_full_refresh_table'), "SnowflakeModel missing _full_refresh_table"

    def test_snowflake_connection_upsert(self):
        """Test SnowflakeConnection has upsert_into_table method."""
        from visitran.adapters.snowflake.connection import SnowflakeConnection
        assert hasattr(SnowflakeConnection, 'upsert_into_table'), "SnowflakeConnection missing upsert_into_table"

    def test_trino_model_execute_incremental(self):
        """Test TrinoModel has execute_incremental method."""
        from visitran.adapters.trino.model import TrinoModel
        assert hasattr(TrinoModel, 'execute_incremental'), "TrinoModel missing execute_incremental"

    def test_trino_model_full_refresh(self):
        """Test TrinoModel has _full_refresh_table method."""
        from visitran.adapters.trino.model import TrinoModel
        assert hasattr(TrinoModel, '_full_refresh_table'), "TrinoModel missing _full_refresh_table"

    def test_trino_connection_upsert(self):
        """Test TrinoQEConnection has upsert_into_table method."""
        from visitran.adapters.trino.connection import TrinoQEConnection
        assert hasattr(TrinoQEConnection, 'upsert_into_table'), "TrinoQEConnection missing upsert_into_table"


class TestDeltaStrategies:
    """Test delta strategy module."""

    def test_all_strategies_available(self):
        """Test all expected strategies are available."""
        from visitran.templates.delta_strategies import DeltaStrategyFactory

        strategies = DeltaStrategyFactory.get_available_strategies()
        assert 'timestamp' in strategies
        assert 'date' in strategies
        assert 'sequence' in strategies
        assert 'checksum' in strategies
        assert 'full_scan' in strategies
        assert 'custom' in strategies

    def test_timestamp_strategy_factory(self):
        """Test factory returns correct strategy type."""
        from visitran.templates.delta_strategies import (
            DeltaStrategyFactory,
            TimestampStrategy,
        )

        strategy = DeltaStrategyFactory.get_strategy('timestamp')
        assert isinstance(strategy, TimestampStrategy)

    def test_create_timestamp_strategy_helper(self):
        """Test create_timestamp_strategy helper function."""
        from visitran.templates.delta_strategies import create_timestamp_strategy

        config = create_timestamp_strategy('updated_at')
        assert config['type'] == 'timestamp'
        assert config['column'] == 'updated_at'

    def test_create_date_strategy_helper(self):
        """Test create_date_strategy helper function."""
        from visitran.templates.delta_strategies import create_date_strategy

        config = create_date_strategy('created_date')
        assert config['type'] == 'date'
        assert config['column'] == 'created_date'

    def test_create_sequence_strategy_helper(self):
        """Test create_sequence_strategy helper function."""
        from visitran.templates.delta_strategies import create_sequence_strategy

        config = create_sequence_strategy('id')
        assert config['type'] == 'sequence'
        assert config['column'] == 'id'
