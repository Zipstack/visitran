"""Delta Detection Strategies for Incremental Processing.

This module provides various strategies for detecting changes in data
for incremental processing. Each strategy is designed to handle
different scenarios where traditional timestamp columns may not be
available or suitable.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Union

from ibis.expr.types.relations import Table


class DeltaStrategy(ABC):
    """Abstract base class for delta detection strategies."""

    @abstractmethod
    def get_incremental_data(
        self, source_table: Table, destination_table: Table, strategy_config: dict[str, Any]
    ) -> Table:
        """Return incremental data based on the strategy."""
        pass


class TimestampStrategy(DeltaStrategy):
    """Strategy using timestamp columns (e.g., updated_at, modified_at)."""

    def get_incremental_data(
        self, source_table: Table, destination_table: Table, strategy_config: dict[str, Any]
    ) -> Table:
        """Get records updated since the last run using timestamp column."""
        timestamp_column = strategy_config.get("column", "updated_at")

        # Get the latest timestamp from destination table
        latest_timestamp = destination_table[timestamp_column].max().name("latest_timestamp")

        # Filter source table for records newer than the latest timestamp
        # Return the final incremental data ready for processing
        incremental_data = source_table.filter(source_table[timestamp_column] > latest_timestamp)

        return incremental_data


class DateStrategy(DeltaStrategy):
    """Strategy using date columns (e.g., created_date, snapshot_date)."""

    def get_incremental_data(
        self, source_table: Table, destination_table: Table, strategy_config: dict[str, Any]
    ) -> Table:
        """Get records for dates after the latest date in destination."""
        date_column = strategy_config.get("column", "created_date")

        # Get the latest date from destination table
        latest_date = destination_table[date_column].max().name("latest_date")

        # Filter source table for records with dates after the latest date
        # Return the final incremental data ready for processing
        incremental_data = source_table.filter(source_table[date_column] > latest_date)

        return incremental_data


class SequenceStrategy(DeltaStrategy):
    """Strategy using sequence/ID columns (e.g., id, sequence_number)."""

    def get_incremental_data(
        self, source_table: Table, destination_table: Table, strategy_config: dict[str, Any]
    ) -> Table:
        """Get records with sequence numbers higher than the maximum in
        destination."""
        sequence_column = strategy_config.get("column", "id")

        # Get the maximum sequence number from destination table
        max_sequence = destination_table[sequence_column].max().name("max_sequence")

        # Filter source table for records with higher sequence numbers
        # Return the final incremental data ready for processing
        incremental_data = source_table.filter(source_table[sequence_column] > max_sequence)

        return incremental_data


class ChecksumStrategy(DeltaStrategy):
    """Strategy using checksum/hash columns to detect changes."""

    def get_incremental_data(
        self, source_table: Table, destination_table: Table, strategy_config: dict[str, Any]
    ) -> Table:
        """Get records where checksum differs from destination."""
        checksum_column = strategy_config.get("column", "checksum")
        key_columns = strategy_config.get("key_columns", [])

        if not key_columns:
            raise ValueError("Checksum strategy requires key_columns configuration")

        # Join source and destination on key columns to compare checksums
        # This is a simplified version - in practice, you'd need more complex logic
        incremental_data = source_table

        return incremental_data


class FullScanStrategy(DeltaStrategy):
    """Strategy that compares all records to detect changes (expensive but
    comprehensive)."""

    def get_incremental_data(
        self, source_table: Table, destination_table: Table, strategy_config: dict[str, Any]
    ) -> Table:
        """Get all records from source table for full comparison."""
        # This strategy returns all source data for comparison
        # The actual comparison logic would be implemented in the model
        return source_table


class CustomStrategy(DeltaStrategy):
    """Strategy using custom logic provided by the user."""

    def get_incremental_data(
        self, source_table: Table, destination_table: Table, strategy_config: dict[str, Any]
    ) -> Table:
        """Execute custom logic to determine incremental data."""
        custom_logic = strategy_config.get("custom_logic")

        if not custom_logic or not callable(custom_logic):
            raise ValueError("Custom strategy requires a callable custom_logic function")

        # Execute custom logic
        return custom_logic(source_table, destination_table, strategy_config)


class DeltaStrategyFactory:
    """Factory class for creating delta detection strategies."""

    _strategies = {
        "timestamp": TimestampStrategy(),
        "date": DateStrategy(),
        "sequence": SequenceStrategy(),
        "checksum": ChecksumStrategy(),
        "full_scan": FullScanStrategy(),
        "custom": CustomStrategy(),
    }

    @classmethod
    def get_strategy(cls, strategy_type: str) -> DeltaStrategy:
        """Get a delta strategy by type."""
        if strategy_type not in cls._strategies:
            raise ValueError(f"Unknown delta strategy: {strategy_type}")

        return cls._strategies[strategy_type]

    @classmethod
    def get_available_strategies(cls) -> list[str]:
        """Get list of available strategy types."""
        return list(cls._strategies.keys())


# Helper functions for common delta detection patterns


def create_timestamp_strategy(column: str = "updated_at") -> dict[str, Any]:
    """Create a timestamp-based delta strategy configuration."""
    return {
        "type": "timestamp",
        "column": column,
    }


def create_date_strategy(column: str = "created_date") -> dict[str, Any]:
    """Create a date-based delta strategy configuration."""
    return {
        "type": "date",
        "column": column,
    }


def create_sequence_strategy(column: str = "id") -> dict[str, Any]:
    """Create a sequence-based delta strategy configuration."""
    return {
        "type": "sequence",
        "column": column,
    }


def create_checksum_strategy(checksum_column: str, key_columns: list[str]) -> dict[str, Any]:
    """Create a checksum-based delta strategy configuration."""
    return {
        "type": "checksum",
        "column": checksum_column,
        "key_columns": key_columns,
    }


def create_full_scan_strategy() -> dict[str, Any]:
    """Create a full scan delta strategy configuration."""
    return {
        "type": "full_scan",
    }


def create_custom_strategy(custom_logic: callable) -> dict[str, Any]:
    """Create a custom delta strategy configuration."""
    return {
        "type": "custom",
        "custom_logic": custom_logic,
    }
