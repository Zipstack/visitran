"""
Tests for incremental model validation functionality.
"""

import pytest
from visitran.materialization import Materialization
from visitran.templates.model import VisitranModel
from visitran.templates.delta_strategies import create_timestamp_strategy


class ValidIncrementalModel(VisitranModel):
    """Valid incremental model with proper configuration."""

    def __init__(self):
        super().__init__()
        self.materialization = Materialization.INCREMENTAL
        self.primary_key = "user_id"
        self.delta_strategy = create_timestamp_strategy(column="updated_at")

    def select(self):
        return None


class InvalidIncrementalModelNoPrimaryKey(VisitranModel):
    """Invalid incremental model missing primary key."""

    def __init__(self):
        super().__init__()
        self.materialization = Materialization.INCREMENTAL
        # Missing primary_key
        self.delta_strategy = create_timestamp_strategy(column="updated_at")

    def select(self):
        return None


class InvalidIncrementalModelNoDeltaStrategy(VisitranModel):
    """Invalid incremental model missing delta strategy."""

    def __init__(self):
        super().__init__()
        self.materialization = Materialization.INCREMENTAL
        self.primary_key = "user_id"
        # Missing delta_strategy

    def select(self):
        return None


class InvalidIncrementalModelInvalidStrategy(VisitranModel):
    """Invalid incremental model with invalid delta strategy."""

    def __init__(self):
        super().__init__()
        self.materialization = Materialization.INCREMENTAL
        self.primary_key = "user_id"
        self.delta_strategy = {"type": "invalid_strategy"}

    def select(self):
        return None


class TestIncrementalValidation:
    """Test incremental model validation."""

    def test_valid_incremental_model(self):
        """Test that valid incremental model passes validation."""
        model = ValidIncrementalModel()
        # Should not raise any exceptions
        model._validate_incremental_config()

    def test_invalid_model_no_primary_key(self):
        """Test that model without primary key raises error."""
        model = InvalidIncrementalModelNoPrimaryKey()

        with pytest.raises(ValueError) as exc_info:
            model._validate_incremental_config()

        assert "Primary key is required" in str(exc_info.value)
        assert "self.primary_key" in str(exc_info.value)

    def test_invalid_model_no_delta_strategy(self):
        """Test that model without delta strategy raises error."""
        model = InvalidIncrementalModelNoDeltaStrategy()

        with pytest.raises(ValueError) as exc_info:
            model._validate_incremental_config()

        assert "Delta strategy is required" in str(exc_info.value)
        assert "self.delta_strategy" in str(exc_info.value)

    def test_invalid_model_invalid_strategy(self):
        """Test that model with invalid strategy type raises error."""
        model = InvalidIncrementalModelInvalidStrategy()

        with pytest.raises(ValueError) as exc_info:
            model._validate_incremental_config()

        assert "Unknown delta strategy type" in str(exc_info.value)
        assert "invalid_strategy" in str(exc_info.value)

    def test_non_incremental_model_no_validation(self):
        """Test that non-incremental models don't require validation."""
        model = VisitranModel()
        model.materialization = Materialization.TABLE  # Not incremental

        # Should not raise any exceptions
        model._validate_incremental_config()
