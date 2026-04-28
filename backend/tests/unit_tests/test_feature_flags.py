"""Unit tests for Feature Flags and Execution Router."""

import json
import os
import tempfile
from datetime import datetime
from unittest.mock import patch

import pytest

from backend.application.config_parser.feature_flags import (
    ExecutionMode,
    RolloutPhase,
    FeatureFlagState,
    FeatureFlags,
    ExecutionRouter,
    ExecutionContext,
    ExecutionContextManager,
    get_context_manager,
    reset_context_manager,
    get_feature_flags,
)


class TestFeatureFlagState:
    """Tests for FeatureFlagState dataclass."""

    def test_default_state(self):
        """Test default state values."""
        state = FeatureFlagState()

        assert state.enable_direct_execution is False
        assert state.execution_mode == ExecutionMode.LEGACY
        assert state.suppress_python_files is False
        assert state.enable_sql_validation is True

    def test_to_dict(self):
        """Test state serialization."""
        state = FeatureFlagState(
            enable_direct_execution=True,
            execution_mode=ExecutionMode.PARALLEL,
        )

        d = state.to_dict()

        assert d["enable_direct_execution"] is True
        assert d["execution_mode"] == "parallel"


class TestExecutionMode:
    """Tests for ExecutionMode enum."""

    def test_values(self):
        """Test enum values."""
        assert ExecutionMode.LEGACY.value == "legacy"
        assert ExecutionMode.DIRECT.value == "direct"
        assert ExecutionMode.PARALLEL.value == "parallel"

    def test_from_string(self):
        """Test creating from string."""
        assert ExecutionMode("legacy") == ExecutionMode.LEGACY
        assert ExecutionMode("direct") == ExecutionMode.DIRECT
        assert ExecutionMode("parallel") == ExecutionMode.PARALLEL


class TestFeatureFlags:
    """Tests for FeatureFlags singleton."""

    def setup_method(self):
        """Reset feature flags before each test."""
        FeatureFlags.reset()

    def test_singleton_pattern(self):
        """Test that FeatureFlags is a singleton."""
        flags1 = FeatureFlags()
        flags2 = FeatureFlags()

        assert flags1 is flags2

    def test_default_disabled(self):
        """Test that direct execution is disabled by default."""
        assert FeatureFlags.is_direct_execution_enabled() is False
        assert FeatureFlags.get_execution_mode() == ExecutionMode.LEGACY

    def test_set_state(self):
        """Test setting feature flag state."""
        FeatureFlags.set_state(
            enable_direct_execution=True,
            execution_mode=ExecutionMode.PARALLEL,
        )

        assert FeatureFlags.is_direct_execution_enabled() is True
        assert FeatureFlags.get_execution_mode() == ExecutionMode.PARALLEL

    def test_set_state_with_string_mode(self):
        """Test setting execution mode with string."""
        FeatureFlags.set_state(execution_mode="direct")

        assert FeatureFlags.get_execution_mode() == ExecutionMode.DIRECT

    def test_get_state_returns_copy(self):
        """Test that get_state returns a copy."""
        state1 = FeatureFlags.get_state()
        state1.enable_direct_execution = True

        state2 = FeatureFlags.get_state()
        assert state2.enable_direct_execution is False

    def test_reset(self):
        """Test resetting to defaults."""
        FeatureFlags.set_state(enable_direct_execution=True)
        assert FeatureFlags.is_direct_execution_enabled() is True

        FeatureFlags.reset()

        assert FeatureFlags.is_direct_execution_enabled() is False

    def test_should_suppress_python_files(self):
        """Test suppress Python files flag."""
        assert FeatureFlags.should_suppress_python_files() is False

        FeatureFlags.set_state(suppress_python_files=True)

        assert FeatureFlags.should_suppress_python_files() is True

    def test_is_sql_validation_enabled(self):
        """Test SQL validation flag."""
        assert FeatureFlags.is_sql_validation_enabled() is True

        FeatureFlags.set_state(enable_sql_validation=False)

        assert FeatureFlags.is_sql_validation_enabled() is False


class TestFeatureFlagsEnvironment:
    """Tests for loading from environment variables."""

    def setup_method(self):
        """Reset feature flags before each test."""
        FeatureFlags.reset()

    def test_load_enable_direct_execution(self):
        """Test loading ENABLE_DIRECT_EXECUTION from env."""
        with patch.dict(os.environ, {"VISITRAN_ENABLE_DIRECT_EXECUTION": "true"}):
            FeatureFlags.reset()
            FeatureFlags()

            assert FeatureFlags.is_direct_execution_enabled() is True
            assert FeatureFlags.get_execution_mode() == ExecutionMode.PARALLEL

    def test_load_execution_mode_direct(self):
        """Test loading EXECUTION_MODE=direct from env."""
        with patch.dict(os.environ, {"VISITRAN_EXECUTION_MODE": "direct"}):
            FeatureFlags.reset()
            FeatureFlags()

            assert FeatureFlags.get_execution_mode() == ExecutionMode.DIRECT
            assert FeatureFlags.is_direct_execution_enabled() is True

    def test_load_execution_mode_legacy(self):
        """Test loading EXECUTION_MODE=legacy from env."""
        with patch.dict(os.environ, {"VISITRAN_EXECUTION_MODE": "legacy"}):
            FeatureFlags.reset()
            FeatureFlags()

            assert FeatureFlags.get_execution_mode() == ExecutionMode.LEGACY
            assert FeatureFlags.is_direct_execution_enabled() is False

    def test_load_suppress_python_files(self):
        """Test loading SUPPRESS_PYTHON_FILES from env."""
        with patch.dict(os.environ, {"VISITRAN_SUPPRESS_PYTHON_FILES": "1"}):
            FeatureFlags.reset()
            FeatureFlags()

            assert FeatureFlags.should_suppress_python_files() is True


class TestFeatureFlagsOverride:
    """Tests for FeatureFlags.override context manager."""

    def setup_method(self):
        """Reset feature flags before each test."""
        FeatureFlags.reset()

    def test_override_applies_changes(self):
        """Test that override applies changes within context."""
        assert FeatureFlags.is_direct_execution_enabled() is False

        with FeatureFlags.override(enable_direct_execution=True):
            assert FeatureFlags.is_direct_execution_enabled() is True

    def test_override_restores_on_exit(self):
        """Test that override restores state on exit."""
        assert FeatureFlags.is_direct_execution_enabled() is False

        with FeatureFlags.override(enable_direct_execution=True):
            pass

        assert FeatureFlags.is_direct_execution_enabled() is False

    def test_override_restores_on_exception(self):
        """Test that override restores state even on exception."""
        assert FeatureFlags.is_direct_execution_enabled() is False

        with pytest.raises(ValueError):
            with FeatureFlags.override(enable_direct_execution=True):
                assert FeatureFlags.is_direct_execution_enabled() is True
                raise ValueError("Test exception")

        assert FeatureFlags.is_direct_execution_enabled() is False

    def test_nested_overrides(self):
        """Test nested override contexts."""
        assert FeatureFlags.get_execution_mode() == ExecutionMode.LEGACY

        with FeatureFlags.override(execution_mode=ExecutionMode.PARALLEL):
            assert FeatureFlags.get_execution_mode() == ExecutionMode.PARALLEL

            with FeatureFlags.override(execution_mode=ExecutionMode.DIRECT):
                assert FeatureFlags.get_execution_mode() == ExecutionMode.DIRECT

            assert FeatureFlags.get_execution_mode() == ExecutionMode.PARALLEL

        assert FeatureFlags.get_execution_mode() == ExecutionMode.LEGACY


class TestExecutionRouter:
    """Tests for ExecutionRouter."""

    def setup_method(self):
        """Reset feature flags before each test."""
        FeatureFlags.reset()

    def test_legacy_mode_routing(self):
        """Test routing in legacy mode."""
        FeatureFlags.set_state(execution_mode=ExecutionMode.LEGACY)

        assert ExecutionRouter.should_execute_legacy() is True
        assert ExecutionRouter.should_execute_direct() is False
        assert ExecutionRouter.is_parallel_validation_mode() is False

    def test_direct_mode_routing(self):
        """Test routing in direct mode."""
        FeatureFlags.set_state(execution_mode=ExecutionMode.DIRECT)

        assert ExecutionRouter.should_execute_legacy() is False
        assert ExecutionRouter.should_execute_direct() is True
        assert ExecutionRouter.is_parallel_validation_mode() is False

    def test_parallel_mode_routing(self):
        """Test routing in parallel mode."""
        FeatureFlags.set_state(execution_mode=ExecutionMode.PARALLEL)

        assert ExecutionRouter.should_execute_legacy() is True
        assert ExecutionRouter.should_execute_direct() is True
        assert ExecutionRouter.is_parallel_validation_mode() is True

    def test_should_write_python_files_legacy(self):
        """Test Python file writing in legacy mode."""
        FeatureFlags.set_state(execution_mode=ExecutionMode.LEGACY)

        assert ExecutionRouter.should_write_python_files() is True

    def test_should_write_python_files_suppressed(self):
        """Test Python file writing when suppressed."""
        FeatureFlags.set_state(
            execution_mode=ExecutionMode.LEGACY,
            suppress_python_files=True,
        )

        assert ExecutionRouter.should_write_python_files() is False

    def test_should_write_python_files_direct_mode(self):
        """Test Python file writing in direct mode."""
        FeatureFlags.set_state(execution_mode=ExecutionMode.DIRECT)

        assert ExecutionRouter.should_write_python_files() is False


class TestRolloutPhase:
    """Tests for RolloutPhase enum."""

    def test_values(self):
        """Test enum values."""
        assert RolloutPhase.PHASE_1_VALIDATION.value == "phase_1_validation"
        assert RolloutPhase.PHASE_2_ALLOWLIST.value == "phase_2_allowlist"
        assert RolloutPhase.PHASE_3_FULL_ROLLOUT.value == "phase_3_full_rollout"

    def test_from_string(self):
        """Test creating from string."""
        assert RolloutPhase("phase_1_validation") == RolloutPhase.PHASE_1_VALIDATION
        assert RolloutPhase("phase_2_allowlist") == RolloutPhase.PHASE_2_ALLOWLIST
        assert RolloutPhase("phase_3_full_rollout") == RolloutPhase.PHASE_3_FULL_ROLLOUT


class TestRolloutPhaseMethods:
    """Tests for rollout phase methods in FeatureFlags."""

    def setup_method(self):
        """Reset feature flags before each test."""
        FeatureFlags.reset()

    def test_default_rollout_phase(self):
        """Test default rollout phase."""
        assert FeatureFlags.get_rollout_phase() == RolloutPhase.PHASE_1_VALIDATION

    def test_set_rollout_phase(self):
        """Test setting rollout phase."""
        FeatureFlags.set_state(rollout_phase=RolloutPhase.PHASE_2_ALLOWLIST)
        assert FeatureFlags.get_rollout_phase() == RolloutPhase.PHASE_2_ALLOWLIST

    def test_set_rollout_phase_string(self):
        """Test setting rollout phase with string."""
        FeatureFlags.set_state(rollout_phase="phase_3_full_rollout")
        assert FeatureFlags.get_rollout_phase() == RolloutPhase.PHASE_3_FULL_ROLLOUT

    def test_is_validation_only_mode(self):
        """Test validation only mode check."""
        FeatureFlags.set_state(rollout_phase=RolloutPhase.PHASE_1_VALIDATION)
        assert FeatureFlags.is_validation_only_mode() is True
        assert FeatureFlags.is_allowlist_mode() is False
        assert FeatureFlags.is_full_rollout_mode() is False

    def test_is_allowlist_mode(self):
        """Test allowlist mode check."""
        FeatureFlags.set_state(rollout_phase=RolloutPhase.PHASE_2_ALLOWLIST)
        assert FeatureFlags.is_validation_only_mode() is False
        assert FeatureFlags.is_allowlist_mode() is True
        assert FeatureFlags.is_full_rollout_mode() is False

    def test_is_full_rollout_mode(self):
        """Test full rollout mode check."""
        FeatureFlags.set_state(rollout_phase=RolloutPhase.PHASE_3_FULL_ROLLOUT)
        assert FeatureFlags.is_validation_only_mode() is False
        assert FeatureFlags.is_allowlist_mode() is False
        assert FeatureFlags.is_full_rollout_mode() is True


class TestFeatureFlagsConfigFile:
    """Tests for loading from config file."""

    def setup_method(self):
        """Reset feature flags before each test."""
        FeatureFlags.reset()

    def test_load_from_json_config(self):
        """Test loading from JSON config file."""
        config = {
            "enable_direct_execution": True,
            "execution_mode": "parallel",
            "rollout_phase": "phase_2_allowlist",
        }

        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.json', delete=False
        ) as f:
            json.dump(config, f)
            config_path = f.name

        try:
            with patch.dict(os.environ, {"VISITRAN_CONFIG_FILE": config_path}):
                FeatureFlags.reset()
                FeatureFlags()

                assert FeatureFlags.is_direct_execution_enabled() is True
                assert FeatureFlags.get_execution_mode() == ExecutionMode.PARALLEL
                assert FeatureFlags.get_rollout_phase() == RolloutPhase.PHASE_2_ALLOWLIST
        finally:
            os.unlink(config_path)

    def test_env_overrides_config_file(self):
        """Test environment variables override config file."""
        config = {
            "enable_direct_execution": False,
            "execution_mode": "legacy",
        }

        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.json', delete=False
        ) as f:
            json.dump(config, f)
            config_path = f.name

        try:
            with patch.dict(os.environ, {
                "VISITRAN_CONFIG_FILE": config_path,
                "VISITRAN_EXECUTION_MODE": "direct",
            }):
                FeatureFlags.reset()
                FeatureFlags()

                # Env should override config file
                assert FeatureFlags.get_execution_mode() == ExecutionMode.DIRECT
        finally:
            os.unlink(config_path)

    def test_invalid_bool_logs_warning(self):
        """Test that invalid boolean values log warning and default to false."""
        config = {
            "enable_direct_execution": "invalid_value",
        }

        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.json', delete=False
        ) as f:
            json.dump(config, f)
            config_path = f.name

        try:
            with patch.dict(os.environ, {"VISITRAN_CONFIG_FILE": config_path}):
                FeatureFlags.reset()
                FeatureFlags()

                # Should default to false for invalid value
                assert FeatureFlags.is_direct_execution_enabled() is False
        finally:
            os.unlink(config_path)

    def test_config_source_tracking(self):
        """Test that config source is tracked."""
        config = {"enable_direct_execution": True}

        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.json', delete=False
        ) as f:
            json.dump(config, f)
            config_path = f.name

        try:
            with patch.dict(os.environ, {"VISITRAN_CONFIG_FILE": config_path}):
                FeatureFlags.reset()
                FeatureFlags()

                state = FeatureFlags.get_state()
                assert config_path in state.config_source
        finally:
            os.unlink(config_path)

    def test_loaded_at_timestamp(self):
        """Test that loaded_at timestamp is set."""
        FeatureFlags.reset()
        FeatureFlags()

        state = FeatureFlags.get_state()
        assert state.loaded_at is not None
        assert isinstance(state.loaded_at, datetime)


class TestFeatureFlagsRolloutPhaseEnv:
    """Tests for loading rollout phase from environment."""

    def setup_method(self):
        """Reset feature flags before each test."""
        FeatureFlags.reset()

    def test_load_rollout_phase_from_env(self):
        """Test loading ROLLOUT_PHASE from env."""
        with patch.dict(os.environ, {"VISITRAN_ROLLOUT_PHASE": "phase_3_full_rollout"}):
            FeatureFlags.reset()
            FeatureFlags()

            assert FeatureFlags.get_rollout_phase() == RolloutPhase.PHASE_3_FULL_ROLLOUT

    def test_invalid_rollout_phase_uses_default(self):
        """Test that invalid rollout phase uses default."""
        with patch.dict(os.environ, {"VISITRAN_ROLLOUT_PHASE": "invalid_phase"}):
            FeatureFlags.reset()
            FeatureFlags()

            # Should use default
            assert FeatureFlags.get_rollout_phase() == RolloutPhase.PHASE_1_VALIDATION


class TestExecutionContext:
    """Tests for ExecutionContext dataclass."""

    def test_creation(self):
        """Test creating context."""
        context = ExecutionContext(
            model_name="test_model",
            execution_path="direct",
        )

        assert context.model_name == "test_model"
        assert context.execution_path == "direct"
        assert context.started_at is not None

    def test_mark_completed(self):
        """Test marking context as completed."""
        context = ExecutionContext(model_name="test")
        assert context.completed_at is None

        context.mark_completed()

        assert context.completed_at is not None

    def test_duration_ms(self):
        """Test duration calculation."""
        context = ExecutionContext(model_name="test")
        assert context.duration_ms is None

        context.mark_completed()

        assert context.duration_ms is not None
        assert context.duration_ms >= 0

    def test_set_sql(self):
        """Test setting SQL outputs."""
        context = ExecutionContext(model_name="test")

        context.set_legacy_sql("SELECT 1")
        context.set_direct_sql("SELECT 2")

        assert context.legacy_sql == "SELECT 1"
        assert context.direct_sql == "SELECT 2"

    def test_to_dict(self):
        """Test conversion to dictionary."""
        context = ExecutionContext(
            model_name="test_model",
            execution_path="legacy",
        )
        context.set_legacy_sql("SELECT 1")
        context.mark_completed()

        d = context.to_dict()

        assert d["model_name"] == "test_model"
        assert d["execution_path"] == "legacy"
        assert d["has_legacy_sql"] is True
        assert d["has_direct_sql"] is False
        assert d["duration_ms"] is not None


class TestExecutionContextManager:
    """Tests for ExecutionContextManager."""

    def setup_method(self):
        """Reset context manager before each test."""
        reset_context_manager()
        FeatureFlags.reset()

    def test_create_context(self):
        """Test creating context."""
        manager = ExecutionContextManager()

        context = manager.create_context("model_a")

        assert context.model_name == "model_a"
        assert context.execution_mode == ExecutionMode.LEGACY

    def test_create_context_with_metadata(self):
        """Test creating context with metadata."""
        manager = ExecutionContextManager()

        context = manager.create_context(
            "model_a",
            model_metadata={"schema": "public"},
        )

        assert context.model_metadata == {"schema": "public"}

    def test_get_context(self):
        """Test getting context."""
        manager = ExecutionContextManager()

        manager.create_context("model_a")
        context = manager.get_context("model_a")

        assert context is not None
        assert context.model_name == "model_a"

    def test_get_nonexistent_context(self):
        """Test getting nonexistent context."""
        manager = ExecutionContextManager()

        context = manager.get_context("nonexistent")

        assert context is None

    def test_complete_context(self):
        """Test completing context."""
        manager = ExecutionContextManager()

        manager.create_context("model_a")
        context = manager.complete_context("model_a", execution_path="direct")

        assert context is not None
        assert context.execution_path == "direct"
        assert context.completed_at is not None

    def test_clear(self):
        """Test clearing all contexts."""
        manager = ExecutionContextManager()

        manager.create_context("model_a")
        manager.create_context("model_b")
        manager.clear()

        assert manager.get_context("model_a") is None
        assert manager.get_context("model_b") is None

    def test_get_all_contexts(self):
        """Test getting all contexts."""
        manager = ExecutionContextManager()

        manager.create_context("model_a")
        manager.create_context("model_b")

        contexts = manager.get_all_contexts()

        assert len(contexts) == 2
        model_names = [c.model_name for c in contexts]
        assert "model_a" in model_names
        assert "model_b" in model_names


class TestGlobalContextManager:
    """Tests for global context manager functions."""

    def setup_method(self):
        """Reset before each test."""
        reset_context_manager()

    def test_get_context_manager(self):
        """Test getting global context manager."""
        manager = get_context_manager()

        assert manager is not None
        assert isinstance(manager, ExecutionContextManager)

    def test_get_context_manager_singleton(self):
        """Test context manager is singleton."""
        manager1 = get_context_manager()
        manager2 = get_context_manager()

        assert manager1 is manager2

    def test_reset_context_manager(self):
        """Test resetting global context manager."""
        manager1 = get_context_manager()
        reset_context_manager()
        manager2 = get_context_manager()

        assert manager1 is not manager2


class TestGetFeatureFlags:
    """Tests for get_feature_flags function."""

    def setup_method(self):
        """Reset before each test."""
        FeatureFlags.reset()

    def test_get_feature_flags_returns_instance(self):
        """Test get_feature_flags returns singleton."""
        flags = get_feature_flags()

        assert flags is not None
        assert isinstance(flags, FeatureFlags)

    def test_get_feature_flags_same_instance(self):
        """Test get_feature_flags returns same instance."""
        flags1 = get_feature_flags()
        flags2 = get_feature_flags()

        assert flags1 is flags2
