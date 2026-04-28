"""Unit tests for Fallback Handler and Graceful Degradation."""

import time
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from backend.application.config_parser.feature_flags import (
    ExecutionMode,
    FeatureFlags,
)
from backend.application.config_parser.fallback_handler import (
    FallbackReason,
    FallbackResult,
    CircuitBreaker,
    CircuitBreakerState,
    AllowlistManager,
    FallbackHandler,
    ValidationMetrics,
    MetricsCollector,
    get_fallback_handler,
    get_metrics_collector,
    reset_fallback_handler,
    reset_metrics_collector,
)


class TestFallbackReason:
    """Tests for FallbackReason enum."""

    def test_values(self):
        """Test enum values."""
        assert FallbackReason.DIRECT_EXECUTION_ERROR.value == "direct_execution_error"
        assert FallbackReason.VALIDATION_MISMATCH.value == "validation_mismatch"
        assert FallbackReason.TIMEOUT.value == "timeout"
        assert FallbackReason.NOT_ON_ALLOWLIST.value == "not_on_allowlist"
        assert FallbackReason.FEATURE_DISABLED.value == "feature_disabled"
        assert FallbackReason.CIRCUIT_BREAKER_OPEN.value == "circuit_breaker_open"


class TestFallbackResult:
    """Tests for FallbackResult dataclass."""

    def test_creation_success(self):
        """Test creating successful result."""
        result = FallbackResult(
            value="success",
            used_fallback=False,
            direct_execution_ms=50.0,
        )

        assert result.value == "success"
        assert result.used_fallback is False
        assert result.direct_execution_ms == 50.0

    def test_creation_with_fallback(self):
        """Test creating result with fallback."""
        result = FallbackResult(
            value="fallback_value",
            used_fallback=True,
            fallback_reason=FallbackReason.DIRECT_EXECUTION_ERROR,
            direct_execution_ms=10.0,
            fallback_execution_ms=100.0,
            error="Connection timeout",
            error_type="TimeoutError",
        )

        assert result.used_fallback is True
        assert result.fallback_reason == FallbackReason.DIRECT_EXECUTION_ERROR
        assert result.error == "Connection timeout"


class TestCircuitBreaker:
    """Tests for CircuitBreaker."""

    def test_init_defaults(self):
        """Test default initialization."""
        cb = CircuitBreaker()

        assert cb.failure_threshold == 5
        assert cb.cooldown_seconds == 60.0

    def test_init_custom(self):
        """Test custom initialization."""
        cb = CircuitBreaker(failure_threshold=3, cooldown_seconds=30.0)

        assert cb.failure_threshold == 3
        assert cb.cooldown_seconds == 30.0

    def test_initially_closed(self):
        """Test circuit is initially closed."""
        cb = CircuitBreaker()

        assert cb.is_open("model_a") is False

    def test_record_success_resets(self):
        """Test that success resets failure count."""
        cb = CircuitBreaker(failure_threshold=3)

        # Record some failures
        cb.record_failure("model_a")
        cb.record_failure("model_a")

        # Record success
        cb.record_success("model_a")

        # Failure count should be reset
        state = cb.get_state("model_a")
        assert state is not None
        assert state.failure_count == 0

    def test_opens_after_threshold(self):
        """Test circuit opens after threshold failures."""
        cb = CircuitBreaker(failure_threshold=3)

        cb.record_failure("model_a")
        assert cb.is_open("model_a") is False

        cb.record_failure("model_a")
        assert cb.is_open("model_a") is False

        cb.record_failure("model_a")  # Third failure
        assert cb.is_open("model_a") is True

    def test_resets_after_cooldown(self):
        """Test circuit resets after cooldown."""
        cb = CircuitBreaker(failure_threshold=2, cooldown_seconds=0.1)

        # Open the circuit
        cb.record_failure("model_a")
        cb.record_failure("model_a")
        assert cb.is_open("model_a") is True

        # Wait for cooldown
        time.sleep(0.15)

        # Should be closed now
        assert cb.is_open("model_a") is False

    def test_independent_models(self):
        """Test circuits are independent per model."""
        cb = CircuitBreaker(failure_threshold=2)

        # Open circuit for model_a
        cb.record_failure("model_a")
        cb.record_failure("model_a")

        # model_b should still be closed
        assert cb.is_open("model_a") is True
        assert cb.is_open("model_b") is False

    def test_reset_single_model(self):
        """Test resetting a single model."""
        cb = CircuitBreaker(failure_threshold=2)

        cb.record_failure("model_a")
        cb.record_failure("model_a")
        cb.record_failure("model_b")
        cb.record_failure("model_b")

        cb.reset("model_a")

        assert cb.is_open("model_a") is False
        assert cb.is_open("model_b") is True

    def test_reset_all(self):
        """Test resetting all models."""
        cb = CircuitBreaker(failure_threshold=2)

        cb.record_failure("model_a")
        cb.record_failure("model_a")
        cb.record_failure("model_b")
        cb.record_failure("model_b")

        cb.reset()

        assert cb.is_open("model_a") is False
        assert cb.is_open("model_b") is False


class TestAllowlistManager:
    """Tests for AllowlistManager."""

    def test_init_defaults(self):
        """Test default initialization."""
        manager = AllowlistManager(use_db=False)

        # Should allow all when no patterns specified
        assert manager.is_allowed("any_model") is True

    def test_init_with_patterns(self):
        """Test initialization with patterns."""
        manager = AllowlistManager(
            use_db=False,
            default_patterns=["schema.allowed_*"],
        )

        assert manager.is_allowed("schema.allowed_model") is True
        assert manager.is_allowed("schema.denied_model") is False

    def test_wildcard_matching(self):
        """Test wildcard pattern matching."""
        manager = AllowlistManager(
            use_db=False,
            default_patterns=["*.orders", "staging.*"],
        )

        assert manager.is_allowed("schema.orders") is True
        assert manager.is_allowed("staging.anything") is True
        assert manager.is_allowed("prod.users") is False

    def test_exact_matching(self):
        """Test exact name matching."""
        manager = AllowlistManager(
            use_db=False,
            default_patterns=["exact_model"],
        )

        assert manager.is_allowed("exact_model") is True
        assert manager.is_allowed("exact_model_extra") is False

    def test_add_pattern(self):
        """Test adding patterns."""
        # Start with a specific allowlist (not empty - empty allows all)
        manager = AllowlistManager(
            use_db=False,
            default_patterns=["existing_*"],
        )

        # new_model doesn't match existing pattern
        assert manager.is_allowed("new_model") is False
        assert manager.is_allowed("existing_model") is True

        manager.add_pattern("new_*")

        # Need to invalidate cache
        manager.invalidate_cache()

        # Now new_model matches new_* pattern
        assert manager.is_allowed("new_model") is True

    def test_cache_invalidation(self):
        """Test cache invalidation."""
        manager = AllowlistManager(
            use_db=False,
            default_patterns=["model_a"],
        )

        # Warm the cache
        assert manager.is_allowed("model_a") is True
        assert manager.is_allowed("model_b") is False  # Not in pattern

        # Verify it's cached
        assert "model_b" in manager._cache

        # Add new pattern that includes model_b
        manager._default_patterns.append("model_b")

        # Still cached as False
        assert manager.is_allowed("model_b") is False  # Still cached

        # Invalidate
        manager.invalidate_cache()

        # Now should be allowed
        assert manager.is_allowed("model_b") is True


class TestFallbackHandler:
    """Tests for FallbackHandler."""

    def test_init_defaults(self):
        """Test default initialization."""
        reset_fallback_handler()
        handler = FallbackHandler()

        assert handler._feature_flags is None
        assert handler._circuit_breaker is not None

    def test_should_use_direct_when_enabled(self):
        """Test direct execution when feature is enabled."""
        FeatureFlags.reset()
        FeatureFlags.set_state(execution_mode=ExecutionMode.DIRECT)
        handler = FallbackHandler()

        should_use, reason = handler.should_use_direct_execution("model_a")

        assert should_use is True
        assert reason is None

    def test_should_not_use_direct_when_disabled(self):
        """Test no direct execution when feature is disabled."""
        FeatureFlags.reset()
        FeatureFlags.set_state(execution_mode=ExecutionMode.LEGACY)
        handler = FallbackHandler()

        should_use, reason = handler.should_use_direct_execution("model_a")

        assert should_use is False
        assert reason == FallbackReason.FEATURE_DISABLED

    def test_should_not_use_direct_when_circuit_open(self):
        """Test no direct execution when circuit breaker is open."""
        FeatureFlags.reset()
        FeatureFlags.set_state(execution_mode=ExecutionMode.DIRECT)
        cb = CircuitBreaker(failure_threshold=2)

        # Open the circuit
        cb.record_failure("model_a")
        cb.record_failure("model_a")

        handler = FallbackHandler(circuit_breaker=cb)

        should_use, reason = handler.should_use_direct_execution("model_a")

        assert should_use is False
        assert reason == FallbackReason.CIRCUIT_BREAKER_OPEN

    def test_should_not_use_direct_when_not_allowlisted(self):
        """Test no direct execution when model not on allowlist."""
        FeatureFlags.reset()
        FeatureFlags.set_state(execution_mode=ExecutionMode.DIRECT)
        allowlist = AllowlistManager(
            use_db=False,
            default_patterns=["allowed_*"],
        )

        handler = FallbackHandler(allowlist_manager=allowlist)

        should_use, reason = handler.should_use_direct_execution("denied_model")

        assert should_use is False
        assert reason == FallbackReason.NOT_ON_ALLOWLIST

    def test_execute_with_fallback_direct_success(self):
        """Test successful direct execution."""
        FeatureFlags.reset()
        FeatureFlags.set_state(execution_mode=ExecutionMode.DIRECT)
        handler = FallbackHandler()

        def direct_fn():
            return "direct_result"

        def legacy_fn():
            return "legacy_result"

        result = handler.execute_with_fallback(
            model_name="model_a",
            direct_fn=direct_fn,
            legacy_fn=legacy_fn,
        )

        assert result.value == "direct_result"
        assert result.used_fallback is False
        assert result.direct_execution_ms is not None

    def test_execute_with_fallback_direct_failure(self):
        """Test fallback on direct execution failure."""
        FeatureFlags.reset()
        FeatureFlags.set_state(execution_mode=ExecutionMode.DIRECT)
        handler = FallbackHandler()

        def direct_fn():
            raise ValueError("Direct failed")

        def legacy_fn():
            return "legacy_result"

        result = handler.execute_with_fallback(
            model_name="model_a",
            direct_fn=direct_fn,
            legacy_fn=legacy_fn,
        )

        assert result.value == "legacy_result"
        assert result.used_fallback is True
        assert result.fallback_reason == FallbackReason.DIRECT_EXECUTION_ERROR
        assert "Direct failed" in result.error

    def test_execute_with_fallback_both_fail(self):
        """Test when both direct and fallback fail."""
        FeatureFlags.reset()
        FeatureFlags.set_state(execution_mode=ExecutionMode.DIRECT)
        handler = FallbackHandler()

        def direct_fn():
            raise ValueError("Direct failed")

        def legacy_fn():
            raise RuntimeError("Legacy also failed")

        with pytest.raises(RuntimeError, match="Legacy also failed"):
            handler.execute_with_fallback(
                model_name="model_a",
                direct_fn=direct_fn,
                legacy_fn=legacy_fn,
            )

    def test_execute_with_fallback_validation_mismatch(self):
        """Test fallback on validation mismatch."""
        FeatureFlags.reset()
        FeatureFlags.set_state(execution_mode=ExecutionMode.DIRECT)
        handler = FallbackHandler()

        def direct_fn():
            return "direct_result"

        def legacy_fn():
            return "legacy_result"

        def validate(direct, legacy):
            return direct == legacy  # Will fail

        result = handler.execute_with_fallback(
            model_name="model_a",
            direct_fn=direct_fn,
            legacy_fn=legacy_fn,
            validate_result=validate,
        )

        assert result.value == "legacy_result"
        assert result.used_fallback is True
        assert result.fallback_reason == FallbackReason.VALIDATION_MISMATCH

    def test_execute_skips_direct_when_disabled(self):
        """Test direct execution is skipped when feature disabled."""
        FeatureFlags.reset()
        FeatureFlags.set_state(execution_mode=ExecutionMode.LEGACY)
        handler = FallbackHandler()

        direct_called = False

        def direct_fn():
            nonlocal direct_called
            direct_called = True
            return "direct"

        def legacy_fn():
            return "legacy"

        result = handler.execute_with_fallback(
            model_name="model_a",
            direct_fn=direct_fn,
            legacy_fn=legacy_fn,
        )

        assert result.value == "legacy"
        assert result.used_fallback is True
        assert result.fallback_reason == FallbackReason.FEATURE_DISABLED
        assert direct_called is False  # Direct was never called

    def test_fallback_context_manager(self):
        """Test fallback context manager."""
        FeatureFlags.reset()
        FeatureFlags.set_state(execution_mode=ExecutionMode.DIRECT)
        handler = FallbackHandler()

        with handler.fallback_context("model_a") as execute:
            result = execute(
                direct_fn=lambda: "direct",
                legacy_fn=lambda: "legacy",
            )

        assert result.value == "direct"
        assert result.used_fallback is False


class TestMetricsCollector:
    """Tests for MetricsCollector."""

    def test_init(self):
        """Test initialization."""
        collector = MetricsCollector()

        metrics = collector.get_metrics()
        assert metrics.total_executions == 0

    def test_record_direct_success(self):
        """Test recording direct success."""
        collector = MetricsCollector()

        collector.record_direct_success(50.0)
        collector.record_direct_success(100.0)

        metrics = collector.get_metrics()

        assert metrics.total_executions == 2
        assert metrics.direct_success_count == 2
        assert metrics.fallback_count == 0
        assert metrics.direct_success_rate == 1.0
        assert metrics.avg_direct_latency_ms == 75.0

    def test_record_fallback(self):
        """Test recording fallback."""
        collector = MetricsCollector()

        collector.record_fallback(200.0)

        metrics = collector.get_metrics()

        assert metrics.total_executions == 1
        assert metrics.fallback_count == 1
        assert metrics.direct_success_rate == 0.0
        assert metrics.avg_fallback_latency_ms == 200.0

    def test_mixed_recordings(self):
        """Test mixed success and fallback recordings."""
        collector = MetricsCollector()

        collector.record_direct_success(50.0)
        collector.record_direct_success(50.0)
        collector.record_direct_success(50.0)
        collector.record_fallback(100.0)

        metrics = collector.get_metrics()

        assert metrics.total_executions == 4
        assert metrics.direct_success_count == 3
        assert metrics.fallback_count == 1
        assert metrics.direct_success_rate == 0.75

    def test_reset(self):
        """Test resetting metrics."""
        collector = MetricsCollector()

        collector.record_direct_success(50.0)
        collector.record_fallback(100.0)
        collector.reset()

        metrics = collector.get_metrics()

        assert metrics.total_executions == 0
        assert metrics.direct_success_count == 0
        assert metrics.fallback_count == 0

    def test_get_metrics_with_circuit_breaker(self):
        """Test getting metrics with circuit breaker info."""
        collector = MetricsCollector()
        cb = CircuitBreaker(failure_threshold=2)

        # Open circuit for one model
        cb.record_failure("model_a")
        cb.record_failure("model_a")

        metrics = collector.get_metrics(circuit_breaker=cb)

        assert "model_a" in metrics.models_with_open_circuit


class TestValidationMetrics:
    """Tests for ValidationMetrics dataclass."""

    def test_creation(self):
        """Test creating metrics."""
        metrics = ValidationMetrics(
            total_executions=100,
            direct_success_count=80,
            fallback_count=20,
            direct_success_rate=0.8,
        )

        assert metrics.total_executions == 100
        assert metrics.direct_success_rate == 0.8

    def test_to_dict(self):
        """Test conversion to dictionary."""
        metrics = ValidationMetrics(
            total_executions=50,
            direct_success_count=45,
            fallback_count=5,
            direct_success_rate=0.9,
            avg_direct_latency_ms=25.0,
            models_with_open_circuit=["model_x"],
        )

        d = metrics.to_dict()

        assert d["total_executions"] == 50
        assert d["direct_success_rate"] == 0.9
        assert "model_x" in d["models_with_open_circuit"]


class TestGlobalFunctions:
    """Tests for global convenience functions."""

    def test_get_fallback_handler(self):
        """Test getting global handler."""
        reset_fallback_handler()

        handler = get_fallback_handler()

        assert handler is not None
        assert isinstance(handler, FallbackHandler)

    def test_get_fallback_handler_singleton(self):
        """Test handler is singleton."""
        reset_fallback_handler()

        handler1 = get_fallback_handler()
        handler2 = get_fallback_handler()

        assert handler1 is handler2

    def test_get_metrics_collector(self):
        """Test getting global collector."""
        reset_metrics_collector()

        collector = get_metrics_collector()

        assert collector is not None
        assert isinstance(collector, MetricsCollector)

    def test_reset_functions(self):
        """Test reset functions."""
        reset_fallback_handler()
        reset_metrics_collector()

        handler1 = get_fallback_handler()
        collector1 = get_metrics_collector()

        reset_fallback_handler()
        reset_metrics_collector()

        handler2 = get_fallback_handler()
        collector2 = get_metrics_collector()

        assert handler1 is not handler2
        assert collector1 is not collector2


class TestIntegration:
    """Integration tests for fallback handling."""

    def test_full_workflow(self):
        """Test complete fallback workflow."""
        FeatureFlags.reset()
        FeatureFlags.set_state(execution_mode=ExecutionMode.DIRECT)
        cb = CircuitBreaker(failure_threshold=3)
        collector = MetricsCollector()
        handler = FallbackHandler(circuit_breaker=cb)

        # Successful direct executions
        for _ in range(5):
            result = handler.execute_with_fallback(
                model_name="good_model",
                direct_fn=lambda: "success",
                legacy_fn=lambda: "fallback",
            )
            if not result.used_fallback:
                collector.record_direct_success(result.direct_execution_ms or 0)

        # Failed executions (trigger circuit breaker)
        for i in range(3):
            result = handler.execute_with_fallback(
                model_name="bad_model",
                direct_fn=lambda: (_ for _ in ()).throw(ValueError("fail")),
                legacy_fn=lambda: "fallback",
            )
            if result.used_fallback:
                collector.record_fallback(result.fallback_execution_ms or 0)

        # Check metrics
        metrics = collector.get_metrics(circuit_breaker=cb)

        assert metrics.direct_success_count == 5
        assert metrics.fallback_count == 3
        assert "bad_model" in metrics.models_with_open_circuit

        # Circuit should be open for bad_model
        assert cb.is_open("bad_model") is True
        assert cb.is_open("good_model") is False

    def test_operates_with_100_percent_fallback(self):
        """Test system can operate with 100% fallback rate."""
        FeatureFlags.reset()
        FeatureFlags.set_state(execution_mode=ExecutionMode.DIRECT)
        handler = FallbackHandler()

        results = []

        # All direct executions fail
        for i in range(10):
            result = handler.execute_with_fallback(
                model_name=f"model_{i}",
                direct_fn=lambda: (_ for _ in ()).throw(ValueError("always fail")),
                legacy_fn=lambda: f"legacy_result",
            )
            results.append(result)

        # All should have used fallback
        assert all(r.used_fallback for r in results)
        assert all(r.value == "legacy_result" for r in results)
