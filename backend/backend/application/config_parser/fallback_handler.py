"""
Fallback Handler for Graceful Degradation.

This module provides automatic fallback from direct execution to legacy
execution when failures occur, ensuring continuous operation without
user intervention.

Usage:
    handler = FallbackHandler()

    # Execute with automatic fallback
    result = handler.execute_with_fallback(
        model_name="my_model",
        direct_fn=direct_execute,
        legacy_fn=legacy_execute,
    )
"""

from __future__ import annotations

import functools
import logging
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from threading import Lock
from typing import Any, Callable, Generic, Optional, TypeVar

from backend.application.config_parser.feature_flags import (
    ExecutionMode,
    FeatureFlags,
    get_feature_flags,
)
try:
    from backend.application.config_parser.validation_storage_service import (
        ValidationStorageService,
        get_validation_storage_service,
    )
except ImportError:
    ValidationStorageService = None  # type: ignore
    get_validation_storage_service = None  # type: ignore

logger = logging.getLogger(__name__)

T = TypeVar("T")


class FallbackReason(Enum):
    """Reasons for falling back to legacy execution."""

    DIRECT_EXECUTION_ERROR = "direct_execution_error"
    VALIDATION_MISMATCH = "validation_mismatch"
    TIMEOUT = "timeout"
    NOT_ON_ALLOWLIST = "not_on_allowlist"
    FEATURE_DISABLED = "feature_disabled"
    CIRCUIT_BREAKER_OPEN = "circuit_breaker_open"


@dataclass
class FallbackResult(Generic[T]):
    """
    Result of an execution with potential fallback.

    Attributes:
        value: The result value from execution
        used_fallback: Whether fallback was used
        fallback_reason: Why fallback was used (if applicable)
        direct_execution_ms: Time spent on direct execution (if attempted)
        fallback_execution_ms: Time spent on fallback (if used)
        error: Error that triggered fallback (if applicable)
    """

    value: T
    used_fallback: bool = False
    fallback_reason: Optional[FallbackReason] = None
    direct_execution_ms: Optional[float] = None
    fallback_execution_ms: Optional[float] = None
    error: Optional[str] = None
    error_type: Optional[str] = None


@dataclass
class CircuitBreakerState:
    """
    State for circuit breaker pattern.

    Attributes:
        failure_count: Current consecutive failure count
        last_failure_time: When the last failure occurred
        is_open: Whether the circuit is open (blocking direct execution)
        last_reset_time: When the circuit was last reset
    """

    failure_count: int = 0
    last_failure_time: Optional[datetime] = None
    is_open: bool = False
    last_reset_time: datetime = field(default_factory=datetime.utcnow)


class CircuitBreaker:
    """
    Circuit breaker to prevent repeated failures.

    When failures exceed a threshold, the circuit opens and
    all requests bypass direct execution for a cooldown period.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        cooldown_seconds: float = 60.0,
    ) -> None:
        """
        Initialize the circuit breaker.

        Args:
            failure_threshold: Failures before circuit opens
            cooldown_seconds: Seconds to wait before retrying
        """
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self._state: dict[str, CircuitBreakerState] = {}
        self._lock = Lock()

    def is_open(self, model_name: str) -> bool:
        """
        Check if circuit is open for a model.

        Args:
            model_name: The model to check

        Returns:
            True if circuit is open (should use fallback)
        """
        with self._lock:
            state = self._state.get(model_name)

            if state is None:
                return False

            if not state.is_open:
                return False

            # Check if cooldown has passed
            if state.last_failure_time:
                elapsed = (datetime.utcnow() - state.last_failure_time).total_seconds()
                if elapsed >= self.cooldown_seconds:
                    # Reset the circuit to half-open (allow one try)
                    state.is_open = False
                    state.failure_count = 0
                    logger.info(f"Circuit breaker reset for {model_name}")
                    return False

            return True

    def record_success(self, model_name: str) -> None:
        """
        Record a successful execution.

        Args:
            model_name: The model that succeeded
        """
        with self._lock:
            if model_name in self._state:
                self._state[model_name] = CircuitBreakerState()

    def record_failure(self, model_name: str) -> bool:
        """
        Record a failed execution.

        Args:
            model_name: The model that failed

        Returns:
            True if circuit is now open
        """
        with self._lock:
            if model_name not in self._state:
                self._state[model_name] = CircuitBreakerState()

            state = self._state[model_name]
            state.failure_count += 1
            state.last_failure_time = datetime.utcnow()

            if state.failure_count >= self.failure_threshold:
                state.is_open = True
                logger.warning(
                    f"Circuit breaker opened for {model_name} "
                    f"after {state.failure_count} failures"
                )
                return True

            return False

    def get_state(self, model_name: str) -> Optional[CircuitBreakerState]:
        """Get the circuit state for a model."""
        with self._lock:
            return self._state.get(model_name)

    def reset(self, model_name: Optional[str] = None) -> None:
        """
        Reset circuit breaker state.

        Args:
            model_name: Model to reset (all if None)
        """
        with self._lock:
            if model_name:
                if model_name in self._state:
                    del self._state[model_name]
            else:
                self._state.clear()


class AllowlistManager:
    """
    Manages the model allowlist with hot-reload support.

    The allowlist controls which models participate in direct execution.
    Models not on the allowlist bypass direct execution entirely.
    """

    def __init__(
        self,
        storage_service: Optional[ValidationStorageService] = None,
        use_db: bool = True,
        default_patterns: Optional[list[str]] = None,
    ) -> None:
        """
        Initialize the allowlist manager.

        Args:
            storage_service: Storage service for DB-backed allowlist
            use_db: Whether to use database for allowlist
            default_patterns: Default patterns if no DB entries
        """
        self._storage_service = storage_service
        self._use_db = use_db
        self._default_patterns = default_patterns or []
        self._cache: dict[str, bool] = {}
        self._cache_time: Optional[datetime] = None
        self._cache_ttl_seconds = 60.0  # Cache TTL for hot-reload
        self._lock = Lock()

    @property
    def storage_service(self) -> ValidationStorageService:
        """Get the storage service."""
        if self._storage_service is None:
            self._storage_service = get_validation_storage_service(
                persist_to_db=self._use_db
            )
        return self._storage_service

    def is_allowed(self, model_name: str) -> bool:
        """
        Check if a model is allowed for direct execution.

        Args:
            model_name: The model to check

        Returns:
            True if model is allowed for direct execution
        """
        # Check cache first
        with self._lock:
            if self._is_cache_valid() and model_name in self._cache:
                return self._cache[model_name]

        # Check storage service
        if self._use_db:
            result = self.storage_service.check_model_allowlist(model_name)
        else:
            result = self._check_default_patterns(model_name)

        # Update cache
        with self._lock:
            self._cache[model_name] = result
            self._cache_time = datetime.utcnow()

        return result

    def _is_cache_valid(self) -> bool:
        """Check if cache is still valid."""
        if self._cache_time is None:
            return False

        elapsed = (datetime.utcnow() - self._cache_time).total_seconds()
        return elapsed < self._cache_ttl_seconds

    def _check_default_patterns(self, model_name: str) -> bool:
        """Check model against default patterns."""
        import fnmatch

        if not self._default_patterns:
            return True  # Allow all if no patterns specified

        for pattern in self._default_patterns:
            if fnmatch.fnmatch(model_name, pattern):
                return True

        return False

    def invalidate_cache(self) -> None:
        """Invalidate the cache to force reload."""
        with self._lock:
            self._cache.clear()
            self._cache_time = None

    def add_pattern(
        self,
        pattern: str,
        notes: str = "",
        added_by: str = "",
    ) -> bool:
        """
        Add a pattern to the allowlist.

        Args:
            pattern: Model name or wildcard pattern
            notes: Optional notes
            added_by: Who added the entry

        Returns:
            True if added successfully
        """
        if self._use_db:
            result = self.storage_service.add_to_allowlist(
                model_name=pattern,
                notes=notes,
                added_by=added_by,
            )
        else:
            self._default_patterns.append(pattern)
            result = True

        self.invalidate_cache()
        return result


class FallbackHandler:
    """
    Handles graceful degradation from direct to legacy execution.

    Provides automatic fallback with:
    - Circuit breaker pattern for repeated failures
    - Allowlist-based execution control
    - Comprehensive logging and metrics
    """

    def __init__(
        self,
        feature_flags: Optional[FeatureFlags] = None,
        storage_service: Optional[ValidationStorageService] = None,
        circuit_breaker: Optional[CircuitBreaker] = None,
        allowlist_manager: Optional[AllowlistManager] = None,
    ) -> None:
        """
        Initialize the fallback handler.

        Args:
            feature_flags: Feature flags configuration
            storage_service: Storage service for logging fallback events
            circuit_breaker: Circuit breaker instance
            allowlist_manager: Allowlist manager instance
        """
        self._feature_flags = feature_flags
        self._storage_service = storage_service
        self._circuit_breaker = circuit_breaker or CircuitBreaker()
        self._allowlist_manager = allowlist_manager

    @property
    def feature_flags(self) -> FeatureFlags:
        """Get feature flags."""
        if self._feature_flags is None:
            self._feature_flags = get_feature_flags()
        return self._feature_flags

    @property
    def execution_mode(self) -> ExecutionMode:
        """Get the current execution mode."""
        return self.feature_flags.get_execution_mode()

    @property
    def storage_service(self) -> ValidationStorageService:
        """Get storage service."""
        if self._storage_service is None:
            self._storage_service = get_validation_storage_service(
                persist_to_db=False
            )
        return self._storage_service

    @property
    def allowlist_manager(self) -> AllowlistManager:
        """Get allowlist manager."""
        if self._allowlist_manager is None:
            self._allowlist_manager = AllowlistManager(
                storage_service=self._storage_service,
                use_db=False,
            )
        return self._allowlist_manager

    @property
    def circuit_breaker(self) -> CircuitBreaker:
        """Get circuit breaker."""
        return self._circuit_breaker

    def should_use_direct_execution(self, model_name: str) -> tuple[bool, Optional[FallbackReason]]:
        """
        Determine if direct execution should be attempted.

        Args:
            model_name: The model to check

        Returns:
            Tuple of (should_use_direct, reason_if_not)
        """
        # Check feature flag
        mode = self.execution_mode
        if mode == ExecutionMode.LEGACY:
            return False, FallbackReason.FEATURE_DISABLED

        # Check circuit breaker
        if self._circuit_breaker.is_open(model_name):
            return False, FallbackReason.CIRCUIT_BREAKER_OPEN

        # Check allowlist
        if not self.allowlist_manager.is_allowed(model_name):
            return False, FallbackReason.NOT_ON_ALLOWLIST

        return True, None

    def execute_with_fallback(
        self,
        model_name: str,
        direct_fn: Callable[[], T],
        legacy_fn: Callable[[], T],
        execution_id: str = "",
        validate_result: Optional[Callable[[T, T], bool]] = None,
    ) -> FallbackResult[T]:
        """
        Execute with automatic fallback on failure.

        Args:
            model_name: Name of the model being executed
            direct_fn: Function for direct execution
            legacy_fn: Function for legacy execution
            execution_id: Optional execution ID for logging
            validate_result: Optional function to validate direct result

        Returns:
            FallbackResult with the execution result
        """
        # Check if we should even try direct execution
        should_direct, reason = self.should_use_direct_execution(model_name)

        if not should_direct:
            # Skip direct execution entirely
            start = time.perf_counter()
            result = legacy_fn()
            elapsed_ms = (time.perf_counter() - start) * 1000

            return FallbackResult(
                value=result,
                used_fallback=True,
                fallback_reason=reason,
                fallback_execution_ms=elapsed_ms,
            )

        # Try direct execution
        direct_start = time.perf_counter()
        direct_result: Optional[T] = None
        direct_error: Optional[str] = None
        direct_error_type: Optional[str] = None

        try:
            direct_result = direct_fn()
            direct_elapsed_ms = (time.perf_counter() - direct_start) * 1000

            # Optional validation
            if validate_result is not None:
                # Run legacy for comparison
                legacy_result = legacy_fn()
                if not validate_result(direct_result, legacy_result):
                    # Validation failed - use legacy result
                    self._record_fallback(
                        model_name=model_name,
                        reason=FallbackReason.VALIDATION_MISMATCH,
                        execution_id=execution_id,
                        direct_execution_ms=direct_elapsed_ms,
                    )
                    return FallbackResult(
                        value=legacy_result,
                        used_fallback=True,
                        fallback_reason=FallbackReason.VALIDATION_MISMATCH,
                        direct_execution_ms=direct_elapsed_ms,
                    )

            # Success
            self._circuit_breaker.record_success(model_name)
            return FallbackResult(
                value=direct_result,
                used_fallback=False,
                direct_execution_ms=direct_elapsed_ms,
            )

        except Exception as e:
            direct_elapsed_ms = (time.perf_counter() - direct_start) * 1000
            direct_error = str(e)
            direct_error_type = type(e).__name__

            logger.warning(
                f"Direct execution failed for {model_name}: {direct_error}"
            )

        # Direct execution failed - try fallback
        self._circuit_breaker.record_failure(model_name)

        fallback_start = time.perf_counter()
        try:
            fallback_result = legacy_fn()
            fallback_elapsed_ms = (time.perf_counter() - fallback_start) * 1000

            # Record successful fallback
            self._record_fallback(
                model_name=model_name,
                reason=FallbackReason.DIRECT_EXECUTION_ERROR,
                execution_id=execution_id,
                error=direct_error,
                error_type=direct_error_type,
                error_traceback=traceback.format_exc(),
                fallback_succeeded=True,
                direct_execution_ms=direct_elapsed_ms,
                fallback_execution_ms=fallback_elapsed_ms,
            )

            return FallbackResult(
                value=fallback_result,
                used_fallback=True,
                fallback_reason=FallbackReason.DIRECT_EXECUTION_ERROR,
                direct_execution_ms=direct_elapsed_ms,
                fallback_execution_ms=fallback_elapsed_ms,
                error=direct_error,
                error_type=direct_error_type,
            )

        except Exception as fallback_error:
            fallback_elapsed_ms = (time.perf_counter() - fallback_start) * 1000

            # Both paths failed
            self._record_fallback(
                model_name=model_name,
                reason=FallbackReason.DIRECT_EXECUTION_ERROR,
                execution_id=execution_id,
                error=f"Direct: {direct_error}; Fallback: {fallback_error}",
                error_type=direct_error_type,
                fallback_succeeded=False,
                direct_execution_ms=direct_elapsed_ms,
                fallback_execution_ms=fallback_elapsed_ms,
            )

            # Re-raise the original error
            raise

    def _record_fallback(
        self,
        model_name: str,
        reason: FallbackReason,
        execution_id: str = "",
        error: Optional[str] = None,
        error_type: Optional[str] = None,
        error_traceback: str = "",
        fallback_succeeded: bool = True,
        direct_execution_ms: Optional[float] = None,
        fallback_execution_ms: Optional[float] = None,
    ) -> None:
        """Record a fallback event."""
        self.storage_service.record_fallback_event(
            model_name=model_name,
            failure_reason=error or reason.value,
            execution_id=execution_id,
            error_type=error_type or "",
            error_traceback=error_traceback,
            fallback_succeeded=fallback_succeeded,
            direct_execution_ms=direct_execution_ms,
            fallback_execution_ms=fallback_execution_ms,
        )

    @contextmanager
    def fallback_context(
        self,
        model_name: str,
        execution_id: str = "",
    ):
        """
        Context manager for fallback-aware execution.

        Yields a function that wraps execution with fallback.

        Usage:
            with handler.fallback_context("model") as execute:
                result = execute(direct_fn, legacy_fn)
        """
        def execute(
            direct_fn: Callable[[], T],
            legacy_fn: Callable[[], T],
        ) -> FallbackResult[T]:
            return self.execute_with_fallback(
                model_name=model_name,
                direct_fn=direct_fn,
                legacy_fn=legacy_fn,
                execution_id=execution_id,
            )

        yield execute


@dataclass
class ValidationMetrics:
    """
    Metrics for monitoring validation and fallback.

    Attributes:
        total_executions: Total execution count
        direct_success_count: Successful direct executions
        fallback_count: Number of fallback events
        direct_success_rate: Rate of direct execution success
        avg_direct_latency_ms: Average direct execution latency
        avg_fallback_latency_ms: Average fallback latency
        models_with_open_circuit: Models with open circuit breaker
    """

    total_executions: int = 0
    direct_success_count: int = 0
    fallback_count: int = 0
    direct_success_rate: float = 0.0
    avg_direct_latency_ms: float = 0.0
    avg_fallback_latency_ms: float = 0.0
    models_with_open_circuit: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_executions": self.total_executions,
            "direct_success_count": self.direct_success_count,
            "fallback_count": self.fallback_count,
            "direct_success_rate": self.direct_success_rate,
            "avg_direct_latency_ms": self.avg_direct_latency_ms,
            "avg_fallback_latency_ms": self.avg_fallback_latency_ms,
            "models_with_open_circuit": self.models_with_open_circuit,
        }


class MetricsCollector:
    """
    Collects and exposes metrics for monitoring dashboard.

    Tracks execution success rates, latencies, and fallback events.
    """

    def __init__(self) -> None:
        """Initialize the metrics collector."""
        self._direct_latencies: list[float] = []
        self._fallback_latencies: list[float] = []
        self._direct_successes = 0
        self._fallback_events = 0
        self._total_executions = 0
        self._lock = Lock()

    def record_direct_success(self, latency_ms: float) -> None:
        """Record a successful direct execution."""
        with self._lock:
            self._direct_successes += 1
            self._total_executions += 1
            self._direct_latencies.append(latency_ms)
            # Keep only last 1000 samples
            if len(self._direct_latencies) > 1000:
                self._direct_latencies = self._direct_latencies[-1000:]

    def record_fallback(self, latency_ms: float) -> None:
        """Record a fallback event."""
        with self._lock:
            self._fallback_events += 1
            self._total_executions += 1
            self._fallback_latencies.append(latency_ms)
            if len(self._fallback_latencies) > 1000:
                self._fallback_latencies = self._fallback_latencies[-1000:]

    def get_metrics(
        self,
        circuit_breaker: Optional[CircuitBreaker] = None,
    ) -> ValidationMetrics:
        """
        Get current metrics.

        Args:
            circuit_breaker: Optional circuit breaker to check for open circuits

        Returns:
            ValidationMetrics instance
        """
        with self._lock:
            total = self._total_executions
            direct_success = self._direct_successes
            fallback = self._fallback_events

            avg_direct = (
                sum(self._direct_latencies) / len(self._direct_latencies)
                if self._direct_latencies else 0.0
            )
            avg_fallback = (
                sum(self._fallback_latencies) / len(self._fallback_latencies)
                if self._fallback_latencies else 0.0
            )

        # Check for open circuits
        open_circuits = []
        if circuit_breaker:
            with circuit_breaker._lock:
                for model_name, state in circuit_breaker._state.items():
                    if state.is_open:
                        open_circuits.append(model_name)

        return ValidationMetrics(
            total_executions=total,
            direct_success_count=direct_success,
            fallback_count=fallback,
            direct_success_rate=direct_success / total if total > 0 else 1.0,
            avg_direct_latency_ms=avg_direct,
            avg_fallback_latency_ms=avg_fallback,
            models_with_open_circuit=open_circuits,
        )

    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self._direct_latencies.clear()
            self._fallback_latencies.clear()
            self._direct_successes = 0
            self._fallback_events = 0
            self._total_executions = 0


# Global instances
_fallback_handler: Optional[FallbackHandler] = None
_metrics_collector: Optional[MetricsCollector] = None


def get_fallback_handler() -> FallbackHandler:
    """Get the global fallback handler."""
    global _fallback_handler

    if _fallback_handler is None:
        _fallback_handler = FallbackHandler()

    return _fallback_handler


def get_metrics_collector() -> MetricsCollector:
    """Get the global metrics collector."""
    global _metrics_collector

    if _metrics_collector is None:
        _metrics_collector = MetricsCollector()

    return _metrics_collector


def reset_fallback_handler() -> None:
    """Reset the global fallback handler (for testing)."""
    global _fallback_handler
    _fallback_handler = None


def reset_metrics_collector() -> None:
    """Reset the global metrics collector (for testing)."""
    global _metrics_collector
    _metrics_collector = None
