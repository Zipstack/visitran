"""
Performance Tracker for Execution Benchmarking.

This module provides performance instrumentation for measuring execution speed
across all phases of the transformation pipeline.

Usage:
    tracker = PerformanceTracker()

    with tracker.time_phase("yaml_parsing"):
        # Parse YAML files
        pass

    metrics = tracker.get_metrics()
    print(metrics.to_json())
"""

from __future__ import annotations

import json
import logging
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Generator, Optional

logger = logging.getLogger(__name__)


class ExecutionPhase(Enum):
    """Phases of the execution pipeline."""

    YAML_PARSING = "yaml_parsing"
    DAG_CONSTRUCTION = "dag_construction"
    SQL_COMPILATION = "sql_compilation"
    MATERIALIZATION = "materialization"
    TOTAL_EXECUTION = "total_execution"


class ExecutionMode(Enum):
    """Execution mode being tracked."""

    LEGACY = "legacy"
    DIRECT = "direct"
    PARALLEL = "parallel"


@dataclass
class PhaseMetrics:
    """
    Metrics for a single execution phase.

    Attributes:
        phase: The execution phase
        duration_ms: Duration in milliseconds
        start_time: When the phase started
        end_time: When the phase ended
        model_count: Number of models processed in this phase
        additional_info: Extra phase-specific metadata
    """

    phase: ExecutionPhase
    duration_ms: float = 0.0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    model_count: int = 0
    additional_info: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "phase": self.phase.value,
            "duration_ms": self.duration_ms,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "model_count": self.model_count,
            "additional_info": self.additional_info,
        }


@dataclass
class CacheMetrics:
    """
    Metrics for expression cache performance.

    Attributes:
        hits: Number of cache hits
        misses: Number of cache misses
        invalidations: Number of cache invalidations
        entries: Current number of cache entries
        memory_estimate_bytes: Estimated memory usage
    """

    hits: int = 0
    misses: int = 0
    invalidations: int = 0
    entries: int = 0
    memory_estimate_bytes: int = 0

    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate."""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "hits": self.hits,
            "misses": self.misses,
            "invalidations": self.invalidations,
            "entries": self.entries,
            "hit_rate": self.hit_rate,
            "memory_estimate_bytes": self.memory_estimate_bytes,
        }


@dataclass
class BenchmarkResult:
    """
    Complete benchmark results for an execution run.

    Attributes:
        execution_id: Unique identifier for this benchmark run
        execution_mode: Legacy, direct, or parallel
        timestamp: When the benchmark started
        model_count: Total number of models processed
        phases: Per-phase timing metrics
        cache_metrics: Cache performance metrics
        total_duration_ms: Total execution time
        success: Whether execution completed successfully
        error: Error message if failed
    """

    execution_id: str
    execution_mode: ExecutionMode
    timestamp: datetime = field(default_factory=datetime.utcnow)
    model_count: int = 0
    phases: dict[ExecutionPhase, PhaseMetrics] = field(default_factory=dict)
    cache_metrics: Optional[CacheMetrics] = None
    total_duration_ms: float = 0.0
    success: bool = True
    error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "execution_id": self.execution_id,
            "execution_mode": self.execution_mode.value,
            "timestamp": self.timestamp.isoformat(),
            "model_count": self.model_count,
            "phases": {
                phase.value: metrics.to_dict()
                for phase, metrics in self.phases.items()
            },
            "cache_metrics": self.cache_metrics.to_dict() if self.cache_metrics else None,
            "total_duration_ms": self.total_duration_ms,
            "success": self.success,
            "error": self.error,
        }

    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)


@dataclass
class ComparisonResult:
    """
    Result comparing legacy vs direct execution performance.

    Attributes:
        legacy_result: Benchmark result for legacy path
        direct_result: Benchmark result for direct path
        speedup_factor: Direct path speedup (>1 means direct is faster)
        phase_comparisons: Per-phase speedup factors
    """

    legacy_result: BenchmarkResult
    direct_result: BenchmarkResult
    speedup_factor: float = 0.0
    phase_comparisons: dict[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Calculate speedup factors."""
        if self.legacy_result.total_duration_ms > 0:
            self.speedup_factor = (
                self.legacy_result.total_duration_ms /
                self.direct_result.total_duration_ms
            )

        # Calculate per-phase comparisons
        for phase in ExecutionPhase:
            legacy_phase = self.legacy_result.phases.get(phase)
            direct_phase = self.direct_result.phases.get(phase)

            if legacy_phase and direct_phase and legacy_phase.duration_ms > 0:
                self.phase_comparisons[phase.value] = (
                    legacy_phase.duration_ms / direct_phase.duration_ms
                )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "legacy_result": self.legacy_result.to_dict(),
            "direct_result": self.direct_result.to_dict(),
            "speedup_factor": self.speedup_factor,
            "phase_comparisons": self.phase_comparisons,
            "direct_is_faster": self.speedup_factor > 1.0,
        }

    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)


class PerformanceTracker:
    """
    Tracks performance metrics for execution phases.

    Thread-safe tracker that captures timing information for each
    phase of the transformation pipeline.

    Usage:
        tracker = PerformanceTracker(execution_mode=ExecutionMode.DIRECT)

        with tracker.time_phase(ExecutionPhase.YAML_PARSING, model_count=10):
            # Parse YAML files
            pass

        result = tracker.get_result()
        print(result.to_json())
    """

    def __init__(
        self,
        execution_mode: ExecutionMode = ExecutionMode.DIRECT,
        execution_id: Optional[str] = None,
    ) -> None:
        """
        Initialize the tracker.

        Args:
            execution_mode: The execution mode being tracked
            execution_id: Optional unique identifier (auto-generated if None)
        """
        self.execution_mode = execution_mode
        self.execution_id = execution_id or self._generate_id()

        self._lock = threading.RLock()
        self._phases: dict[ExecutionPhase, PhaseMetrics] = {}
        self._active_timers: dict[ExecutionPhase, float] = {}
        self._start_time: Optional[float] = None
        self._end_time: Optional[float] = None
        self._model_count = 0
        self._success = True
        self._error: Optional[str] = None
        self._cache_metrics: Optional[CacheMetrics] = None

    def _generate_id(self) -> str:
        """Generate a unique execution ID."""
        import uuid
        return str(uuid.uuid4())[:8]

    def start(self) -> None:
        """Start overall timing."""
        with self._lock:
            self._start_time = time.perf_counter()

    def stop(self, success: bool = True, error: Optional[str] = None) -> None:
        """
        Stop overall timing.

        Args:
            success: Whether execution succeeded
            error: Error message if failed
        """
        with self._lock:
            self._end_time = time.perf_counter()
            self._success = success
            self._error = error

    @contextmanager
    def time_phase(
        self,
        phase: ExecutionPhase,
        model_count: int = 0,
        **additional_info: Any,
    ) -> Generator[PhaseMetrics, None, None]:
        """
        Context manager for timing an execution phase.

        Args:
            phase: The phase being timed
            model_count: Number of models in this phase
            **additional_info: Extra metadata

        Yields:
            PhaseMetrics for the phase (populated after exit)
        """
        metrics = PhaseMetrics(
            phase=phase,
            model_count=model_count,
            start_time=datetime.utcnow(),
            additional_info=dict(additional_info),
        )

        start = time.perf_counter()

        with self._lock:
            self._active_timers[phase] = start

        try:
            yield metrics
        finally:
            end = time.perf_counter()
            duration_ms = (end - start) * 1000

            with self._lock:
                metrics.duration_ms = duration_ms
                metrics.end_time = datetime.utcnow()
                self._phases[phase] = metrics
                self._active_timers.pop(phase, None)

                if model_count > self._model_count:
                    self._model_count = model_count

    def start_phase(
        self,
        phase: ExecutionPhase,
        model_count: int = 0,
    ) -> None:
        """
        Start timing a phase (alternative to context manager).

        Args:
            phase: The phase to start timing
            model_count: Number of models
        """
        with self._lock:
            self._active_timers[phase] = time.perf_counter()
            self._phases[phase] = PhaseMetrics(
                phase=phase,
                model_count=model_count,
                start_time=datetime.utcnow(),
            )
            if model_count > self._model_count:
                self._model_count = model_count

    def stop_phase(
        self,
        phase: ExecutionPhase,
        **additional_info: Any,
    ) -> PhaseMetrics:
        """
        Stop timing a phase.

        Args:
            phase: The phase to stop timing
            **additional_info: Extra metadata

        Returns:
            PhaseMetrics for the phase
        """
        with self._lock:
            start = self._active_timers.pop(phase, None)
            if start is None:
                raise ValueError(f"Phase {phase} was not started")

            end = time.perf_counter()
            duration_ms = (end - start) * 1000

            metrics = self._phases.get(phase, PhaseMetrics(phase=phase))
            metrics.duration_ms = duration_ms
            metrics.end_time = datetime.utcnow()
            metrics.additional_info.update(additional_info)
            self._phases[phase] = metrics

            return metrics

    def set_cache_metrics(self, cache_metrics: CacheMetrics) -> None:
        """
        Set cache performance metrics.

        Args:
            cache_metrics: The cache metrics to include
        """
        with self._lock:
            self._cache_metrics = cache_metrics

    def get_result(self) -> BenchmarkResult:
        """
        Get the complete benchmark result.

        Returns:
            BenchmarkResult with all metrics
        """
        with self._lock:
            total_ms = 0.0
            if self._start_time is not None:
                end = self._end_time or time.perf_counter()
                total_ms = (end - self._start_time) * 1000

            return BenchmarkResult(
                execution_id=self.execution_id,
                execution_mode=self.execution_mode,
                model_count=self._model_count,
                phases=dict(self._phases),
                cache_metrics=self._cache_metrics,
                total_duration_ms=total_ms,
                success=self._success,
                error=self._error,
            )

    def log_metrics(self, level: str = "info") -> None:
        """
        Log the benchmark metrics.

        Args:
            level: Logging level (debug, info, warning, error)
        """
        result = self.get_result()
        log_func = getattr(logger, level, logger.info)

        log_func(
            f"Performance Metrics [id={result.execution_id}, "
            f"mode={result.execution_mode.value}]:\n"
            f"{result.to_json()}"
        )


class ExpressionCache:
    """
    Cache for compiled expressions with file-based invalidation.

    Thread-safe cache that stores compiled expressions and automatically
    invalidates entries when source files change.

    Usage:
        cache = ExpressionCache()

        # Try cache first
        result = cache.get("expression_key", "/path/to/source.yaml")
        if result is None:
            # Cache miss - compute and store
            result = compile_expression(...)
            cache.set("expression_key", result, "/path/to/source.yaml")

        # Get cache statistics
        metrics = cache.get_metrics()
    """

    def __init__(self, max_entries: int = 10000) -> None:
        """
        Initialize the cache.

        Args:
            max_entries: Maximum number of cache entries
        """
        self.max_entries = max_entries
        self._lock = threading.RLock()
        self._cache: dict[str, tuple[Any, Optional[str], Optional[float]]] = {}
        self._metrics = CacheMetrics()

    def _get_file_mtime(self, file_path: str) -> Optional[float]:
        """Get file modification time."""
        import os
        try:
            return os.path.getmtime(file_path)
        except OSError:
            return None

    def _is_valid(
        self,
        key: str,
        stored_path: Optional[str],
        stored_mtime: Optional[float],
    ) -> bool:
        """Check if cache entry is still valid."""
        if stored_path is None:
            return True

        current_mtime = self._get_file_mtime(stored_path)

        # If file never existed (stored_mtime is None), it's valid
        # as long as the file still doesn't exist
        if stored_mtime is None:
            return current_mtime is None

        # If file existed but now doesn't, invalidate
        if current_mtime is None:
            return False

        return stored_mtime == current_mtime

    def get(
        self,
        key: str,
        source_file: Optional[str] = None,
    ) -> Optional[Any]:
        """
        Get a cached value.

        Args:
            key: Cache key
            source_file: Optional source file for validation

        Returns:
            Cached value if found and valid, None otherwise
        """
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                self._metrics.misses += 1
                return None

            value, stored_path, stored_mtime = entry

            # Validate against source file if provided
            if source_file and stored_path != source_file:
                self._metrics.misses += 1
                return None

            # Check if still valid
            if not self._is_valid(key, stored_path, stored_mtime):
                self._invalidate_entry(key)
                self._metrics.misses += 1
                return None

            self._metrics.hits += 1
            return value

    def set(
        self,
        key: str,
        value: Any,
        source_file: Optional[str] = None,
    ) -> None:
        """
        Store a value in the cache.

        Args:
            key: Cache key
            value: Value to cache
            source_file: Optional source file for invalidation tracking
        """
        with self._lock:
            # Enforce max entries
            if len(self._cache) >= self.max_entries:
                self._evict_oldest()

            mtime = None
            if source_file:
                mtime = self._get_file_mtime(source_file)

            self._cache[key] = (value, source_file, mtime)
            self._metrics.entries = len(self._cache)

    def invalidate(self, key: str) -> bool:
        """
        Invalidate a specific cache entry.

        Args:
            key: Cache key to invalidate

        Returns:
            True if entry was found and removed
        """
        with self._lock:
            return self._invalidate_entry(key)

    def _invalidate_entry(self, key: str) -> bool:
        """Internal invalidation (assumes lock held)."""
        if key in self._cache:
            del self._cache[key]
            self._metrics.invalidations += 1
            self._metrics.entries = len(self._cache)
            return True
        return False

    def invalidate_by_file(self, file_path: str) -> int:
        """
        Invalidate all entries associated with a file.

        Args:
            file_path: File path to invalidate

        Returns:
            Number of entries invalidated
        """
        with self._lock:
            keys_to_remove = [
                key for key, (_, stored_path, _) in self._cache.items()
                if stored_path == file_path
            ]

            for key in keys_to_remove:
                self._invalidate_entry(key)

            return len(keys_to_remove)

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            self._metrics.invalidations += count
            self._metrics.entries = 0

    def _evict_oldest(self) -> None:
        """Evict oldest entry when cache is full."""
        if self._cache:
            oldest_key = next(iter(self._cache))
            self._invalidate_entry(oldest_key)

    def get_metrics(self) -> CacheMetrics:
        """
        Get cache performance metrics.

        Returns:
            CacheMetrics with current statistics
        """
        with self._lock:
            # Estimate memory usage (rough approximation)
            import sys
            total_size = 0
            for key, (value, path, _) in self._cache.items():
                total_size += sys.getsizeof(key)
                total_size += sys.getsizeof(value) if value else 0
                total_size += sys.getsizeof(path) if path else 0

            return CacheMetrics(
                hits=self._metrics.hits,
                misses=self._metrics.misses,
                invalidations=self._metrics.invalidations,
                entries=len(self._cache),
                memory_estimate_bytes=total_size,
            )

    def validate_all(self) -> int:
        """
        Validate all cache entries against their source files.

        Returns:
            Number of entries invalidated
        """
        with self._lock:
            invalid_keys = []

            for key, (_, stored_path, stored_mtime) in self._cache.items():
                if not self._is_valid(key, stored_path, stored_mtime):
                    invalid_keys.append(key)

            for key in invalid_keys:
                self._invalidate_entry(key)

            return len(invalid_keys)


# Global instances
_tracker: Optional[PerformanceTracker] = None
_expression_cache: Optional[ExpressionCache] = None


def get_tracker(
    execution_mode: ExecutionMode = ExecutionMode.DIRECT,
    reset: bool = False,
) -> PerformanceTracker:
    """
    Get the global performance tracker.

    Args:
        execution_mode: Execution mode to use
        reset: If True, create a new tracker

    Returns:
        PerformanceTracker instance
    """
    global _tracker
    if _tracker is None or reset:
        _tracker = PerformanceTracker(execution_mode=execution_mode)
    return _tracker


def get_expression_cache(max_entries: int = 10000) -> ExpressionCache:
    """
    Get the global expression cache.

    Args:
        max_entries: Maximum cache entries

    Returns:
        ExpressionCache instance
    """
    global _expression_cache
    if _expression_cache is None:
        _expression_cache = ExpressionCache(max_entries=max_entries)
    return _expression_cache


def benchmark_execution(
    execute_fn: callable,
    execution_mode: ExecutionMode = ExecutionMode.DIRECT,
    model_count: int = 0,
) -> BenchmarkResult:
    """
    Benchmark a single execution.

    Args:
        execute_fn: Function to execute and benchmark
        execution_mode: Mode being benchmarked
        model_count: Number of models

    Returns:
        BenchmarkResult with metrics
    """
    tracker = PerformanceTracker(execution_mode=execution_mode)
    tracker.start()

    try:
        with tracker.time_phase(ExecutionPhase.TOTAL_EXECUTION, model_count):
            execute_fn()
        tracker.stop(success=True)
    except Exception as e:
        tracker.stop(success=False, error=str(e))

    # Include cache metrics
    cache = get_expression_cache()
    tracker.set_cache_metrics(cache.get_metrics())

    return tracker.get_result()


def compare_execution_paths(
    legacy_fn: callable,
    direct_fn: callable,
    model_count: int = 0,
) -> ComparisonResult:
    """
    Compare performance between legacy and direct execution paths.

    Args:
        legacy_fn: Legacy execution function
        direct_fn: Direct execution function
        model_count: Number of models

    Returns:
        ComparisonResult with comparison metrics
    """
    # Benchmark legacy path
    legacy_result = benchmark_execution(
        legacy_fn,
        ExecutionMode.LEGACY,
        model_count,
    )

    # Clear cache between runs for fair comparison
    get_expression_cache().clear()

    # Benchmark direct path
    direct_result = benchmark_execution(
        direct_fn,
        ExecutionMode.DIRECT,
        model_count,
    )

    return ComparisonResult(
        legacy_result=legacy_result,
        direct_result=direct_result,
    )
