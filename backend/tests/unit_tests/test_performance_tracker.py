"""Unit tests for Performance Tracker and Expression Cache."""

import json
import os
import tempfile
import time
from unittest.mock import MagicMock, patch

import pytest

from backend.application.config_parser.performance_tracker import (
    ExecutionPhase,
    ExecutionMode,
    PhaseMetrics,
    CacheMetrics,
    BenchmarkResult,
    ComparisonResult,
    PerformanceTracker,
    ExpressionCache,
    get_tracker,
    get_expression_cache,
    benchmark_execution,
    compare_execution_paths,
)


class TestExecutionPhase:
    """Tests for ExecutionPhase enum."""

    def test_values(self):
        """Test enum values."""
        assert ExecutionPhase.YAML_PARSING.value == "yaml_parsing"
        assert ExecutionPhase.DAG_CONSTRUCTION.value == "dag_construction"
        assert ExecutionPhase.SQL_COMPILATION.value == "sql_compilation"
        assert ExecutionPhase.MATERIALIZATION.value == "materialization"
        assert ExecutionPhase.TOTAL_EXECUTION.value == "total_execution"


class TestExecutionMode:
    """Tests for ExecutionMode enum."""

    def test_values(self):
        """Test enum values."""
        assert ExecutionMode.LEGACY.value == "legacy"
        assert ExecutionMode.DIRECT.value == "direct"
        assert ExecutionMode.PARALLEL.value == "parallel"


class TestPhaseMetrics:
    """Tests for PhaseMetrics dataclass."""

    def test_creation(self):
        """Test creating metrics."""
        metrics = PhaseMetrics(
            phase=ExecutionPhase.YAML_PARSING,
            duration_ms=100.5,
            model_count=10,
        )

        assert metrics.phase == ExecutionPhase.YAML_PARSING
        assert metrics.duration_ms == 100.5
        assert metrics.model_count == 10

    def test_to_dict(self):
        """Test conversion to dictionary."""
        metrics = PhaseMetrics(
            phase=ExecutionPhase.SQL_COMPILATION,
            duration_ms=50.0,
        )

        d = metrics.to_dict()

        assert d["phase"] == "sql_compilation"
        assert d["duration_ms"] == 50.0


class TestCacheMetrics:
    """Tests for CacheMetrics dataclass."""

    def test_creation(self):
        """Test creating metrics."""
        metrics = CacheMetrics(
            hits=80,
            misses=20,
            invalidations=5,
            entries=100,
        )

        assert metrics.hits == 80
        assert metrics.misses == 20
        assert metrics.hit_rate == 0.8

    def test_hit_rate_zero_accesses(self):
        """Test hit rate with zero accesses."""
        metrics = CacheMetrics()
        assert metrics.hit_rate == 0.0

    def test_to_dict(self):
        """Test conversion to dictionary."""
        metrics = CacheMetrics(hits=10, misses=10)

        d = metrics.to_dict()

        assert d["hits"] == 10
        assert d["misses"] == 10
        assert d["hit_rate"] == 0.5


class TestBenchmarkResult:
    """Tests for BenchmarkResult dataclass."""

    def test_creation(self):
        """Test creating result."""
        result = BenchmarkResult(
            execution_id="test-123",
            execution_mode=ExecutionMode.DIRECT,
            model_count=50,
            total_duration_ms=1000.0,
        )

        assert result.execution_id == "test-123"
        assert result.execution_mode == ExecutionMode.DIRECT
        assert result.model_count == 50
        assert result.success is True

    def test_to_dict(self):
        """Test conversion to dictionary."""
        result = BenchmarkResult(
            execution_id="test-456",
            execution_mode=ExecutionMode.LEGACY,
            model_count=10,
        )

        d = result.to_dict()

        assert d["execution_id"] == "test-456"
        assert d["execution_mode"] == "legacy"
        assert "timestamp" in d

    def test_to_json(self):
        """Test JSON serialization."""
        result = BenchmarkResult(
            execution_id="test-789",
            execution_mode=ExecutionMode.DIRECT,
        )

        json_str = result.to_json()
        parsed = json.loads(json_str)

        assert parsed["execution_id"] == "test-789"


class TestComparisonResult:
    """Tests for ComparisonResult dataclass."""

    def test_speedup_calculation(self):
        """Test speedup factor calculation."""
        legacy = BenchmarkResult(
            execution_id="legacy-1",
            execution_mode=ExecutionMode.LEGACY,
            total_duration_ms=1000.0,
        )
        direct = BenchmarkResult(
            execution_id="direct-1",
            execution_mode=ExecutionMode.DIRECT,
            total_duration_ms=100.0,
        )

        comparison = ComparisonResult(
            legacy_result=legacy,
            direct_result=direct,
        )

        assert comparison.speedup_factor == 10.0

    def test_to_dict(self):
        """Test conversion to dictionary."""
        legacy = BenchmarkResult(
            execution_id="l1",
            execution_mode=ExecutionMode.LEGACY,
            total_duration_ms=200.0,
        )
        direct = BenchmarkResult(
            execution_id="d1",
            execution_mode=ExecutionMode.DIRECT,
            total_duration_ms=100.0,
        )

        comparison = ComparisonResult(
            legacy_result=legacy,
            direct_result=direct,
        )

        d = comparison.to_dict()

        assert d["speedup_factor"] == 2.0
        assert d["direct_is_faster"] is True


class TestPerformanceTracker:
    """Tests for PerformanceTracker."""

    def test_init(self):
        """Test tracker initialization."""
        tracker = PerformanceTracker(execution_mode=ExecutionMode.DIRECT)

        assert tracker.execution_mode == ExecutionMode.DIRECT
        assert tracker.execution_id is not None

    def test_time_phase_context_manager(self):
        """Test timing a phase with context manager."""
        tracker = PerformanceTracker()

        with tracker.time_phase(ExecutionPhase.YAML_PARSING, model_count=5):
            time.sleep(0.01)  # 10ms

        result = tracker.get_result()
        phase_metrics = result.phases.get(ExecutionPhase.YAML_PARSING)

        assert phase_metrics is not None
        assert phase_metrics.duration_ms >= 10.0
        assert phase_metrics.model_count == 5

    def test_start_stop_phase(self):
        """Test manual phase timing."""
        tracker = PerformanceTracker()

        tracker.start_phase(ExecutionPhase.DAG_CONSTRUCTION, model_count=10)
        time.sleep(0.01)
        metrics = tracker.stop_phase(ExecutionPhase.DAG_CONSTRUCTION)

        assert metrics.duration_ms >= 10.0
        assert metrics.model_count == 10

    def test_stop_unstarted_phase_raises_error(self):
        """Test stopping an unstarted phase raises error."""
        tracker = PerformanceTracker()

        with pytest.raises(ValueError):
            tracker.stop_phase(ExecutionPhase.SQL_COMPILATION)

    def test_overall_timing(self):
        """Test overall execution timing."""
        tracker = PerformanceTracker()

        tracker.start()
        time.sleep(0.01)
        tracker.stop()

        result = tracker.get_result()
        assert result.total_duration_ms >= 10.0
        assert result.success is True

    def test_stop_with_error(self):
        """Test stopping with error."""
        tracker = PerformanceTracker()

        tracker.start()
        tracker.stop(success=False, error="Test error")

        result = tracker.get_result()
        assert result.success is False
        assert result.error == "Test error"

    def test_set_cache_metrics(self):
        """Test setting cache metrics."""
        tracker = PerformanceTracker()
        cache_metrics = CacheMetrics(hits=100, misses=20)

        tracker.set_cache_metrics(cache_metrics)

        result = tracker.get_result()
        assert result.cache_metrics is not None
        assert result.cache_metrics.hits == 100

    def test_multiple_phases(self):
        """Test tracking multiple phases."""
        tracker = PerformanceTracker()

        with tracker.time_phase(ExecutionPhase.YAML_PARSING):
            time.sleep(0.005)

        with tracker.time_phase(ExecutionPhase.DAG_CONSTRUCTION):
            time.sleep(0.005)

        result = tracker.get_result()
        assert len(result.phases) == 2
        assert ExecutionPhase.YAML_PARSING in result.phases
        assert ExecutionPhase.DAG_CONSTRUCTION in result.phases


class TestExpressionCache:
    """Tests for ExpressionCache."""

    def test_init(self):
        """Test cache initialization."""
        cache = ExpressionCache(max_entries=100)
        assert cache.max_entries == 100

    def test_set_and_get(self):
        """Test basic set and get."""
        cache = ExpressionCache()

        cache.set("key1", "value1")
        result = cache.get("key1")

        assert result == "value1"

    def test_cache_miss(self):
        """Test cache miss."""
        cache = ExpressionCache()

        result = cache.get("nonexistent")

        assert result is None

    def test_cache_metrics_hit_miss(self):
        """Test cache metrics track hits and misses."""
        cache = ExpressionCache()

        cache.set("key1", "value1")
        cache.get("key1")  # Hit
        cache.get("key2")  # Miss

        metrics = cache.get_metrics()
        assert metrics.hits == 1
        assert metrics.misses == 1

    def test_invalidate(self):
        """Test invalidating a cache entry."""
        cache = ExpressionCache()

        cache.set("key1", "value1")
        result = cache.invalidate("key1")

        assert result is True
        assert cache.get("key1") is None

    def test_invalidate_nonexistent(self):
        """Test invalidating nonexistent entry."""
        cache = ExpressionCache()

        result = cache.invalidate("nonexistent")

        assert result is False

    def test_clear(self):
        """Test clearing the cache."""
        cache = ExpressionCache()

        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.clear()

        assert cache.get("key1") is None
        assert cache.get("key2") is None
        assert cache.get_metrics().entries == 0

    def test_file_based_invalidation(self):
        """Test cache invalidation when file changes."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("test: content")
            temp_path = f.name

        try:
            cache = ExpressionCache()

            # Set entry with file tracking
            cache.set("key1", "value1", source_file=temp_path)

            # Should be valid initially
            assert cache.get("key1", source_file=temp_path) == "value1"

            # Modify the file
            time.sleep(0.1)
            with open(temp_path, 'w') as f:
                f.write("modified: content")

            # Should be invalid now
            assert cache.get("key1", source_file=temp_path) is None

        finally:
            os.unlink(temp_path)

    def test_invalidate_by_file(self):
        """Test invalidating all entries for a file."""
        cache = ExpressionCache()

        cache.set("key1", "value1", source_file="/path/to/file.yaml")
        cache.set("key2", "value2", source_file="/path/to/file.yaml")
        cache.set("key3", "value3", source_file="/other/file.yaml")

        count = cache.invalidate_by_file("/path/to/file.yaml")

        assert count == 2
        assert cache.get("key1") is None
        assert cache.get("key2") is None
        assert cache.get("key3") == "value3"

    def test_max_entries_eviction(self):
        """Test eviction when max entries reached."""
        cache = ExpressionCache(max_entries=3)

        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.set("key3", "value3")
        cache.set("key4", "value4")  # Should evict key1

        metrics = cache.get_metrics()
        assert metrics.entries == 3

    def test_validate_all(self):
        """Test validating all cache entries."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("test: content")
            temp_path = f.name

        try:
            cache = ExpressionCache()

            cache.set("key1", "value1", source_file=temp_path)

            # Modify file
            time.sleep(0.1)
            with open(temp_path, 'w') as f:
                f.write("modified: content")

            # Validate all
            invalid_count = cache.validate_all()

            assert invalid_count == 1
            assert cache.get("key1") is None

        finally:
            os.unlink(temp_path)


class TestConvenienceFunctions:
    """Tests for module-level convenience functions."""

    def test_get_tracker(self):
        """Test getting global tracker."""
        tracker = get_tracker(reset=True)
        assert tracker is not None
        assert isinstance(tracker, PerformanceTracker)

    def test_get_expression_cache(self):
        """Test getting global cache."""
        cache = get_expression_cache()
        assert cache is not None
        assert isinstance(cache, ExpressionCache)

    def test_benchmark_execution_success(self):
        """Test benchmarking a successful execution."""
        def mock_fn():
            time.sleep(0.01)

        result = benchmark_execution(mock_fn, ExecutionMode.DIRECT, model_count=10)

        assert result.success is True
        assert result.total_duration_ms >= 10.0
        assert result.execution_mode == ExecutionMode.DIRECT

    def test_benchmark_execution_failure(self):
        """Test benchmarking a failed execution."""
        def failing_fn():
            raise ValueError("Test error")

        result = benchmark_execution(failing_fn)

        assert result.success is False
        assert "Test error" in result.error

    def test_compare_execution_paths(self):
        """Test comparing legacy and direct paths."""
        def slow_legacy():
            time.sleep(0.02)

        def fast_direct():
            time.sleep(0.01)

        comparison = compare_execution_paths(
            slow_legacy,
            fast_direct,
            model_count=10,
        )

        assert comparison.legacy_result.execution_mode == ExecutionMode.LEGACY
        assert comparison.direct_result.execution_mode == ExecutionMode.DIRECT
        assert comparison.speedup_factor > 1.0  # Direct should be faster


class TestThreadSafety:
    """Tests for thread safety."""

    def test_tracker_concurrent_phases(self):
        """Test concurrent phase timing."""
        import threading

        tracker = PerformanceTracker()
        results = []

        def time_phase(phase):
            with tracker.time_phase(phase):
                time.sleep(0.01)
            results.append(phase)

        threads = [
            threading.Thread(target=time_phase, args=(ExecutionPhase.YAML_PARSING,)),
            threading.Thread(target=time_phase, args=(ExecutionPhase.DAG_CONSTRUCTION,)),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 2
        result = tracker.get_result()
        assert len(result.phases) == 2

    def test_cache_concurrent_access(self):
        """Test concurrent cache access."""
        import threading

        cache = ExpressionCache()
        errors = []

        def writer():
            try:
                for i in range(100):
                    cache.set(f"key_{threading.current_thread().name}_{i}", f"value_{i}")
            except Exception as e:
                errors.append(e)

        def reader():
            try:
                for i in range(100):
                    cache.get(f"key_{threading.current_thread().name}_{i}")
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=writer, name="w1"),
            threading.Thread(target=writer, name="w2"),
            threading.Thread(target=reader, name="r1"),
            threading.Thread(target=reader, name="r2"),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
