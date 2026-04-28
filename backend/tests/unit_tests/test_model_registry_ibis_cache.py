"""Unit tests for ModelRegistry Ibis Table caching."""

import os
import tempfile
import time
from unittest.mock import MagicMock, patch

import pytest

from backend.application.config_parser.model_registry import (
    ModelRegistry,
    IbisTableCache,
    ModelMetadata,
    ExecutionStatus,
)
from backend.application.config_parser.config_parser import ConfigParser


class TestIbisTableCache:
    """Tests for IbisTableCache dataclass."""

    def test_creation_minimal(self):
        """Test creating IbisTableCache with minimal fields."""
        mock_table = MagicMock()
        cache = IbisTableCache(ibis_table=mock_table)

        assert cache.ibis_table is mock_table
        assert cache.file_path is None
        assert cache.file_mtime is None

    def test_creation_full(self):
        """Test creating IbisTableCache with all fields."""
        mock_table = MagicMock()
        cache = IbisTableCache(
            ibis_table=mock_table,
            file_path="/path/to/file.yaml",
            file_mtime=1234567890.0,
            created_at=100.0,
        )

        assert cache.file_path == "/path/to/file.yaml"
        assert cache.file_mtime == 1234567890.0
        assert cache.created_at == 100.0

    def test_is_valid_no_file_tracking(self):
        """Test is_valid returns True when no file tracking."""
        mock_table = MagicMock()
        cache = IbisTableCache(ibis_table=mock_table)

        assert cache.is_valid() is True

    def test_is_valid_with_unchanged_file(self):
        """Test is_valid returns True when file unchanged."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("test: content")
            temp_path = f.name

        try:
            mtime = os.path.getmtime(temp_path)
            mock_table = MagicMock()
            cache = IbisTableCache(
                ibis_table=mock_table,
                file_path=temp_path,
                file_mtime=mtime,
            )

            assert cache.is_valid() is True
        finally:
            os.unlink(temp_path)

    def test_is_valid_with_modified_file(self):
        """Test is_valid returns False when file modified."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("test: content")
            temp_path = f.name

        try:
            old_mtime = os.path.getmtime(temp_path)
            mock_table = MagicMock()
            cache = IbisTableCache(
                ibis_table=mock_table,
                file_path=temp_path,
                file_mtime=old_mtime - 1.0,  # Older timestamp
            )

            assert cache.is_valid() is False
        finally:
            os.unlink(temp_path)

    def test_is_valid_with_missing_file(self):
        """Test is_valid returns False when file doesn't exist."""
        mock_table = MagicMock()
        cache = IbisTableCache(
            ibis_table=mock_table,
            file_path="/nonexistent/file.yaml",
            file_mtime=1234567890.0,
        )

        assert cache.is_valid() is False


class TestModelRegistryIbisCaching:
    """Tests for ModelRegistry Ibis caching methods."""

    def setup_method(self):
        """Reset singleton before each test."""
        ModelRegistry.reset_instance()
        ConfigParser._instances.clear()

    def _create_mock_config(self, schema, model):
        """Create a mock ConfigParser."""
        config = MagicMock(spec=ConfigParser)
        config.destination_schema_name = schema
        config.model_name = model
        config.materialization = "TABLE"
        return config

    def test_register_model_with_ibis_table(self):
        """Test registering model with ConfigParser and Ibis table."""
        registry = ModelRegistry()
        config = self._create_mock_config("p", "m")
        mock_table = MagicMock()

        registry.register_model("p", "m", config, mock_table)

        assert registry.contains("p", "m")
        assert registry.get_ibis_table("p", "m") is mock_table

    def test_register_model_duplicate_raises_error(self):
        """Test that duplicate registration raises ValueError."""
        registry = ModelRegistry()
        config = self._create_mock_config("p", "m")
        mock_table = MagicMock()

        registry.register_model("p", "m", config, mock_table)

        with pytest.raises(ValueError) as exc_info:
            registry.register_model("p", "m", config, mock_table)

        assert "already registered" in str(exc_info.value)

    def test_register_model_locked_raises_error(self):
        """Test that registration fails when registry is locked."""
        registry = ModelRegistry()
        registry.lock()

        config = self._create_mock_config("p", "m")
        mock_table = MagicMock()

        with pytest.raises(RuntimeError) as exc_info:
            registry.register_model("p", "m", config, mock_table)

        assert "locked" in str(exc_info.value).lower()

    def test_get_model_returns_tuple(self):
        """Test get_model returns (config, ibis_table) tuple."""
        registry = ModelRegistry()
        config = self._create_mock_config("p", "m")
        mock_table = MagicMock()

        registry.register_model("p", "m", config, mock_table)

        result_config, result_table = registry.get_model("p", "m")

        assert result_config is config
        assert result_table is mock_table

    def test_get_model_not_found_raises_error(self):
        """Test get_model raises KeyError for unknown model."""
        registry = ModelRegistry()

        with pytest.raises(KeyError):
            registry.get_model("p", "unknown")

    def test_cache_ibis_table_existing_model(self):
        """Test caching Ibis table for existing model."""
        registry = ModelRegistry()
        config = self._create_mock_config("p", "m")
        registry.register("p", "m", config)

        mock_table = MagicMock()
        registry.cache_ibis_table("p", "m", mock_table)

        assert registry.get_ibis_table("p", "m") is mock_table

    def test_cache_ibis_table_unregistered_raises_error(self):
        """Test caching fails for unregistered model."""
        registry = ModelRegistry()
        mock_table = MagicMock()

        with pytest.raises(KeyError):
            registry.cache_ibis_table("p", "unknown", mock_table)

    def test_cache_ibis_table_locked_raises_error(self):
        """Test caching fails when registry is locked."""
        registry = ModelRegistry()
        config = self._create_mock_config("p", "m")
        registry.register("p", "m", config)
        registry.lock()

        mock_table = MagicMock()

        with pytest.raises(RuntimeError):
            registry.cache_ibis_table("p", "m", mock_table)

    def test_get_ibis_table_not_cached(self):
        """Test get_ibis_table returns None for uncached model."""
        registry = ModelRegistry()
        config = self._create_mock_config("p", "m")
        registry.register("p", "m", config)

        assert registry.get_ibis_table("p", "m") is None

    def test_is_cache_valid_no_cache(self):
        """Test is_cache_valid returns False when no cache."""
        registry = ModelRegistry()

        assert registry.is_cache_valid("p", "m") is False

    def test_is_cache_valid_with_cache(self):
        """Test is_cache_valid returns True for valid cache."""
        registry = ModelRegistry()
        config = self._create_mock_config("p", "m")
        mock_table = MagicMock()

        registry.register_model("p", "m", config, mock_table)

        assert registry.is_cache_valid("p", "m") is True

    def test_invalidate_cache(self):
        """Test invalidating a specific cache entry."""
        registry = ModelRegistry()
        config = self._create_mock_config("p", "m")
        mock_table = MagicMock()

        registry.register_model("p", "m", config, mock_table)
        assert registry.get_ibis_table("p", "m") is not None

        result = registry.invalidate_cache("p", "m")

        assert result is True
        assert registry.get_ibis_table("p", "m") is None

    def test_invalidate_cache_not_cached(self):
        """Test invalidating non-existent cache returns False."""
        registry = ModelRegistry()

        result = registry.invalidate_cache("p", "unknown")

        assert result is False

    def test_invalidate_all_caches(self):
        """Test invalidating all cache entries."""
        registry = ModelRegistry()

        # Register multiple models with caches
        for i in range(3):
            config = self._create_mock_config("p", f"m{i}")
            mock_table = MagicMock()
            registry.register_model("p", f"m{i}", config, mock_table)

        count = registry.invalidate_all_caches()

        assert count == 3
        assert registry.cached_table_count == 0

    def test_cached_table_count(self):
        """Test cached_table_count property."""
        registry = ModelRegistry()

        assert registry.cached_table_count == 0

        for i in range(2):
            config = self._create_mock_config("p", f"m{i}")
            mock_table = MagicMock()
            registry.register_model("p", f"m{i}", config, mock_table)

        assert registry.cached_table_count == 2

    def test_clear_also_clears_cache(self):
        """Test that clear() also clears Ibis cache."""
        registry = ModelRegistry()
        config = self._create_mock_config("p", "m")
        mock_table = MagicMock()

        registry.register_model("p", "m", config, mock_table)
        assert registry.cached_table_count == 1

        registry.clear()

        assert registry.cached_table_count == 0
        assert registry.model_count == 0


class TestModelRegistryCacheWithFiles:
    """Tests for file-based cache invalidation."""

    def setup_method(self):
        """Reset singleton before each test."""
        ModelRegistry.reset_instance()
        ConfigParser._instances.clear()

    def _create_mock_config(self, schema, model):
        """Create a mock ConfigParser."""
        config = MagicMock(spec=ConfigParser)
        config.destination_schema_name = schema
        config.model_name = model
        config.materialization = "TABLE"
        return config

    def test_cache_with_file_path(self):
        """Test caching with file path for timestamp tracking."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("test: content")
            temp_path = f.name

        try:
            registry = ModelRegistry()
            config = self._create_mock_config("p", "m")
            mock_table = MagicMock()

            registry.register_model("p", "m", config, mock_table, file_path=temp_path)

            assert registry.is_cache_valid("p", "m") is True
        finally:
            os.unlink(temp_path)

    def test_cache_invalidation_on_file_change(self):
        """Test that cache is invalidated when source file changes."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("test: content")
            temp_path = f.name

        try:
            registry = ModelRegistry()
            config = self._create_mock_config("p", "m")
            mock_table = MagicMock()

            registry.register_model("p", "m", config, mock_table, file_path=temp_path)

            # Modify the file
            time.sleep(0.1)  # Ensure timestamp changes
            with open(temp_path, 'w') as f:
                f.write("modified: content")

            # Cache should be invalid now
            assert registry.is_cache_valid("p", "m") is False

            # get_ibis_table with validation should return None
            assert registry.get_ibis_table("p", "m", validate_cache=True) is None
        finally:
            os.unlink(temp_path)

    def test_get_ibis_table_skip_validation(self):
        """Test getting Ibis table without cache validation."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("test: content")
            temp_path = f.name

        try:
            registry = ModelRegistry()
            config = self._create_mock_config("p", "m")
            mock_table = MagicMock()

            registry.register_model("p", "m", config, mock_table, file_path=temp_path)

            # Modify the file
            time.sleep(0.1)
            with open(temp_path, 'w') as f:
                f.write("modified: content")

            # With validate_cache=False, should still return the table
            result = registry.get_ibis_table("p", "m", validate_cache=False)
            assert result is mock_table
        finally:
            os.unlink(temp_path)

    def test_get_stale_caches(self):
        """Test getting list of stale caches."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f1:
            f1.write("test: content1")
            temp_path1 = f1.name

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f2:
            f2.write("test: content2")
            temp_path2 = f2.name

        try:
            registry = ModelRegistry()

            # Register two models
            config1 = self._create_mock_config("p", "m1")
            config2 = self._create_mock_config("p", "m2")

            registry.register_model("p", "m1", config1, MagicMock(), file_path=temp_path1)
            registry.register_model("p", "m2", config2, MagicMock(), file_path=temp_path2)

            # Modify one file
            time.sleep(0.1)
            with open(temp_path1, 'w') as f:
                f.write("modified: content")

            stale = registry.get_stale_caches()

            assert "p.m1" in stale
            assert "p.m2" not in stale
        finally:
            os.unlink(temp_path1)
            os.unlink(temp_path2)


class TestModelRegistryContextManager:
    """Tests for context manager with Ibis cache."""

    def setup_method(self):
        """Reset singleton before each test."""
        ModelRegistry.reset_instance()
        ConfigParser._instances.clear()

    def _create_mock_config(self, schema, model):
        """Create a mock ConfigParser."""
        config = MagicMock(spec=ConfigParser)
        config.destination_schema_name = schema
        config.model_name = model
        config.materialization = "TABLE"
        return config

    def test_context_manager_clears_cache_on_exit(self):
        """Test that context manager clears cache on exit."""
        registry = ModelRegistry()
        config = self._create_mock_config("p", "m")
        mock_table = MagicMock()

        registry.register_model("p", "m", config, mock_table)

        with registry:
            assert registry.cached_table_count == 1
            assert registry.is_locked

        # After context, cache should be cleared
        assert registry.cached_table_count == 0
        assert not registry.is_locked

    def test_context_manager_clears_cache_on_exception(self):
        """Test that context manager clears cache even on exception."""
        registry = ModelRegistry()
        config = self._create_mock_config("p", "m")
        mock_table = MagicMock()

        registry.register_model("p", "m", config, mock_table)

        with pytest.raises(ValueError):
            with registry:
                assert registry.cached_table_count == 1
                raise ValueError("Test exception")

        # After exception, cache should still be cleared
        assert registry.cached_table_count == 0
        assert not registry.is_locked
