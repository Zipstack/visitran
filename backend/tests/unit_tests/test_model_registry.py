"""Unit tests for ModelRegistry singleton class."""

import threading
from unittest.mock import MagicMock, Mock

import pytest

from backend.application.config_parser.model_registry import (
    ExecutionStatus,
    ModelMetadata,
    ModelRegistry,
)


@pytest.fixture(autouse=True)
def reset_registry():
    """Reset the ModelRegistry singleton before and after each test."""
    ModelRegistry.reset_instance()
    yield
    ModelRegistry.reset_instance()


class TestModelRegistrySingleton:
    """Tests for singleton pattern."""

    def test_singleton_pattern_returns_same_instance(self):
        """Test that ModelRegistry returns the same instance."""
        registry1 = ModelRegistry()
        registry2 = ModelRegistry()
        assert registry1 is registry2

    def test_singleton_maintains_state(self):
        """Test that singleton maintains state across references."""
        registry1 = ModelRegistry()
        config = MagicMock()
        registry1.register("schema", "model", config)

        registry2 = ModelRegistry()
        assert registry2.contains("schema", "model")


class TestModelRegistryRegistration:
    """Tests for model registration."""

    def test_register_model_successfully(self):
        """Test registering a new model."""
        registry = ModelRegistry()
        config = MagicMock()

        registry.register("public", "orders", config)

        assert registry.contains("public", "orders")
        assert registry.get("public", "orders") is config

    def test_register_with_custom_table_name(self):
        """Test registering with custom table name."""
        registry = ModelRegistry()
        config = MagicMock()

        registry.register(
            "public", "orders", config, table_name="order_table", materialization_type="VIEW"
        )

        metadata = registry.get_metadata("public", "orders")
        assert metadata.table_name == "order_table"
        assert metadata.materialization_type == "VIEW"

    def test_register_duplicate_raises_error(self):
        """Test that duplicate registration raises ValueError."""
        registry = ModelRegistry()
        config = MagicMock()

        registry.register("public", "orders", config)

        with pytest.raises(ValueError) as exc_info:
            registry.register("public", "orders", config)

        assert "public.orders" in str(exc_info.value)
        assert "already registered" in str(exc_info.value)

    def test_register_when_locked_raises_error(self):
        """Test that registration fails when registry is locked."""
        registry = ModelRegistry()
        config = MagicMock()

        registry.lock()

        with pytest.raises(RuntimeError) as exc_info:
            registry.register("public", "orders", config)

        assert "Cannot modify locked registry" in str(exc_info.value)


class TestModelRegistryLazyLoading:
    """Tests for lazy loading functionality."""

    def test_register_lazy_model(self):
        """Test registering a model for lazy loading."""
        registry = ModelRegistry()
        loader = MagicMock(return_value=MagicMock())

        registry.register_lazy("public", "orders", loader, {})

        assert registry.contains("public", "orders")
        # Loader should not be called yet
        loader.assert_not_called()

    def test_lazy_load_triggers_on_get(self):
        """Test that lazy loading triggers on first access."""
        registry = ModelRegistry()
        expected_config = MagicMock()
        loader = MagicMock(return_value=expected_config)

        registry.register_lazy("public", "orders", loader, {"file": "orders.yaml"})

        # First access triggers loading
        config = registry.get("public", "orders")

        loader.assert_called_once_with(file="orders.yaml")
        assert config is expected_config

    def test_lazy_load_only_once(self):
        """Test that lazy loading only happens once."""
        registry = ModelRegistry()
        expected_config = MagicMock()
        loader = MagicMock(return_value=expected_config)

        registry.register_lazy("public", "orders", loader, {})

        # Multiple accesses
        registry.get("public", "orders")
        registry.get("public", "orders")
        registry.get("public", "orders")

        # Should only call loader once
        loader.assert_called_once()

    def test_lazy_metadata_updates_after_load(self):
        """Test that metadata.config_loaded is updated after lazy load."""
        registry = ModelRegistry()
        loader = MagicMock(return_value=MagicMock())

        registry.register_lazy("public", "orders", loader, {})

        # Before loading
        metadata = registry.get_metadata("public", "orders")
        assert metadata.config_loaded is False

        # Trigger loading
        registry.get("public", "orders")

        # After loading
        metadata = registry.get_metadata("public", "orders")
        assert metadata.config_loaded is True


class TestModelRegistryGet:
    """Tests for get operations."""

    def test_get_existing_model(self):
        """Test getting an existing model."""
        registry = ModelRegistry()
        config = MagicMock()
        registry.register("public", "orders", config)

        result = registry.get("public", "orders")

        assert result is config

    def test_get_missing_model_raises_keyerror(self):
        """Test that getting missing model raises KeyError."""
        registry = ModelRegistry()

        with pytest.raises(KeyError) as exc_info:
            registry.get("public", "nonexistent")

        assert "public.nonexistent" in str(exc_info.value)
        assert "not found" in str(exc_info.value)

    def test_get_metadata_for_existing_model(self):
        """Test getting metadata for existing model."""
        registry = ModelRegistry()
        config = MagicMock()
        registry.register("public", "orders", config, materialization_type="VIEW")

        metadata = registry.get_metadata("public", "orders")

        assert isinstance(metadata, ModelMetadata)
        assert metadata.materialization_type == "VIEW"

    def test_get_metadata_for_missing_model_raises_keyerror(self):
        """Test that getting metadata for missing model raises KeyError."""
        registry = ModelRegistry()

        with pytest.raises(KeyError) as exc_info:
            registry.get_metadata("public", "nonexistent")

        assert "public.nonexistent" in str(exc_info.value)


class TestModelRegistryClear:
    """Tests for clear operation."""

    def test_clear_removes_all_models(self):
        """Test that clear removes all registered models."""
        registry = ModelRegistry()
        registry.register("public", "orders", MagicMock())
        registry.register("public", "customers", MagicMock())

        registry.clear()

        assert len(registry) == 0
        assert not registry.contains("public", "orders")
        assert not registry.contains("public", "customers")

    def test_clear_resets_locked_state(self):
        """Test that clear resets the locked state."""
        registry = ModelRegistry()
        registry.lock()
        assert registry.is_locked

        registry.clear()

        assert not registry.is_locked


class TestModelRegistryLocking:
    """Tests for locking mechanism."""

    def test_lock_prevents_registration(self):
        """Test that locked registry prevents registration."""
        registry = ModelRegistry()
        registry.lock()

        with pytest.raises(RuntimeError):
            registry.register("public", "orders", MagicMock())

    def test_lock_allows_reads(self):
        """Test that locked registry allows reads."""
        registry = ModelRegistry()
        config = MagicMock()
        registry.register("public", "orders", config)
        registry.lock()

        # Should not raise
        result = registry.get("public", "orders")
        assert result is config

    def test_unlock_allows_registration(self):
        """Test that unlocked registry allows registration."""
        registry = ModelRegistry()
        registry.lock()
        registry.unlock()

        # Should not raise
        registry.register("public", "orders", MagicMock())
        assert registry.contains("public", "orders")


class TestModelRegistryContextManager:
    """Tests for context manager protocol."""

    def test_context_manager_locks_on_enter(self):
        """Test that context manager locks registry on enter."""
        registry = ModelRegistry()

        with registry:
            assert registry.is_locked

    def test_context_manager_clears_on_exit(self):
        """Test that context manager clears registry on exit."""
        registry = ModelRegistry()
        registry.register("public", "orders", MagicMock())

        with registry:
            pass

        assert len(registry) == 0

    def test_context_manager_clears_on_exception(self):
        """Test that context manager clears registry even on exception."""
        registry = ModelRegistry()
        registry.register("public", "orders", MagicMock())

        try:
            with registry:
                raise ValueError("Test error")
        except ValueError:
            pass

        assert len(registry) == 0
        assert not registry.is_locked

    def test_context_manager_returns_registry(self):
        """Test that context manager returns the registry."""
        registry = ModelRegistry()

        with registry as ctx:
            assert ctx is registry


class TestModelRegistryStatusUpdate:
    """Tests for status update operations."""

    def test_update_status(self):
        """Test updating execution status."""
        registry = ModelRegistry()
        registry.register("public", "orders", MagicMock())

        registry.update_status("public", "orders", ExecutionStatus.RUNNING)

        metadata = registry.get_metadata("public", "orders")
        assert metadata.execution_status == ExecutionStatus.RUNNING

    def test_update_status_with_error_codes(self):
        """Test updating status with error codes."""
        registry = ModelRegistry()
        registry.register("public", "orders", MagicMock())

        registry.update_status(
            "public", "orders", ExecutionStatus.FAILED, error_codes=["E001", "E002"]
        )

        metadata = registry.get_metadata("public", "orders")
        assert metadata.execution_status == ExecutionStatus.FAILED
        assert metadata.error_codes == ["E001", "E002"]

    def test_update_status_for_missing_model_raises_keyerror(self):
        """Test that updating status for missing model raises KeyError."""
        registry = ModelRegistry()

        with pytest.raises(KeyError):
            registry.update_status("public", "nonexistent", ExecutionStatus.RUNNING)


class TestModelRegistryThreadSafety:
    """Tests for thread-safety."""

    def test_concurrent_reads(self):
        """Test that concurrent reads work correctly."""
        registry = ModelRegistry()
        config = MagicMock()
        registry.register("public", "orders", config)

        results = []
        errors = []

        def read_model():
            try:
                result = registry.get("public", "orders")
                results.append(result)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=read_model) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == 10
        assert all(r is config for r in results)

    def test_concurrent_registration_race_condition(self):
        """Test that concurrent registration handles race conditions."""
        registry = ModelRegistry()
        success_count = []
        error_count = []

        def register_model(i):
            try:
                registry.register("public", f"model_{i}", MagicMock())
                success_count.append(i)
            except ValueError:
                error_count.append(i)

        threads = [threading.Thread(target=register_model, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All should succeed since they have different names
        assert len(success_count) == 10
        assert len(error_count) == 0


class TestModelMetadata:
    """Tests for ModelMetadata dataclass."""

    def test_metadata_to_dict(self):
        """Test metadata to_dict conversion."""
        metadata = ModelMetadata(
            table_name="orders",
            schema_name="public",
            materialization_type="TABLE",
            execution_status=ExecutionStatus.COMPLETED,
            error_codes=["E001"],
            config_loaded=True,
        )

        result = metadata.to_dict()

        assert result["table_name"] == "orders"
        assert result["schema_name"] == "public"
        assert result["execution_status"] == "completed"
        assert result["error_codes"] == ["E001"]

    def test_metadata_error_codes_copy(self):
        """Test that to_dict returns a copy of error_codes."""
        metadata = ModelMetadata(
            table_name="orders",
            schema_name="public",
            materialization_type="TABLE",
            error_codes=["E001"],
        )

        result = metadata.to_dict()
        result["error_codes"].append("E002")

        # Original should be unchanged
        assert metadata.error_codes == ["E001"]


class TestModelRegistryUtilityMethods:
    """Tests for utility methods."""

    def test_list_models(self):
        """Test listing all registered models."""
        registry = ModelRegistry()
        registry.register("public", "orders", MagicMock())
        registry.register("public", "customers", MagicMock())
        registry.register_lazy("sales", "reports", MagicMock(), {})

        models = registry.list_models()

        assert sorted(models) == ["public.customers", "public.orders", "sales.reports"]

    def test_model_count(self):
        """Test model count property."""
        registry = ModelRegistry()
        registry.register("public", "orders", MagicMock())
        registry.register_lazy("public", "customers", MagicMock(), {})

        assert registry.model_count == 2
        assert len(registry) == 2

    def test_contains_with_string_key(self):
        """Test __contains__ with string key."""
        registry = ModelRegistry()
        registry.register("public", "orders", MagicMock())

        assert "public.orders" in registry
        assert "public.nonexistent" not in registry
