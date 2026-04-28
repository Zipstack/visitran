"""
ModelRegistry: Thread-safe singleton for managing model configurations.

This module provides a central registry for storing and accessing ConfigParser
instances with schema-qualified keys. It supports concurrent reads during
parallel execution and provides locking to prevent modifications during
active execution cycles.

Enhanced with Ibis Table caching and timestamp-based cache invalidation
for the YAML-to-Ibis Direct Execution architecture.
"""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

if TYPE_CHECKING:
    from ibis.expr.types import Table
    from backend.application.config_parser.config_parser import ConfigParser


class ExecutionStatus(Enum):
    """Status of a model's execution."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class ModelMetadata:
    """Metadata for a registered model."""

    table_name: str
    schema_name: str
    materialization_type: str
    execution_status: ExecutionStatus = ExecutionStatus.PENDING
    error_codes: list[str] = field(default_factory=list)
    config_loaded: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert metadata to dictionary representation."""
        return {
            "table_name": self.table_name,
            "schema_name": self.schema_name,
            "materialization_type": self.materialization_type,
            "execution_status": self.execution_status.value,
            "error_codes": self.error_codes.copy(),
            "config_loaded": self.config_loaded,
        }


@dataclass
class IbisTableCache:
    """
    Cache entry for an Ibis Table expression with timestamp-based invalidation.

    Attributes:
        ibis_table: The cached Ibis Table expression
        file_path: Path to the YAML source file
        file_mtime: Modification timestamp when cache was created
        created_at: When this cache entry was created (monotonic time)
    """

    ibis_table: Table
    file_path: Optional[str] = None
    file_mtime: Optional[float] = None
    created_at: float = field(default_factory=lambda: 0.0)

    def is_valid(self) -> bool:
        """
        Check if the cache entry is still valid.

        Returns:
            True if the source file hasn't been modified since caching
        """
        if self.file_path is None or self.file_mtime is None:
            # No file tracking, assume valid
            return True

        try:
            current_mtime = os.path.getmtime(self.file_path)
            return current_mtime == self.file_mtime
        except OSError:
            # File doesn't exist or can't be accessed
            return False


class ModelRegistry:
    """
    Thread-safe singleton registry for model configurations.

    The ModelRegistry serves as a central repository for all model configurations
    and metadata during DAG execution. It ensures thread-safe access for concurrent
    reads while preventing modifications during active execution cycles.

    Usage:
        # Get the singleton instance
        registry = ModelRegistry()

        # Register a model
        registry.register("public", "orders", config)

        # Use as context manager for execution
        with registry:
            config = registry.get("public", "orders")
            # ... execute transformations ...
        # Registry is automatically cleared after execution

    Attributes:
        _instance: Class-level singleton instance reference
        _configs: Dictionary mapping schema.model_name to ConfigParser instances
        _metadata: Dictionary mapping schema.model_name to ModelMetadata
        _locked: Boolean flag indicating if registry is in execution mode
        _lock: Threading lock for thread-safe operations
    """

    _instance: Optional[ModelRegistry] = None
    _initialized: bool = False

    def __new__(cls) -> ModelRegistry:
        """Enforce singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        """Initialize the registry (only on first instantiation)."""
        # Prevent re-initialization on subsequent __init__ calls
        if ModelRegistry._initialized:
            return

        self._configs: dict[str, ConfigParser] = {}
        self._metadata: dict[str, ModelMetadata] = {}
        self._locked: bool = False
        self._lock: threading.RLock = threading.RLock()
        self._lazy_loaders: dict[str, tuple[Any, dict[str, Any]]] = {}
        # Ibis Table caching
        self._ibis_cache: dict[str, IbisTableCache] = {}
        ModelRegistry._initialized = True

    @staticmethod
    def _make_key(schema: str, model: str) -> str:
        """Create a schema-qualified key for the registry."""
        return f"{schema}.{model}"

    def register(
        self,
        schema: str,
        model: str,
        config: ConfigParser,
        table_name: Optional[str] = None,
        materialization_type: str = "TABLE",
    ) -> None:
        """
        Register a model configuration in the registry.

        Args:
            schema: The schema name for the model
            model: The model name
            config: The ConfigParser instance for the model
            table_name: Optional table name (defaults to model name)
            materialization_type: Type of materialization (TABLE, VIEW, etc.)

        Raises:
            RuntimeError: If the registry is locked during execution
            ValueError: If a model with the same key is already registered
        """
        key = self._make_key(schema, model)

        with self._lock:
            if self._locked:
                raise RuntimeError(
                    f"Cannot modify locked registry. "
                    f"Attempted to register model '{key}' during active execution."
                )

            if key in self._configs:
                raise ValueError(
                    f"Model '{key}' is already registered. "
                    f"Use a different schema or model name, or clear the registry first."
                )

            self._configs[key] = config
            self._metadata[key] = ModelMetadata(
                table_name=table_name or model,
                schema_name=schema,
                materialization_type=materialization_type,
                config_loaded=True,
            )

    def register_lazy(
        self,
        schema: str,
        model: str,
        loader_func: Any,
        loader_kwargs: dict[str, Any],
        table_name: Optional[str] = None,
        materialization_type: str = "TABLE",
    ) -> None:
        """
        Register a model for lazy loading.

        The ConfigParser will only be instantiated when first accessed via get().

        Args:
            schema: The schema name for the model
            model: The model name
            loader_func: Callable that returns a ConfigParser when invoked
            loader_kwargs: Keyword arguments to pass to the loader function
            table_name: Optional table name (defaults to model name)
            materialization_type: Type of materialization

        Raises:
            RuntimeError: If the registry is locked during execution
            ValueError: If a model with the same key is already registered
        """
        key = self._make_key(schema, model)

        with self._lock:
            if self._locked:
                raise RuntimeError(
                    f"Cannot modify locked registry. "
                    f"Attempted to register lazy model '{key}' during active execution."
                )

            if key in self._configs or key in self._lazy_loaders:
                raise ValueError(
                    f"Model '{key}' is already registered. "
                    f"Use a different schema or model name, or clear the registry first."
                )

            self._lazy_loaders[key] = (loader_func, loader_kwargs)
            self._metadata[key] = ModelMetadata(
                table_name=table_name or model,
                schema_name=schema,
                materialization_type=materialization_type,
                config_loaded=False,
            )

    def get(self, schema: str, model: str) -> ConfigParser:
        """
        Retrieve a model configuration from the registry.

        If the model was registered for lazy loading, this will trigger
        the loading process on first access.

        Args:
            schema: The schema name
            model: The model name

        Returns:
            The ConfigParser instance for the model

        Raises:
            KeyError: If the model is not found in the registry
        """
        key = self._make_key(schema, model)

        with self._lock:
            # Check if already loaded
            if key in self._configs:
                return self._configs[key]

            # Check for lazy loading
            if key in self._lazy_loaders:
                loader_func, loader_kwargs = self._lazy_loaders[key]
                config = loader_func(**loader_kwargs)
                self._configs[key] = config
                self._metadata[key].config_loaded = True
                del self._lazy_loaders[key]
                return config

            raise KeyError(
                f"Model '{key}' not found in registry. "
                f"Ensure the model is registered before accessing it."
            )

    def get_metadata(self, schema: str, model: str) -> ModelMetadata:
        """
        Retrieve metadata for a registered model.

        Args:
            schema: The schema name
            model: The model name

        Returns:
            The ModelMetadata for the model

        Raises:
            KeyError: If the model is not found in the registry
        """
        key = self._make_key(schema, model)

        with self._lock:
            if key not in self._metadata:
                raise KeyError(
                    f"Model '{key}' not found in registry. "
                    f"Ensure the model is registered before accessing metadata."
                )
            return self._metadata[key]

    def update_status(
        self,
        schema: str,
        model: str,
        status: ExecutionStatus,
        error_codes: Optional[list[str]] = None,
    ) -> None:
        """
        Update the execution status of a model.

        Args:
            schema: The schema name
            model: The model name
            status: The new execution status
            error_codes: Optional list of error codes to record

        Raises:
            KeyError: If the model is not found in the registry
        """
        key = self._make_key(schema, model)

        with self._lock:
            if key not in self._metadata:
                raise KeyError(
                    f"Model '{key}' not found in registry. "
                    f"Cannot update status for unregistered model."
                )

            self._metadata[key].execution_status = status
            if error_codes:
                self._metadata[key].error_codes.extend(error_codes)

    def contains(self, schema: str, model: str) -> bool:
        """Check if a model is registered (loaded or lazy)."""
        key = self._make_key(schema, model)
        with self._lock:
            return key in self._configs or key in self._lazy_loaders

    def list_models(self) -> list[str]:
        """Return a list of all registered model keys."""
        with self._lock:
            all_keys = set(self._configs.keys()) | set(self._lazy_loaders.keys())
            return sorted(all_keys)

    def clear(self) -> None:
        """
        Clear all registered models from the registry.

        This resets the registry to its initial empty state.
        Should be called after execution completes.
        """
        with self._lock:
            self._configs.clear()
            self._metadata.clear()
            self._lazy_loaders.clear()
            self._ibis_cache.clear()
            self._locked = False

    # =========================================================================
    # Ibis Table Caching Methods
    # =========================================================================

    def register_model(
        self,
        schema: str,
        model: str,
        config: ConfigParser,
        ibis_table: Table,
        file_path: Optional[Union[str, Path]] = None,
    ) -> None:
        """
        Register a model with both ConfigParser and Ibis Table.

        This is the primary method for registering models in the YAML-to-Ibis
        architecture. It stores both the configuration and the compiled
        Ibis expression together.

        Args:
            schema: The schema name for the model
            model: The model name
            config: The ConfigParser instance for the model
            ibis_table: The compiled Ibis Table expression
            file_path: Optional path to the YAML source file for cache invalidation

        Raises:
            RuntimeError: If the registry is locked during execution
            ValueError: If a model with the same key is already registered
        """
        key = self._make_key(schema, model)

        with self._lock:
            if self._locked:
                raise RuntimeError(
                    f"Cannot modify locked registry. "
                    f"Attempted to register model '{key}' during active execution."
                )

            if key in self._configs:
                raise ValueError(
                    f"Model '{key}' is already registered. "
                    f"Use a different schema or model name, or clear the registry first."
                )

            # Register the config
            self._configs[key] = config
            self._metadata[key] = ModelMetadata(
                table_name=model,
                schema_name=schema,
                materialization_type=config.materialization if hasattr(config, 'materialization') else "TABLE",
                config_loaded=True,
            )

            # Cache the Ibis table
            self._cache_ibis_table_internal(key, ibis_table, file_path)

    def get_model(
        self,
        schema: str,
        model: str,
    ) -> tuple[ConfigParser, Optional[Table]]:
        """
        Retrieve both ConfigParser and Ibis Table for a model.

        Args:
            schema: The schema name
            model: The model name

        Returns:
            Tuple of (ConfigParser, ibis.Table) - Table may be None if not cached

        Raises:
            KeyError: If the model is not found in the registry
        """
        key = self._make_key(schema, model)

        with self._lock:
            if key not in self._configs and key not in self._lazy_loaders:
                raise KeyError(
                    f"Model '{key}' not found in registry. "
                    f"Ensure the model is registered before accessing it."
                )

            config = self.get(schema, model)
            ibis_table = self.get_ibis_table(schema, model)

            return (config, ibis_table)

    def cache_ibis_table(
        self,
        schema: str,
        model: str,
        ibis_table: Table,
        file_path: Optional[Union[str, Path]] = None,
    ) -> None:
        """
        Cache an Ibis Table expression for a registered model.

        Args:
            schema: The schema name
            model: The model name
            ibis_table: The Ibis Table expression to cache
            file_path: Optional path to source file for timestamp-based invalidation

        Raises:
            RuntimeError: If the registry is locked during execution
            KeyError: If the model is not registered
        """
        key = self._make_key(schema, model)

        with self._lock:
            if self._locked:
                raise RuntimeError(
                    f"Cannot modify locked registry. "
                    f"Attempted to cache Ibis table for '{key}' during active execution."
                )

            if key not in self._configs and key not in self._lazy_loaders:
                raise KeyError(
                    f"Model '{key}' not found in registry. "
                    f"Register the model before caching its Ibis table."
                )

            self._cache_ibis_table_internal(key, ibis_table, file_path)

    def _cache_ibis_table_internal(
        self,
        key: str,
        ibis_table: Table,
        file_path: Optional[Union[str, Path]] = None,
    ) -> None:
        """
        Internal method to cache an Ibis table.

        Args:
            key: The schema.model key
            ibis_table: The Ibis Table expression
            file_path: Optional source file path
        """
        import time

        file_mtime = None
        file_path_str = None

        if file_path is not None:
            file_path_str = str(file_path)
            try:
                file_mtime = os.path.getmtime(file_path_str)
            except OSError:
                pass

        self._ibis_cache[key] = IbisTableCache(
            ibis_table=ibis_table,
            file_path=file_path_str,
            file_mtime=file_mtime,
            created_at=time.monotonic(),
        )

    def get_ibis_table(
        self,
        schema: str,
        model: str,
        validate_cache: bool = True,
    ) -> Optional[Table]:
        """
        Retrieve a cached Ibis Table expression.

        Args:
            schema: The schema name
            model: The model name
            validate_cache: If True, check timestamp and return None if stale

        Returns:
            The cached Ibis Table, or None if not cached or cache is invalid
        """
        key = self._make_key(schema, model)

        with self._lock:
            cache_entry = self._ibis_cache.get(key)
            if cache_entry is None:
                return None

            if validate_cache and not cache_entry.is_valid():
                # Cache is stale, remove it
                del self._ibis_cache[key]
                return None

            return cache_entry.ibis_table

    def is_cache_valid(self, schema: str, model: str) -> bool:
        """
        Check if the Ibis table cache for a model is valid.

        Args:
            schema: The schema name
            model: The model name

        Returns:
            True if cache exists and is valid, False otherwise
        """
        key = self._make_key(schema, model)

        with self._lock:
            cache_entry = self._ibis_cache.get(key)
            if cache_entry is None:
                return False
            return cache_entry.is_valid()

    def invalidate_cache(self, schema: str, model: str) -> bool:
        """
        Invalidate the Ibis table cache for a specific model.

        Args:
            schema: The schema name
            model: The model name

        Returns:
            True if cache was invalidated, False if no cache existed
        """
        key = self._make_key(schema, model)

        with self._lock:
            if key in self._ibis_cache:
                del self._ibis_cache[key]
                return True
            return False

    def invalidate_all_caches(self) -> int:
        """
        Invalidate all Ibis table caches.

        Returns:
            Number of cache entries invalidated
        """
        with self._lock:
            count = len(self._ibis_cache)
            self._ibis_cache.clear()
            return count

    def get_stale_caches(self) -> list[str]:
        """
        Get list of models with stale (invalid) caches.

        Returns:
            List of schema.model keys with stale caches
        """
        with self._lock:
            stale = []
            for key, cache_entry in self._ibis_cache.items():
                if not cache_entry.is_valid():
                    stale.append(key)
            return stale

    @property
    def cached_table_count(self) -> int:
        """Return the number of cached Ibis tables."""
        with self._lock:
            return len(self._ibis_cache)

    def lock(self) -> None:
        """
        Lock the registry to prevent modifications.

        Call this when starting execution to ensure no new models
        can be registered during the execution cycle.
        """
        with self._lock:
            self._locked = True

    def unlock(self) -> None:
        """
        Unlock the registry to allow modifications.

        Call this after execution completes to allow new registrations.
        """
        with self._lock:
            self._locked = False

    @property
    def is_locked(self) -> bool:
        """Check if the registry is currently locked."""
        with self._lock:
            return self._locked

    @property
    def model_count(self) -> int:
        """Return the total number of registered models (loaded + lazy)."""
        with self._lock:
            return len(self._configs) + len(self._lazy_loaders)

    def __enter__(self) -> ModelRegistry:
        """Enter execution context - lock the registry."""
        self.lock()
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> bool:
        """Exit execution context - unlock and clear the registry."""
        self.unlock()
        self.clear()
        # Don't suppress exceptions
        return False

    def __len__(self) -> int:
        """Return the number of registered models."""
        return self.model_count

    def __contains__(self, key: str) -> bool:
        """Check if a schema.model key is in the registry."""
        with self._lock:
            return key in self._configs or key in self._lazy_loaders

    @classmethod
    def reset_instance(cls) -> None:
        """
        Reset the singleton instance (for testing purposes only).

        WARNING: This should only be used in test teardown.
        """
        if cls._instance is not None:
            cls._instance.clear()
        cls._instance = None
        cls._initialized = False
