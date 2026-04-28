"""
Feature Flags for YAML-to-Ibis Direct Execution.

This module provides configuration flags to control execution behavior
between legacy Python generation and direct Ibis execution paths.

Supports configuration from:
1. Environment variables (highest priority)
2. Configuration files (YAML/JSON)
3. Default values (lowest priority)

Usage:
    from backend.application.config_parser.feature_flags import FeatureFlags

    if FeatureFlags.is_direct_execution_enabled():
        # Execute via direct Ibis path
        pass
    else:
        # Execute via legacy Python generation
        pass
"""

from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


class ExecutionMode(Enum):
    """Execution mode for model transformations."""

    LEGACY = "legacy"           # Legacy Python file generation only
    DIRECT = "direct"           # Direct Ibis execution only
    PARALLEL = "parallel"       # Both paths for validation


class RolloutPhase(Enum):
    """
    Phased rollout stages for direct execution.

    Phase 1: Validation only - both paths run, results logged, legacy used
    Phase 2: Allowlist - only allowlisted models use direct execution
    Phase 3: Full rollout - all models use direct execution
    """

    PHASE_1_VALIDATION = "phase_1_validation"
    PHASE_2_ALLOWLIST = "phase_2_allowlist"
    PHASE_3_FULL_ROLLOUT = "phase_3_full_rollout"


@dataclass
class FeatureFlagState:
    """
    State container for feature flags.

    Attributes:
        enable_direct_execution: Master flag for direct Ibis execution
        execution_mode: Current execution mode
        rollout_phase: Current rollout phase for phased deployment
        suppress_python_files: Whether to skip writing Python files
        enable_sql_validation: Whether to compare SQL outputs
        validation_log_level: Logging level for validation (debug, info, warning)
        config_source: Where the configuration was loaded from
        loaded_at: When the configuration was loaded
    """

    enable_direct_execution: bool = False
    execution_mode: ExecutionMode = ExecutionMode.LEGACY
    rollout_phase: RolloutPhase = RolloutPhase.PHASE_1_VALIDATION
    suppress_python_files: bool = False
    enable_sql_validation: bool = True
    validation_log_level: str = "info"
    config_source: str = "default"
    loaded_at: Optional[datetime] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert state to dictionary."""
        return {
            "enable_direct_execution": self.enable_direct_execution,
            "execution_mode": self.execution_mode.value,
            "rollout_phase": self.rollout_phase.value,
            "suppress_python_files": self.suppress_python_files,
            "enable_sql_validation": self.enable_sql_validation,
            "validation_log_level": self.validation_log_level,
            "config_source": self.config_source,
            "loaded_at": self.loaded_at.isoformat() if self.loaded_at else None,
        }


class FeatureFlags:
    """
    Singleton feature flag manager for execution mode control.

    Provides thread-safe access to feature flags that control the
    execution path (legacy vs direct Ibis).

    Usage:
        # Check if direct execution is enabled
        if FeatureFlags.is_direct_execution_enabled():
            ...

        # Get current execution mode
        mode = FeatureFlags.get_execution_mode()

        # Enable direct execution (for testing)
        with FeatureFlags.override(enable_direct_execution=True):
            ...
    """

    _instance: Optional[FeatureFlags] = None
    _initialized: bool = False
    _lock: threading.RLock = threading.RLock()

    # Default state
    _state: FeatureFlagState = FeatureFlagState()

    # Environment variable prefix
    ENV_PREFIX = "VISITRAN_"

    def __new__(cls) -> FeatureFlags:
        """Enforce singleton pattern."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    # Default config file paths to search
    CONFIG_FILE_PATHS = [
        "visitran_flags.yaml",
        "visitran_flags.json",
        ".visitran/flags.yaml",
        ".visitran/flags.json",
    ]

    def __init__(self) -> None:
        """Initialize feature flags from config file and environment."""
        if FeatureFlags._initialized:
            return

        with FeatureFlags._lock:
            if FeatureFlags._initialized:
                return

            # Load from config file first (lower priority)
            self._load_from_config_file()

            # Load from environment (higher priority, overrides config file)
            self._load_from_environment()

            # Record when loaded
            FeatureFlags._state.loaded_at = datetime.utcnow()
            FeatureFlags._initialized = True

            logger.info(
                f"Feature flags loaded from {FeatureFlags._state.config_source}: "
                f"mode={FeatureFlags._state.execution_mode.value}, "
                f"phase={FeatureFlags._state.rollout_phase.value}"
            )

    def _load_from_config_file(self) -> None:
        """Load feature flags from configuration file."""
        config_path = os.getenv(f"{self.ENV_PREFIX}CONFIG_FILE")

        if config_path:
            # Use explicitly specified config file
            paths_to_try = [config_path]
        else:
            # Search default paths
            paths_to_try = self.CONFIG_FILE_PATHS

        for path in paths_to_try:
            try:
                config_file = Path(path)
                if not config_file.exists():
                    continue

                with open(config_file) as f:
                    if path.endswith(".json"):
                        config = json.load(f)
                    else:
                        # Try YAML
                        try:
                            import yaml
                            config = yaml.safe_load(f)
                        except ImportError:
                            logger.debug("PyYAML not available, skipping YAML config files")
                            continue

                self._apply_config(config, source=str(config_file))
                logger.info(f"Loaded feature flags from {config_file}")
                return

            except Exception as e:
                logger.warning(f"Failed to load config from {path}: {e}")
                continue

    def _apply_config(self, config: dict[str, Any], source: str) -> None:
        """Apply configuration dictionary to state."""
        if not isinstance(config, dict):
            logger.warning(f"Invalid config format from {source}, expected dict")
            return

        FeatureFlags._state.config_source = source

        # enable_direct_execution
        if "enable_direct_execution" in config:
            value = self._parse_bool(
                config["enable_direct_execution"],
                "enable_direct_execution",
                source,
            )
            if value is not None:
                FeatureFlags._state.enable_direct_execution = value
                if value:
                    FeatureFlags._state.execution_mode = ExecutionMode.PARALLEL

        # execution_mode
        if "execution_mode" in config:
            mode_str = str(config["execution_mode"]).lower()
            try:
                mode = ExecutionMode(mode_str)
                FeatureFlags._state.execution_mode = mode
                FeatureFlags._state.enable_direct_execution = mode != ExecutionMode.LEGACY
            except ValueError:
                logger.warning(
                    f"Invalid execution_mode '{mode_str}' in {source}, "
                    f"using default. Valid values: legacy, direct, parallel"
                )

        # rollout_phase
        if "rollout_phase" in config:
            phase_str = str(config["rollout_phase"]).lower()
            try:
                phase = RolloutPhase(phase_str)
                FeatureFlags._state.rollout_phase = phase
            except ValueError:
                logger.warning(
                    f"Invalid rollout_phase '{phase_str}' in {source}, "
                    f"using default. Valid values: phase_1_validation, "
                    f"phase_2_allowlist, phase_3_full_rollout"
                )

        # suppress_python_files
        if "suppress_python_files" in config:
            value = self._parse_bool(
                config["suppress_python_files"],
                "suppress_python_files",
                source,
            )
            if value is not None:
                FeatureFlags._state.suppress_python_files = value

        # enable_sql_validation
        if "enable_sql_validation" in config:
            value = self._parse_bool(
                config["enable_sql_validation"],
                "enable_sql_validation",
                source,
            )
            if value is not None:
                FeatureFlags._state.enable_sql_validation = value

        # validation_log_level
        if "validation_log_level" in config:
            level = str(config["validation_log_level"]).lower()
            if level in ("debug", "info", "warning", "error"):
                FeatureFlags._state.validation_log_level = level
            else:
                logger.warning(
                    f"Invalid validation_log_level '{level}' in {source}, "
                    f"using default. Valid values: debug, info, warning, error"
                )

    def _parse_bool(
        self,
        value: Any,
        field_name: str,
        source: str,
    ) -> Optional[bool]:
        """Parse a boolean value with proper logging for invalid inputs."""
        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            lower = value.lower()
            if lower in ("true", "1", "yes", "on"):
                return True
            if lower in ("false", "0", "no", "off"):
                return False

        logger.warning(
            f"Invalid boolean value '{value}' for {field_name} in {source}, "
            f"using default (false). Valid values: true, false, yes, no, 1, 0"
        )
        return None

    def _load_from_environment(self) -> None:
        """Load feature flags from environment variables (overrides config file)."""
        env_loaded = False

        # VISITRAN_ENABLE_DIRECT_EXECUTION
        env_direct = os.getenv(f"{self.ENV_PREFIX}ENABLE_DIRECT_EXECUTION", "")
        if env_direct:
            value = self._parse_bool(env_direct, "ENABLE_DIRECT_EXECUTION", "environment")
            if value is not None:
                FeatureFlags._state.enable_direct_execution = value
                if value:
                    FeatureFlags._state.execution_mode = ExecutionMode.PARALLEL
                env_loaded = True

        # VISITRAN_EXECUTION_MODE
        env_mode = os.getenv(f"{self.ENV_PREFIX}EXECUTION_MODE", "")
        if env_mode:
            mode_str = env_mode.lower()
            try:
                mode = ExecutionMode(mode_str)
                FeatureFlags._state.execution_mode = mode
                FeatureFlags._state.enable_direct_execution = mode != ExecutionMode.LEGACY
                env_loaded = True
            except ValueError:
                logger.warning(
                    f"Invalid VISITRAN_EXECUTION_MODE '{env_mode}', "
                    f"using default. Valid values: legacy, direct, parallel"
                )

        # VISITRAN_ROLLOUT_PHASE
        env_phase = os.getenv(f"{self.ENV_PREFIX}ROLLOUT_PHASE", "")
        if env_phase:
            phase_str = env_phase.lower()
            try:
                phase = RolloutPhase(phase_str)
                FeatureFlags._state.rollout_phase = phase
                env_loaded = True
            except ValueError:
                logger.warning(
                    f"Invalid VISITRAN_ROLLOUT_PHASE '{env_phase}', "
                    f"using default. Valid values: phase_1_validation, "
                    f"phase_2_allowlist, phase_3_full_rollout"
                )

        # VISITRAN_SUPPRESS_PYTHON_FILES
        env_suppress = os.getenv(f"{self.ENV_PREFIX}SUPPRESS_PYTHON_FILES", "")
        if env_suppress:
            value = self._parse_bool(env_suppress, "SUPPRESS_PYTHON_FILES", "environment")
            if value is not None:
                FeatureFlags._state.suppress_python_files = value
                env_loaded = True

        # VISITRAN_ENABLE_SQL_VALIDATION
        env_validation = os.getenv(f"{self.ENV_PREFIX}ENABLE_SQL_VALIDATION", "")
        if env_validation:
            value = self._parse_bool(env_validation, "ENABLE_SQL_VALIDATION", "environment")
            if value is not None:
                FeatureFlags._state.enable_sql_validation = value
                env_loaded = True

        if env_loaded:
            FeatureFlags._state.config_source = "environment"

    @classmethod
    def is_direct_execution_enabled(cls) -> bool:
        """
        Check if direct Ibis execution is enabled.

        Returns:
            True if direct execution is enabled (parallel or direct mode)
        """
        cls()  # Ensure initialized
        return cls._state.enable_direct_execution

    @classmethod
    def get_execution_mode(cls) -> ExecutionMode:
        """
        Get the current execution mode.

        Returns:
            The current ExecutionMode
        """
        cls()  # Ensure initialized
        return cls._state.execution_mode

    @classmethod
    def should_suppress_python_files(cls) -> bool:
        """
        Check if Python file generation should be suppressed.

        Returns:
            True if Python files should not be written
        """
        cls()  # Ensure initialized
        return cls._state.suppress_python_files

    @classmethod
    def is_sql_validation_enabled(cls) -> bool:
        """
        Check if SQL validation is enabled.

        Returns:
            True if SQL outputs should be compared and validated
        """
        cls()  # Ensure initialized
        return cls._state.enable_sql_validation

    @classmethod
    def get_rollout_phase(cls) -> RolloutPhase:
        """
        Get the current rollout phase.

        Returns:
            The current RolloutPhase
        """
        cls()  # Ensure initialized
        return cls._state.rollout_phase

    @classmethod
    def is_validation_only_mode(cls) -> bool:
        """
        Check if in validation-only mode (Phase 1).

        In this mode, both paths run but legacy results are used.

        Returns:
            True if in validation-only mode
        """
        cls()  # Ensure initialized
        return cls._state.rollout_phase == RolloutPhase.PHASE_1_VALIDATION

    @classmethod
    def is_allowlist_mode(cls) -> bool:
        """
        Check if in allowlist mode (Phase 2).

        In this mode, only allowlisted models use direct execution.

        Returns:
            True if in allowlist mode
        """
        cls()  # Ensure initialized
        return cls._state.rollout_phase == RolloutPhase.PHASE_2_ALLOWLIST

    @classmethod
    def is_full_rollout_mode(cls) -> bool:
        """
        Check if in full rollout mode (Phase 3).

        In this mode, all models use direct execution.

        Returns:
            True if in full rollout mode
        """
        cls()  # Ensure initialized
        return cls._state.rollout_phase == RolloutPhase.PHASE_3_FULL_ROLLOUT

    @classmethod
    def get_state(cls) -> FeatureFlagState:
        """
        Get the current feature flag state.

        Returns:
            Copy of the current FeatureFlagState
        """
        cls()  # Ensure initialized
        return FeatureFlagState(
            enable_direct_execution=cls._state.enable_direct_execution,
            execution_mode=cls._state.execution_mode,
            rollout_phase=cls._state.rollout_phase,
            suppress_python_files=cls._state.suppress_python_files,
            enable_sql_validation=cls._state.enable_sql_validation,
            validation_log_level=cls._state.validation_log_level,
            config_source=cls._state.config_source,
            loaded_at=cls._state.loaded_at,
        )

    @classmethod
    def set_state(cls, **kwargs: Any) -> None:
        """
        Set feature flag state.

        Args:
            **kwargs: Keyword arguments matching FeatureFlagState fields
        """
        cls()  # Ensure initialized
        with cls._lock:
            if "enable_direct_execution" in kwargs:
                cls._state.enable_direct_execution = bool(kwargs["enable_direct_execution"])
            if "execution_mode" in kwargs:
                mode = kwargs["execution_mode"]
                if isinstance(mode, str):
                    mode = ExecutionMode(mode)
                cls._state.execution_mode = mode
            if "rollout_phase" in kwargs:
                phase = kwargs["rollout_phase"]
                if isinstance(phase, str):
                    phase = RolloutPhase(phase)
                cls._state.rollout_phase = phase
            if "suppress_python_files" in kwargs:
                cls._state.suppress_python_files = bool(kwargs["suppress_python_files"])
            if "enable_sql_validation" in kwargs:
                cls._state.enable_sql_validation = bool(kwargs["enable_sql_validation"])
            if "validation_log_level" in kwargs:
                cls._state.validation_log_level = str(kwargs["validation_log_level"])

    @classmethod
    def reset(cls) -> None:
        """Reset feature flags to defaults."""
        with cls._lock:
            cls._state = FeatureFlagState()
            cls._initialized = False

    class override:
        """
        Context manager for temporarily overriding feature flags.

        Usage:
            with FeatureFlags.override(enable_direct_execution=True):
                # Code runs with direct execution enabled
                pass
            # Original state restored
        """

        def __init__(self, **kwargs: Any):
            """
            Initialize override context.

            Args:
                **kwargs: Feature flag values to override
            """
            self._overrides = kwargs
            self._previous_state: Optional[FeatureFlagState] = None

        def __enter__(self) -> FeatureFlags.override:
            """Apply overrides."""
            self._previous_state = FeatureFlags.get_state()
            FeatureFlags.set_state(**self._overrides)
            return self

        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
            """Restore previous state."""
            if self._previous_state is not None:
                FeatureFlags.set_state(
                    enable_direct_execution=self._previous_state.enable_direct_execution,
                    execution_mode=self._previous_state.execution_mode,
                    rollout_phase=self._previous_state.rollout_phase,
                    suppress_python_files=self._previous_state.suppress_python_files,
                    enable_sql_validation=self._previous_state.enable_sql_validation,
                    validation_log_level=self._previous_state.validation_log_level,
                )
            return False


class ExecutionRouter:
    """
    Routes model execution to appropriate paths based on feature flags.

    This class determines which execution path(s) to use and coordinates
    parallel execution when validation mode is enabled.
    """

    @staticmethod
    def should_execute_legacy() -> bool:
        """
        Check if legacy Python generation should execute.

        Returns:
            True if legacy path should run
        """
        mode = FeatureFlags.get_execution_mode()
        return mode in (ExecutionMode.LEGACY, ExecutionMode.PARALLEL)

    @staticmethod
    def should_execute_direct() -> bool:
        """
        Check if direct Ibis execution should run.

        Returns:
            True if direct Ibis path should run
        """
        mode = FeatureFlags.get_execution_mode()
        return mode in (ExecutionMode.DIRECT, ExecutionMode.PARALLEL)

    @staticmethod
    def is_parallel_validation_mode() -> bool:
        """
        Check if both paths should execute for validation.

        Returns:
            True if parallel execution mode is active
        """
        return FeatureFlags.get_execution_mode() == ExecutionMode.PARALLEL

    @staticmethod
    def should_write_python_files() -> bool:
        """
        Check if Python files should be written.

        Returns:
            True if Python file generation is enabled
        """
        if FeatureFlags.should_suppress_python_files():
            return False
        return ExecutionRouter.should_execute_legacy()


@dataclass
class ExecutionContext:
    """
    Context for tracking execution path and metadata.

    Tracks which execution path was used for each model and
    preserves metadata throughout the execution pipeline.
    """

    model_name: str
    execution_path: str = "legacy"  # "legacy" or "direct"
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    legacy_sql: Optional[str] = None
    direct_sql: Optional[str] = None
    execution_mode: ExecutionMode = ExecutionMode.LEGACY
    rollout_phase: RolloutPhase = RolloutPhase.PHASE_1_VALIDATION
    model_metadata: dict[str, Any] = field(default_factory=dict)

    def mark_completed(self) -> None:
        """Mark execution as completed."""
        self.completed_at = datetime.utcnow()

    def set_legacy_sql(self, sql: str) -> None:
        """Set the SQL from legacy path."""
        self.legacy_sql = sql

    def set_direct_sql(self, sql: str) -> None:
        """Set the SQL from direct path."""
        self.direct_sql = sql

    @property
    def duration_ms(self) -> Optional[float]:
        """Get execution duration in milliseconds."""
        if self.completed_at is None:
            return None
        delta = self.completed_at - self.started_at
        return delta.total_seconds() * 1000

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "model_name": self.model_name,
            "execution_path": self.execution_path,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_ms": self.duration_ms,
            "execution_mode": self.execution_mode.value,
            "rollout_phase": self.rollout_phase.value,
            "has_legacy_sql": self.legacy_sql is not None,
            "has_direct_sql": self.direct_sql is not None,
        }


class ExecutionContextManager:
    """
    Manages execution contexts for model executions.

    Provides context creation and tracking for each model execution,
    supporting parallel validation mode.
    """

    def __init__(self) -> None:
        """Initialize the context manager."""
        self._contexts: dict[str, ExecutionContext] = {}
        self._lock = threading.Lock()

    def create_context(
        self,
        model_name: str,
        model_metadata: Optional[dict[str, Any]] = None,
    ) -> ExecutionContext:
        """
        Create a new execution context for a model.

        Args:
            model_name: Name of the model
            model_metadata: Optional metadata about the model

        Returns:
            ExecutionContext instance
        """
        context = ExecutionContext(
            model_name=model_name,
            execution_mode=FeatureFlags.get_execution_mode(),
            rollout_phase=FeatureFlags.get_rollout_phase(),
            model_metadata=model_metadata or {},
        )

        with self._lock:
            self._contexts[model_name] = context

        return context

    def get_context(self, model_name: str) -> Optional[ExecutionContext]:
        """Get context for a model."""
        with self._lock:
            return self._contexts.get(model_name)

    def complete_context(
        self,
        model_name: str,
        execution_path: str = "legacy",
    ) -> Optional[ExecutionContext]:
        """
        Mark a context as completed.

        Args:
            model_name: Name of the model
            execution_path: Which path was used ("legacy" or "direct")

        Returns:
            The completed context or None
        """
        with self._lock:
            context = self._contexts.get(model_name)
            if context:
                context.execution_path = execution_path
                context.mark_completed()
            return context

    def clear(self) -> None:
        """Clear all contexts."""
        with self._lock:
            self._contexts.clear()

    def get_all_contexts(self) -> list[ExecutionContext]:
        """Get all contexts."""
        with self._lock:
            return list(self._contexts.values())


# Global context manager
_context_manager: Optional[ExecutionContextManager] = None


def get_context_manager() -> ExecutionContextManager:
    """Get the global execution context manager."""
    global _context_manager
    if _context_manager is None:
        _context_manager = ExecutionContextManager()
    return _context_manager


def reset_context_manager() -> None:
    """Reset the global context manager (for testing)."""
    global _context_manager
    _context_manager = None


def get_feature_flags() -> FeatureFlags:
    """
    Get the global FeatureFlags singleton instance.

    Returns:
        The FeatureFlags singleton
    """
    return FeatureFlags()
