"""
YAML Loader with Database Integration

This module provides functionality to load transformation configurations from
various sources (database, files, raw strings) and parse them into ConfigParser
instances with complete source location tracking.

Usage:
    loader = YAMLConfigLoader()

    # Load from database model
    config_parser = loader.load_from_model(config_model_instance)

    # Load from YAML string
    config_parser = loader.load_from_string(yaml_content, model_name="my_model")

    # Load from file path
    config_parser = loader.load_from_file("/path/to/config.yaml")
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

import yaml

from backend.application.config_parser.config_parser import ConfigParser
from backend.application.config_parser.yaml_source_tracker import (
    YAMLSourceTracker,
    parse_yaml_with_locations,
)
from visitran.errors import TransformationError

if TYPE_CHECKING:
    from backend.core.models.config_models import ConfigModels

logger = logging.getLogger(__name__)


class YAMLParseError(Exception):
    """Exception raised when YAML parsing fails."""

    def __init__(
        self,
        message: str,
        line: Optional[int] = None,
        column: Optional[int] = None,
        yaml_content: Optional[str] = None,
    ):
        self.message = message
        self.line = line
        self.column = column
        self.yaml_content = yaml_content
        super().__init__(message)


class YAMLConfigLoader:
    """
    Loads transformation configurations from various sources with source tracking.

    This loader handles:
    - Loading from database ConfigModels
    - Loading from YAML strings
    - Loading from file paths
    - Multi-document YAML files
    - Wrapping parse errors in TransformationError with location context
    """

    def __init__(self) -> None:
        """Initialize the YAML config loader."""
        self._tracker = YAMLSourceTracker()

    def load_from_model(self, config_model: ConfigModels) -> ConfigParser:
        """
        Load configuration from a database ConfigModels instance.

        Args:
            config_model: The ConfigModels database instance

        Returns:
            ConfigParser instance with source location tracking

        Raises:
            TransformationError: If parsing fails with location context
        """
        model_name = config_model.model_name
        model_data = config_model.model_data

        # Clear singleton cache for this model name to ensure fresh instance
        if model_name in ConfigParser._instances:
            del ConfigParser._instances[model_name]

        # Create ConfigParser with the model data
        parser = ConfigParser(model_data, model_name)

        # Note: model_data is already parsed JSON, so we can't extract
        # YAML source locations from it. Source tracking requires raw YAML.
        # If the original YAML is needed, it should be stored separately.

        logger.debug(f"Loaded config from database model: {model_name}")
        return parser

    def load_from_string(
        self,
        yaml_content: str,
        model_name: str,
        *,
        strict: bool = True,
    ) -> ConfigParser:
        """
        Load configuration from a YAML string with source tracking.

        Args:
            yaml_content: The raw YAML content string
            model_name: Name to identify this model/configuration
            strict: If True, raise TransformationError on parse failures

        Returns:
            ConfigParser instance with populated source map

        Raises:
            TransformationError: If parsing fails and strict=True
        """
        # Clear singleton cache for this model name
        if model_name in ConfigParser._instances:
            del ConfigParser._instances[model_name]

        try:
            # Parse YAML with source location tracking
            model_data, source_map = parse_yaml_with_locations(yaml_content)

            if model_data is None:
                model_data = {}

            # Create ConfigParser
            parser = ConfigParser(model_data, model_name)

            # Set the YAML content for source tracking
            parser.set_yaml_content(yaml_content)

            # Also set source map directly (may have additional locations)
            if source_map:
                parser.set_source_map(source_map)

            logger.debug(
                f"Loaded config from string: {model_name}, "
                f"tracked {len(source_map)} locations"
            )
            return parser

        except yaml.YAMLError as e:
            error_info = self._extract_yaml_error_info(e, yaml_content)

            if strict:
                raise TransformationError(
                    model_name=model_name,
                    transformation_id="yaml_parse",
                    line_number=error_info["line"],
                    column_number=error_info["column"],
                    error_message=f"YAML parsing error: {error_info['message']}",
                    yaml_snippet=error_info["snippet"],
                    original_exception=e,
                    suggested_fix="Check YAML syntax - ensure proper indentation and valid structure",
                ) from e
            else:
                logger.warning(f"YAML parse error in {model_name}: {e}")
                # Return empty parser on non-strict mode
                parser = ConfigParser({}, model_name)
                return parser

    def load_from_file(
        self,
        file_path: Union[str, Path],
        model_name: Optional[str] = None,
        *,
        strict: bool = True,
    ) -> ConfigParser:
        """
        Load configuration from a YAML file with source tracking.

        Args:
            file_path: Path to the YAML configuration file
            model_name: Optional name for the model (defaults to filename)
            strict: If True, raise TransformationError on failures

        Returns:
            ConfigParser instance with populated source map

        Raises:
            TransformationError: If file reading or parsing fails
            FileNotFoundError: If the file doesn't exist
        """
        file_path = Path(file_path)

        if model_name is None:
            model_name = file_path.stem

        if not file_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {file_path}")

        try:
            yaml_content = file_path.read_text(encoding="utf-8")
        except IOError as e:
            if strict:
                raise TransformationError(
                    model_name=model_name,
                    transformation_id="file_read",
                    line_number=1,
                    column_number=1,
                    error_message=f"Failed to read configuration file: {e}",
                    yaml_snippet="",
                    original_exception=e,
                ) from e
            else:
                logger.error(f"Failed to read file {file_path}: {e}")
                raise

        return self.load_from_string(yaml_content, model_name, strict=strict)

    def load_multi_document(
        self,
        yaml_content: str,
        base_model_name: str = "document",
    ) -> list[ConfigParser]:
        """
        Load multiple YAML documents from a single string.

        Handles YAML files with multiple documents separated by '---'.

        Args:
            yaml_content: YAML content potentially containing multiple documents
            base_model_name: Base name for models (appended with index)

        Returns:
            List of ConfigParser instances, one per document
        """
        parsers = []

        try:
            # Use yaml.safe_load_all for multi-document parsing
            documents = list(yaml.safe_load_all(yaml_content))

            # Split content by document separator for source tracking
            doc_contents = self._split_yaml_documents(yaml_content)

            for idx, (doc_data, doc_content) in enumerate(
                zip(documents, doc_contents)
            ):
                if doc_data is None:
                    continue

                model_name = f"{base_model_name}_{idx}" if len(documents) > 1 else base_model_name

                # Clear singleton cache
                if model_name in ConfigParser._instances:
                    del ConfigParser._instances[model_name]

                parser = ConfigParser(doc_data, model_name)

                if doc_content:
                    parser.set_yaml_content(doc_content)

                parsers.append(parser)

        except yaml.YAMLError as e:
            error_info = self._extract_yaml_error_info(e, yaml_content)
            raise TransformationError(
                model_name=base_model_name,
                transformation_id="yaml_multi_parse",
                line_number=error_info["line"],
                column_number=error_info["column"],
                error_message=f"Multi-document YAML parsing error: {error_info['message']}",
                yaml_snippet=error_info["snippet"],
                original_exception=e,
            ) from e

        return parsers

    def _extract_yaml_error_info(
        self,
        error: yaml.YAMLError,
        yaml_content: str,
    ) -> dict[str, Any]:
        """
        Extract location and context information from a PyYAML error.

        Args:
            error: The PyYAML exception
            yaml_content: The original YAML content

        Returns:
            Dictionary with line, column, message, and snippet
        """
        line = 1
        column = 1
        message = str(error)

        # Extract position from PyYAML error if available
        if hasattr(error, "problem_mark") and error.problem_mark:
            mark = error.problem_mark
            line = mark.line + 1  # Convert to 1-based
            column = mark.column + 1
        elif hasattr(error, "context_mark") and error.context_mark:
            mark = error.context_mark
            line = mark.line + 1
            column = mark.column + 1

        # Extract snippet
        snippet = TransformationError.extract_yaml_snippet(
            yaml_content, line, column
        )

        return {
            "line": line,
            "column": column,
            "message": message,
            "snippet": snippet,
        }

    def _split_yaml_documents(self, yaml_content: str) -> list[str]:
        """
        Split YAML content into individual documents.

        Args:
            yaml_content: Multi-document YAML string

        Returns:
            List of individual document strings
        """
        documents = []
        current_doc = []

        for line in yaml_content.splitlines(keepends=True):
            if line.strip() == "---" and current_doc:
                documents.append("".join(current_doc))
                current_doc = []
            else:
                current_doc.append(line)

        if current_doc:
            documents.append("".join(current_doc))

        return documents


# Convenience functions for common use cases

def load_yaml_config(
    source: Union[str, Path, "ConfigModels"],
    model_name: Optional[str] = None,
) -> ConfigParser:
    """
    Convenience function to load a configuration from any source.

    Args:
        source: YAML string, file path, or ConfigModels instance
        model_name: Optional model name (required for string sources)

    Returns:
        ConfigParser instance with source tracking
    """
    loader = YAMLConfigLoader()

    # Check if it's a database model
    if hasattr(source, "model_data") and hasattr(source, "model_name"):
        return loader.load_from_model(source)

    # Check if it's a file path
    if isinstance(source, Path) or (isinstance(source, str) and "\n" not in source and len(source) < 500):
        path = Path(source)
        if path.exists() and path.is_file():
            return loader.load_from_file(path, model_name)

    # Treat as YAML string
    if model_name is None:
        model_name = "unnamed_model"

    return loader.load_from_string(str(source), model_name)
