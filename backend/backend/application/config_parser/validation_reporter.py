"""
Validation Reporter for SQL Equivalence Analysis.

This module provides reporting capabilities for SQL validation results,
generating detailed reports on equivalence between legacy and direct
execution paths.

Usage:
    reporter = ValidationReporter(storage_service)
    report = reporter.generate_report(execution_id)
    print(report.to_markdown())
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from backend.application.config_parser.sql_validator import (
    ValidationResult,
    ValidationResultStore,
)
from backend.application.config_parser.validation_storage_service import (
    ValidationStorageService,
    get_validation_storage_service,
)

logger = logging.getLogger(__name__)


@dataclass
class ModelValidationDetail:
    """
    Detailed validation information for a single model.

    Attributes:
        model_name: Name of the model
        match_status: Whether SQL matched
        match_type: Type of match (exact, normalized, no_match)
        legacy_sql: SQL from legacy path
        direct_sql: SQL from direct path
        diff: Diff output if mismatch
        execution_time_legacy_ms: Legacy path execution time
        execution_time_direct_ms: Direct path execution time
    """

    model_name: str
    match_status: bool
    match_type: str = "no_match"
    legacy_sql: str = ""
    direct_sql: str = ""
    diff: str = ""
    execution_time_legacy_ms: Optional[float] = None
    execution_time_direct_ms: Optional[float] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "model_name": self.model_name,
            "match_status": self.match_status,
            "match_type": self.match_type,
            "has_diff": bool(self.diff),
            "execution_time_legacy_ms": self.execution_time_legacy_ms,
            "execution_time_direct_ms": self.execution_time_direct_ms,
        }


@dataclass
class ValidationReport:
    """
    Comprehensive validation report for an execution run.

    Attributes:
        execution_id: Unique identifier for this execution
        generated_at: When the report was generated
        total_models: Total number of models validated
        equivalent_count: Models with equivalent SQL
        non_equivalent_count: Models with non-equivalent SQL
        skipped_count: Models that were skipped
        equivalence_rate: Percentage of equivalent models
        model_details: Detailed results per model
        non_equivalent_models: List of models with mismatches
        execution_mode: The execution mode used
        total_duration_ms: Total execution time
        performance_summary: Performance comparison data
    """

    execution_id: str
    generated_at: datetime = field(default_factory=datetime.utcnow)
    total_models: int = 0
    equivalent_count: int = 0
    non_equivalent_count: int = 0
    skipped_count: int = 0
    equivalence_rate: float = 0.0
    model_details: list[ModelValidationDetail] = field(default_factory=list)
    non_equivalent_models: list[str] = field(default_factory=list)
    execution_mode: str = "parallel"
    total_duration_ms: float = 0.0
    performance_summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "execution_id": self.execution_id,
            "generated_at": self.generated_at.isoformat(),
            "summary": {
                "total_models": self.total_models,
                "equivalent_count": self.equivalent_count,
                "non_equivalent_count": self.non_equivalent_count,
                "skipped_count": self.skipped_count,
                "equivalence_rate": self.equivalence_rate,
            },
            "non_equivalent_models": self.non_equivalent_models,
            "execution_mode": self.execution_mode,
            "total_duration_ms": self.total_duration_ms,
            "performance_summary": self.performance_summary,
            "model_details": [d.to_dict() for d in self.model_details],
        }

    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    def to_markdown(self) -> str:
        """
        Generate a Markdown-formatted report.

        Returns:
            Markdown string representation of the report
        """
        lines = [
            f"# SQL Validation Report",
            f"",
            f"**Execution ID:** `{self.execution_id}`",
            f"**Generated At:** {self.generated_at.strftime('%Y-%m-%d %H:%M:%S UTC')}",
            f"**Execution Mode:** {self.execution_mode}",
            f"",
            f"## Summary",
            f"",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Total Models | {self.total_models} |",
            f"| Equivalent | {self.equivalent_count} |",
            f"| Non-Equivalent | {self.non_equivalent_count} |",
            f"| Skipped | {self.skipped_count} |",
            f"| **Equivalence Rate** | **{self.equivalence_rate:.1%}** |",
            f"",
        ]

        # Performance summary
        if self.performance_summary:
            lines.extend([
                f"## Performance",
                f"",
                f"| Metric | Value |",
                f"|--------|-------|",
            ])
            if "total_legacy_ms" in self.performance_summary:
                lines.append(
                    f"| Legacy Total | {self.performance_summary['total_legacy_ms']:.2f} ms |"
                )
            if "total_direct_ms" in self.performance_summary:
                lines.append(
                    f"| Direct Total | {self.performance_summary['total_direct_ms']:.2f} ms |"
                )
            if "speedup_factor" in self.performance_summary:
                lines.append(
                    f"| Speedup Factor | {self.performance_summary['speedup_factor']:.2f}x |"
                )
            lines.append("")

        # Non-equivalent models
        if self.non_equivalent_models:
            lines.extend([
                f"## Non-Equivalent Models ({len(self.non_equivalent_models)})",
                f"",
            ])
            for model in self.non_equivalent_models:
                lines.append(f"- `{model}`")
            lines.append("")

            # Detailed diffs for non-equivalent models
            lines.extend([
                f"## Detailed Discrepancies",
                f"",
            ])
            for detail in self.model_details:
                if not detail.match_status and detail.diff:
                    lines.extend([
                        f"### {detail.model_name}",
                        f"",
                        f"**Match Type:** {detail.match_type}",
                        f"",
                        f"```diff",
                        detail.diff,
                        f"```",
                        f"",
                    ])

        # Success message if all equivalent
        if self.equivalence_rate == 1.0:
            lines.extend([
                f"## Result: SUCCESS",
                f"",
                f"All {self.total_models} models produced equivalent SQL output.",
            ])

        return "\n".join(lines)

    def to_console(self) -> str:
        """
        Generate a console-friendly report.

        Returns:
            Formatted string for console output
        """
        status = "PASS" if self.equivalence_rate == 1.0 else "FAIL"
        status_icon = "✓" if status == "PASS" else "✗"

        lines = [
            f"",
            f"{'=' * 60}",
            f"  SQL VALIDATION REPORT - {status_icon} {status}",
            f"{'=' * 60}",
            f"",
            f"  Execution ID: {self.execution_id[:8]}...",
            f"  Mode: {self.execution_mode}",
            f"",
            f"  SUMMARY",
            f"  -------",
            f"  Total Models:    {self.total_models}",
            f"  Equivalent:      {self.equivalent_count}",
            f"  Non-Equivalent:  {self.non_equivalent_count}",
            f"  Skipped:         {self.skipped_count}",
            f"  Equivalence:     {self.equivalence_rate:.1%}",
            f"",
        ]

        if self.performance_summary:
            lines.extend([
                f"  PERFORMANCE",
                f"  -----------",
            ])
            if "speedup_factor" in self.performance_summary:
                lines.append(
                    f"  Speedup:         {self.performance_summary['speedup_factor']:.2f}x"
                )

        if self.non_equivalent_models:
            lines.extend([
                f"",
                f"  NON-EQUIVALENT MODELS",
                f"  ---------------------",
            ])
            for model in self.non_equivalent_models[:10]:  # Limit to 10
                lines.append(f"  - {model}")
            if len(self.non_equivalent_models) > 10:
                lines.append(
                    f"  ... and {len(self.non_equivalent_models) - 10} more"
                )

        lines.extend([
            f"",
            f"{'=' * 60}",
        ])

        return "\n".join(lines)


class ValidationReporter:
    """
    Generates validation reports from stored results.

    Provides multiple output formats: JSON, Markdown, and console.
    """

    def __init__(
        self,
        storage_service: Optional[ValidationStorageService] = None,
    ) -> None:
        """
        Initialize the reporter.

        Args:
            storage_service: Storage service to read results from
        """
        self._storage_service = storage_service

    @property
    def storage_service(self) -> ValidationStorageService:
        """Get the storage service, creating default if needed."""
        if self._storage_service is None:
            self._storage_service = get_validation_storage_service(
                persist_to_db=False
            )
        return self._storage_service

    def generate_report(
        self,
        execution_id: str,
        include_details: bool = True,
        include_sql: bool = False,
    ) -> ValidationReport:
        """
        Generate a validation report for an execution.

        Args:
            execution_id: The execution ID to report on
            include_details: Include per-model details
            include_sql: Include full SQL in details

        Returns:
            ValidationReport instance
        """
        # Get results from in-memory store
        store = self.storage_service.in_memory_store
        all_results = store.get_all()

        # Filter by execution context if we have execution_id
        # For in-memory store, we use all results
        results = all_results

        # Calculate metrics
        total = len(results)
        equivalent = sum(1 for r in results if r.match_status)
        non_equivalent = total - equivalent

        # Build model details
        model_details = []
        non_equivalent_models = []

        for result in results:
            detail = ModelValidationDetail(
                model_name=result.model_name,
                match_status=result.match_status,
                match_type=(
                    "exact" if "Exact" in result.discrepancy_details
                    else "normalized" if result.match_status
                    else "no_match"
                ),
                diff=result.discrepancy_details if not result.match_status else "",
            )

            if include_sql:
                detail.legacy_sql = result.legacy_sql
                detail.direct_sql = result.direct_sql

            if include_details:
                model_details.append(detail)

            if not result.match_status:
                non_equivalent_models.append(result.model_name)

        report = ValidationReport(
            execution_id=execution_id,
            total_models=total,
            equivalent_count=equivalent,
            non_equivalent_count=non_equivalent,
            equivalence_rate=equivalent / total if total > 0 else 1.0,
            model_details=model_details,
            non_equivalent_models=non_equivalent_models,
        )

        return report

    def generate_report_from_results(
        self,
        results: list[ValidationResult],
        execution_id: str = "manual",
        performance_data: Optional[dict[str, Any]] = None,
    ) -> ValidationReport:
        """
        Generate a report from a list of ValidationResults.

        Args:
            results: List of validation results
            execution_id: ID for this execution
            performance_data: Optional performance metrics

        Returns:
            ValidationReport instance
        """
        total = len(results)
        equivalent = sum(1 for r in results if r.match_status)
        non_equivalent = total - equivalent

        model_details = []
        non_equivalent_models = []

        for result in results:
            detail = ModelValidationDetail(
                model_name=result.model_name,
                match_status=result.match_status,
                match_type=(
                    "exact" if "Exact" in result.discrepancy_details
                    else "normalized" if result.match_status
                    else "no_match"
                ),
                diff=result.discrepancy_details if not result.match_status else "",
            )
            model_details.append(detail)

            if not result.match_status:
                non_equivalent_models.append(result.model_name)

        report = ValidationReport(
            execution_id=execution_id,
            total_models=total,
            equivalent_count=equivalent,
            non_equivalent_count=non_equivalent,
            equivalence_rate=equivalent / total if total > 0 else 1.0,
            model_details=model_details,
            non_equivalent_models=non_equivalent_models,
            performance_summary=performance_data or {},
        )

        return report

    def print_report(
        self,
        execution_id: str,
        format: str = "console",
    ) -> None:
        """
        Print a report to stdout.

        Args:
            execution_id: The execution ID to report on
            format: Output format (console, markdown, json)
        """
        report = self.generate_report(execution_id)

        if format == "markdown":
            print(report.to_markdown())
        elif format == "json":
            print(report.to_json())
        else:
            print(report.to_console())

    def save_report(
        self,
        execution_id: str,
        output_path: str,
        format: str = "markdown",
    ) -> None:
        """
        Save a report to a file.

        Args:
            execution_id: The execution ID to report on
            output_path: Path to save the report
            format: Output format (markdown, json)
        """
        report = self.generate_report(execution_id)

        content = (
            report.to_markdown() if format == "markdown"
            else report.to_json()
        )

        with open(output_path, "w") as f:
            f.write(content)

        logger.info(f"Report saved to {output_path}")


class YAMLErrorMapper:
    """
    Maps error locations from generated code back to YAML source.

    Uses ConfigParser's line tracking to provide user-friendly
    error messages that reference original YAML files.
    """

    def __init__(self) -> None:
        """Initialize the error mapper."""
        self._source_map: dict[str, dict[str, tuple[str, int]]] = {}

    def register_source(
        self,
        model_name: str,
        element_id: str,
        yaml_file: str,
        line_number: int,
    ) -> None:
        """
        Register a source mapping.

        Args:
            model_name: Name of the model
            element_id: Identifier for the element (column, transform, etc.)
            yaml_file: Path to the YAML file
            line_number: Line number in the YAML file
        """
        if model_name not in self._source_map:
            self._source_map[model_name] = {}
        self._source_map[model_name][element_id] = (yaml_file, line_number)

    def get_source_location(
        self,
        model_name: str,
        element_id: str,
    ) -> Optional[tuple[str, int]]:
        """
        Get the source location for an element.

        Args:
            model_name: Name of the model
            element_id: Identifier for the element

        Returns:
            Tuple of (yaml_file, line_number) or None
        """
        model_map = self._source_map.get(model_name, {})
        return model_map.get(element_id)

    def format_error_location(
        self,
        model_name: str,
        element_id: str,
        error_message: str,
    ) -> str:
        """
        Format an error message with YAML source location.

        Args:
            model_name: Name of the model
            element_id: Identifier for the element
            error_message: The error message

        Returns:
            Formatted error message with source location
        """
        location = self.get_source_location(model_name, element_id)

        if location:
            yaml_file, line_number = location
            return f"{yaml_file}:{line_number}: {error_message}"
        else:
            return f"{model_name}/{element_id}: {error_message}"

    def map_traceback(
        self,
        traceback_str: str,
        model_name: str,
    ) -> str:
        """
        Map a traceback to YAML source locations where possible.

        Args:
            traceback_str: The original traceback string
            model_name: Name of the model being executed

        Returns:
            Traceback with YAML locations added
        """
        # For now, just add a header with the model name
        # Future: parse traceback and map specific lines
        header = f"Error in model '{model_name}':\n"

        model_map = self._source_map.get(model_name, {})
        if model_map:
            # Add hint about YAML source
            first_entry = next(iter(model_map.values()))
            header += f"  Source: {first_entry[0]}\n\n"

        return header + traceback_str

    def clear(self) -> None:
        """Clear all source mappings."""
        self._source_map.clear()


# Global instances
_reporter: Optional[ValidationReporter] = None
_error_mapper: Optional[YAMLErrorMapper] = None


def get_validation_reporter(
    storage_service: Optional[ValidationStorageService] = None,
) -> ValidationReporter:
    """
    Get the global validation reporter.

    Args:
        storage_service: Optional storage service

    Returns:
        ValidationReporter instance
    """
    global _reporter

    if _reporter is None:
        _reporter = ValidationReporter(storage_service)

    return _reporter


def get_error_mapper() -> YAMLErrorMapper:
    """Get the global YAML error mapper."""
    global _error_mapper

    if _error_mapper is None:
        _error_mapper = YAMLErrorMapper()

    return _error_mapper


def reset_reporter() -> None:
    """Reset the global reporter (for testing)."""
    global _reporter
    _reporter = None


def reset_error_mapper() -> None:
    """Reset the global error mapper (for testing)."""
    global _error_mapper
    _error_mapper = None
