"""
SQL Validator for Comparing Legacy and Direct Execution SQL Outputs.

This module provides SQL equivalence checking to validate that the direct
Ibis execution path produces semantically equivalent SQL to the legacy
Python generation path.

Usage:
    validator = SQLValidator()
    result = validator.compare(legacy_sql, direct_sql)

    if not result.is_match:
        print(f"Discrepancy found: {result.diff}")
"""

from __future__ import annotations

import difflib
import hashlib
import logging
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class SQLNormalizationOptions:
    """
    Options for SQL normalization before comparison.

    Attributes:
        strip_whitespace: Normalize all whitespace to single spaces
        strip_comments: Remove SQL comments
        lowercase_keywords: Convert SQL keywords to lowercase
        normalize_aliases: Normalize table aliases (pending clarification)
        ignore_semicolons: Remove trailing semicolons
    """

    strip_whitespace: bool = True
    strip_comments: bool = True
    lowercase_keywords: bool = True
    normalize_aliases: bool = False  # Pending clarification
    ignore_semicolons: bool = True


@dataclass
class SQLComparisonResult:
    """
    Result of comparing two SQL strings.

    Attributes:
        is_match: True if SQLs are semantically equivalent
        legacy_sql: The normalized legacy SQL
        direct_sql: The normalized direct SQL
        diff: Unified diff showing differences (if any)
        match_type: Type of match (exact, normalized, no_match)
        execution_id: Unique identifier for this comparison
        timestamp: When comparison was performed
    """

    is_match: bool
    legacy_sql: str
    direct_sql: str
    diff: str = ""
    match_type: str = "no_match"  # exact, normalized, no_match
    execution_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary for storage."""
        return {
            "execution_id": self.execution_id,
            "is_match": self.is_match,
            "match_type": self.match_type,
            "legacy_sql": self.legacy_sql,
            "direct_sql": self.direct_sql,
            "diff": self.diff,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class ValidationResult:
    """
    Complete validation result for a model execution.

    Attributes:
        execution_id: Unique identifier
        model_name: Name of the model being validated
        legacy_sql: SQL from legacy Python generation
        direct_sql: SQL from direct Ibis execution
        match_status: Whether SQLs match
        discrepancy_details: Details about any differences
        execution_timestamp: When validation occurred
    """

    execution_id: str
    model_name: str
    legacy_sql: str
    direct_sql: str
    match_status: bool
    discrepancy_details: str = ""
    execution_timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for database storage."""
        return {
            "execution_id": self.execution_id,
            "model_name": self.model_name,
            "legacy_sql": self.legacy_sql,
            "direct_sql": self.direct_sql,
            "match_status": self.match_status,
            "discrepancy_details": self.discrepancy_details,
            "execution_timestamp": self.execution_timestamp.isoformat(),
        }


class SQLNormalizer:
    """
    Normalizes SQL strings for semantic comparison.

    Applies transformations to make SQL comparison more robust against
    functionally equivalent but syntactically different SQL.
    """

    # SQL keywords for case normalization
    SQL_KEYWORDS = {
        "select", "from", "where", "and", "or", "not", "in", "like",
        "join", "left", "right", "inner", "outer", "full", "cross",
        "on", "as", "group", "by", "having", "order", "asc", "desc",
        "limit", "offset", "union", "except", "intersect", "distinct",
        "null", "is", "between", "case", "when", "then", "else", "end",
        "insert", "into", "values", "update", "set", "delete", "create",
        "table", "view", "index", "drop", "alter", "add", "column",
        "primary", "key", "foreign", "references", "constraint", "unique",
        "default", "check", "exists", "all", "any", "some", "true", "false",
    }

    def __init__(self, options: Optional[SQLNormalizationOptions] = None) -> None:
        """
        Initialize normalizer with options.

        Args:
            options: Normalization options (defaults used if None)
        """
        self.options = options or SQLNormalizationOptions()

    def normalize(self, sql: str) -> str:
        """
        Normalize SQL string for comparison.

        Args:
            sql: The SQL string to normalize

        Returns:
            Normalized SQL string
        """
        if not sql:
            return ""

        result = sql

        if self.options.strip_comments:
            result = self._strip_comments(result)

        if self.options.strip_whitespace:
            result = self._normalize_whitespace(result)

        if self.options.lowercase_keywords:
            result = self._lowercase_keywords(result)

        if self.options.ignore_semicolons:
            result = result.rstrip().rstrip(";")

        return result.strip()

    def _strip_comments(self, sql: str) -> str:
        """Remove SQL comments."""
        # Remove single-line comments (-- style)
        sql = re.sub(r"--[^\n]*", "", sql)
        # Remove multi-line comments (/* */ style)
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
        return sql

    def _normalize_whitespace(self, sql: str) -> str:
        """Normalize all whitespace to single spaces."""
        # Replace all whitespace sequences with single space
        return re.sub(r"\s+", " ", sql)

    def _lowercase_keywords(self, sql: str) -> str:
        """Convert SQL keywords to lowercase."""
        words = sql.split()
        result = []
        for word in words:
            # Check if word (without punctuation) is a keyword
            clean_word = re.sub(r"[^\w]", "", word)
            if clean_word.lower() in self.SQL_KEYWORDS:
                result.append(word.lower())
            else:
                result.append(word)
        return " ".join(result)

    def compute_hash(self, sql: str) -> str:
        """
        Compute hash of normalized SQL for quick comparison.

        Args:
            sql: SQL string to hash

        Returns:
            MD5 hash of normalized SQL
        """
        normalized = self.normalize(sql)
        return hashlib.md5(normalized.encode()).hexdigest()


class SQLValidator:
    """
    Validates SQL equivalence between legacy and direct execution paths.

    Compares SQL outputs from both paths and provides detailed
    discrepancy information when differences are found.
    """

    def __init__(
        self,
        normalizer: Optional[SQLNormalizer] = None,
        strict_mode: bool = False,
    ) -> None:
        """
        Initialize SQL validator.

        Args:
            normalizer: SQL normalizer (default created if None)
            strict_mode: If True, require exact match (no normalization)
        """
        self.normalizer = normalizer or SQLNormalizer()
        self.strict_mode = strict_mode

    def compare(
        self,
        legacy_sql: str,
        direct_sql: str,
        model_name: str = "unknown",
    ) -> ValidationResult:
        """
        Compare legacy and direct SQL outputs.

        Args:
            legacy_sql: SQL from legacy Python generation
            direct_sql: SQL from direct Ibis execution
            model_name: Name of the model being validated

        Returns:
            ValidationResult with match status and details
        """
        execution_id = str(uuid.uuid4())

        # Check for exact match first
        if legacy_sql == direct_sql:
            return ValidationResult(
                execution_id=execution_id,
                model_name=model_name,
                legacy_sql=legacy_sql,
                direct_sql=direct_sql,
                match_status=True,
                discrepancy_details="Exact match",
            )

        # If strict mode, no normalization
        if self.strict_mode:
            diff = self._generate_diff(legacy_sql, direct_sql)
            return ValidationResult(
                execution_id=execution_id,
                model_name=model_name,
                legacy_sql=legacy_sql,
                direct_sql=direct_sql,
                match_status=False,
                discrepancy_details=f"Strict mode: no exact match\n{diff}",
            )

        # Normalize and compare
        normalized_legacy = self.normalizer.normalize(legacy_sql)
        normalized_direct = self.normalizer.normalize(direct_sql)

        if normalized_legacy == normalized_direct:
            return ValidationResult(
                execution_id=execution_id,
                model_name=model_name,
                legacy_sql=legacy_sql,
                direct_sql=direct_sql,
                match_status=True,
                discrepancy_details="Normalized match (semantically equivalent)",
            )

        # No match - generate detailed diff
        diff = self._generate_diff(normalized_legacy, normalized_direct)

        return ValidationResult(
            execution_id=execution_id,
            model_name=model_name,
            legacy_sql=legacy_sql,
            direct_sql=direct_sql,
            match_status=False,
            discrepancy_details=diff,
        )

    def _generate_diff(self, sql1: str, sql2: str) -> str:
        """
        Generate unified diff between two SQL strings.

        Args:
            sql1: First SQL string
            sql2: Second SQL string

        Returns:
            Unified diff string
        """
        lines1 = sql1.splitlines(keepends=True)
        lines2 = sql2.splitlines(keepends=True)

        diff = difflib.unified_diff(
            lines1,
            lines2,
            fromfile="legacy_sql",
            tofile="direct_sql",
            lineterm="",
        )

        return "".join(diff)

    def log_discrepancy(
        self,
        result: ValidationResult,
        level: str = "warning",
    ) -> None:
        """
        Log a validation discrepancy.

        Args:
            result: The validation result to log
            level: Logging level (debug, info, warning, error)
        """
        if result.match_status:
            return

        message = (
            f"SQL discrepancy detected for model '{result.model_name}'\n"
            f"Execution ID: {result.execution_id}\n"
            f"Timestamp: {result.execution_timestamp.isoformat()}\n"
            f"\nLegacy SQL:\n{result.legacy_sql}\n"
            f"\nDirect SQL:\n{result.direct_sql}\n"
            f"\nDiff:\n{result.discrepancy_details}"
        )

        log_func = getattr(logger, level, logger.warning)
        log_func(message)


class ValidationResultStore:
    """
    In-memory store for validation results.

    For production use, this should be replaced with a database-backed
    implementation using Django models.
    """

    def __init__(self) -> None:
        """Initialize the store."""
        self._results: list[ValidationResult] = []

    def store(self, result: ValidationResult) -> None:
        """
        Store a validation result.

        Args:
            result: The validation result to store
        """
        self._results.append(result)

    def get_all(self) -> list[ValidationResult]:
        """Get all stored results."""
        return self._results.copy()

    def get_by_model(self, model_name: str) -> list[ValidationResult]:
        """Get results for a specific model."""
        return [r for r in self._results if r.model_name == model_name]

    def get_discrepancies(self) -> list[ValidationResult]:
        """Get only results with discrepancies."""
        return [r for r in self._results if not r.match_status]

    def get_summary(self) -> dict[str, Any]:
        """
        Get summary statistics.

        Returns:
            Dictionary with total, matches, discrepancies, match_rate
        """
        total = len(self._results)
        matches = sum(1 for r in self._results if r.match_status)
        discrepancies = total - matches

        return {
            "total_executions": total,
            "matches": matches,
            "discrepancies": discrepancies,
            "match_rate": matches / total if total > 0 else 1.0,
        }

    def clear(self) -> None:
        """Clear all stored results."""
        self._results.clear()


# Global validator instance
_validator: Optional[SQLValidator] = None
_store: Optional[ValidationResultStore] = None


def get_validator() -> SQLValidator:
    """Get the global SQL validator instance."""
    global _validator
    if _validator is None:
        _validator = SQLValidator()
    return _validator


def get_validation_store() -> ValidationResultStore:
    """Get the global validation result store."""
    global _store
    if _store is None:
        _store = ValidationResultStore()
    return _store


def validate_sql_equivalence(
    legacy_sql: str,
    direct_sql: str,
    model_name: str = "unknown",
    store_result: bool = True,
    log_discrepancy: bool = True,
) -> ValidationResult:
    """
    Convenience function to validate SQL equivalence.

    Args:
        legacy_sql: SQL from legacy path
        direct_sql: SQL from direct Ibis path
        model_name: Name of the model
        store_result: Whether to store the result
        log_discrepancy: Whether to log if discrepancy found

    Returns:
        ValidationResult
    """
    validator = get_validator()
    result = validator.compare(legacy_sql, direct_sql, model_name)

    if store_result:
        get_validation_store().store(result)

    if log_discrepancy and not result.match_status:
        validator.log_discrepancy(result)

    return result
