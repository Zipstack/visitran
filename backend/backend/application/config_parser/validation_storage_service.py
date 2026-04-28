"""
Validation Storage Service for Database Persistence.

This module provides a service layer that bridges the in-memory ValidationResultStore
with database persistence using Django models. It supports both synchronous and
asynchronous storage operations.

Usage:
    service = ValidationStorageService()

    # Store a validation result
    service.store_result(validation_result)

    # Get summary for an execution
    summary = service.get_execution_summary(execution_id)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Optional

from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from backend.application.config_parser.sql_validator import (
    ValidationResult,
    ValidationResultStore,
)

logger = logging.getLogger(__name__)


class ValidationStorageService:
    """
    Service for persisting validation results to the database.

    Provides a bridge between the in-memory ValidationResultStore and
    the Django ORM models for persistent storage.
    """

    def __init__(
        self,
        persist_to_db: bool = True,
        in_memory_store: Optional[ValidationResultStore] = None,
    ) -> None:
        """
        Initialize the validation storage service.

        Args:
            persist_to_db: Whether to persist results to the database
            in_memory_store: Optional in-memory store for caching
        """
        self.persist_to_db = persist_to_db
        self._in_memory_store = in_memory_store or ValidationResultStore()

    @property
    def in_memory_store(self) -> ValidationResultStore:
        """Get the in-memory store."""
        return self._in_memory_store

    def store_result(
        self,
        result: ValidationResult,
        execution_id: Optional[str] = None,
        execution_mode: str = "parallel",
        legacy_execution_ms: Optional[float] = None,
        direct_execution_ms: Optional[float] = None,
    ) -> str:
        """
        Store a validation result.

        Args:
            result: The validation result to store
            execution_id: Optional execution ID to group results
            execution_mode: Execution mode (legacy, direct, parallel)
            legacy_execution_ms: Legacy path execution time
            direct_execution_ms: Direct path execution time

        Returns:
            The validation_id of the stored result
        """
        # Store in memory first
        self._in_memory_store.store(result)

        if not self.persist_to_db:
            return result.execution_id

        # Import here to avoid circular imports and Django setup issues
        from backend.core.models.validation_models import SQLValidationResult

        # Parse schema from model name if present
        schema_name = ""
        model_name = result.model_name
        if "." in model_name:
            parts = model_name.split(".", 1)
            schema_name = parts[0]
            model_name = parts[1] if len(parts) > 1 else model_name

        # Determine match type
        match_type = "no_match"
        if result.match_status:
            if "Exact match" in result.discrepancy_details:
                match_type = "exact"
            else:
                match_type = "normalized"

        try:
            db_result = SQLValidationResult.objects.create(
                model_name=result.model_name,
                schema_name=schema_name,
                legacy_sql=result.legacy_sql,
                direct_sql=result.direct_sql,
                match_status=result.match_status,
                match_type=match_type,
                diff_output=result.discrepancy_details if not result.match_status else "",
                execution_id=execution_id or "",
                execution_mode=execution_mode,
                validated_at=result.execution_timestamp,
                legacy_execution_ms=legacy_execution_ms,
                direct_execution_ms=direct_execution_ms,
            )

            logger.debug(f"Stored validation result: {db_result.validation_id}")
            return str(db_result.validation_id)

        except Exception as e:
            logger.error(f"Failed to store validation result: {e}")
            # Return the in-memory ID even if DB storage fails
            return result.execution_id

    def store_results_batch(
        self,
        results: list[ValidationResult],
        execution_id: str,
        execution_mode: str = "parallel",
    ) -> int:
        """
        Store multiple validation results in a batch.

        Args:
            results: List of validation results
            execution_id: Execution ID to group results
            execution_mode: Execution mode

        Returns:
            Number of results stored successfully
        """
        stored_count = 0

        for result in results:
            try:
                self.store_result(
                    result=result,
                    execution_id=execution_id,
                    execution_mode=execution_mode,
                )
                stored_count += 1
            except Exception as e:
                logger.error(f"Failed to store result for {result.model_name}: {e}")

        return stored_count

    def create_summary(
        self,
        execution_id: str,
        execution_mode: str = "parallel",
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
    ) -> dict[str, Any]:
        """
        Create and store a summary for an execution run.

        Args:
            execution_id: The execution ID
            execution_mode: The execution mode used
            started_at: When execution started
            completed_at: When execution completed

        Returns:
            Summary dictionary
        """
        if not self.persist_to_db:
            # Return in-memory summary
            return self._in_memory_store.get_summary()

        from backend.core.models.validation_models import (
            SQLValidationResult,
            ValidationSummary,
        )

        # Get results for this execution
        results = SQLValidationResult.objects.filter(execution_id=execution_id)

        total_models = results.count()
        matched_count = results.filter(match_status=True).count()
        mismatched_count = results.filter(match_status=False).count()

        # Calculate timing totals
        total_legacy_ms = sum(
            r.legacy_execution_ms or 0
            for r in results.only("legacy_execution_ms")
        )
        total_direct_ms = sum(
            r.direct_execution_ms or 0
            for r in results.only("direct_execution_ms")
        )

        # Calculate match rate
        match_rate = matched_count / total_models if total_models > 0 else 0.0

        # Calculate speedup
        speedup_factor = (
            total_legacy_ms / total_direct_ms
            if total_direct_ms > 0 else 1.0
        )

        # Get list of mismatched models
        mismatched_models = list(
            results.filter(match_status=False).values_list("model_name", flat=True)
        )

        try:
            summary, created = ValidationSummary.objects.update_or_create(
                execution_id=execution_id,
                defaults={
                    "total_models": total_models,
                    "matched_count": matched_count,
                    "mismatched_count": mismatched_count,
                    "match_rate": match_rate,
                    "total_legacy_ms": total_legacy_ms,
                    "total_direct_ms": total_direct_ms,
                    "speedup_factor": speedup_factor,
                    "execution_mode": execution_mode,
                    "started_at": started_at,
                    "completed_at": completed_at or timezone.now(),
                    "mismatched_models": mismatched_models,
                },
            )

            logger.info(
                f"Created summary for {execution_id}: "
                f"{match_rate:.1%} match rate ({matched_count}/{total_models})"
            )

            return {
                "summary_id": str(summary.summary_id),
                "execution_id": execution_id,
                "total_models": total_models,
                "matched_count": matched_count,
                "mismatched_count": mismatched_count,
                "match_rate": match_rate,
                "speedup_factor": speedup_factor,
                "mismatched_models": mismatched_models,
            }

        except Exception as e:
            logger.error(f"Failed to create summary: {e}")
            return self._in_memory_store.get_summary()

    def get_execution_results(
        self,
        execution_id: str,
    ) -> list[dict[str, Any]]:
        """
        Get all validation results for an execution.

        Args:
            execution_id: The execution ID

        Returns:
            List of result dictionaries
        """
        if not self.persist_to_db:
            return [r.to_dict() for r in self._in_memory_store.get_all()]

        from backend.core.models.validation_models import SQLValidationResult

        results = SQLValidationResult.objects.filter(execution_id=execution_id)

        return [
            {
                "validation_id": str(r.validation_id),
                "model_name": r.model_name,
                "match_status": r.match_status,
                "match_type": r.match_type,
                "diff_output": r.diff_output,
                "validated_at": r.validated_at.isoformat(),
            }
            for r in results
        ]

    def get_discrepancies(
        self,
        execution_id: Optional[str] = None,
        model_name: Optional[str] = None,
        since: Optional[datetime] = None,
    ) -> list[dict[str, Any]]:
        """
        Get validation discrepancies with optional filters.

        Args:
            execution_id: Filter by execution ID
            model_name: Filter by model name
            since: Filter to results after this time

        Returns:
            List of discrepancy dictionaries
        """
        if not self.persist_to_db:
            discrepancies = self._in_memory_store.get_discrepancies()
            return [r.to_dict() for r in discrepancies]

        from backend.core.models.validation_models import SQLValidationResult

        queryset = SQLValidationResult.objects.filter(match_status=False)

        if execution_id:
            queryset = queryset.filter(execution_id=execution_id)
        if model_name:
            queryset = queryset.filter(model_name__icontains=model_name)
        if since:
            queryset = queryset.filter(validated_at__gte=since)

        return [
            {
                "validation_id": str(r.validation_id),
                "model_name": r.model_name,
                "execution_id": r.execution_id,
                "legacy_sql": r.legacy_sql,
                "direct_sql": r.direct_sql,
                "diff_output": r.diff_output,
                "validated_at": r.validated_at.isoformat(),
            }
            for r in queryset.order_by("-validated_at")[:100]  # Limit to 100
        ]

    def record_fallback_event(
        self,
        model_name: str,
        failure_reason: str,
        execution_id: str = "",
        error_type: str = "",
        error_traceback: str = "",
        fallback_succeeded: bool = True,
        direct_execution_ms: Optional[float] = None,
        fallback_execution_ms: Optional[float] = None,
    ) -> Optional[str]:
        """
        Record a fallback event when direct execution fails.

        Args:
            model_name: The model that failed
            failure_reason: Why direct execution failed
            execution_id: The execution ID
            error_type: Type of error
            error_traceback: Full traceback
            fallback_succeeded: Whether legacy fallback worked
            direct_execution_ms: Time spent on direct execution
            fallback_execution_ms: Time spent on fallback

        Returns:
            Event ID if stored, None otherwise
        """
        if not self.persist_to_db:
            logger.warning(
                f"Fallback event for {model_name}: {failure_reason} "
                f"(not persisted to DB)"
            )
            return None

        from backend.core.models.validation_models import FallbackEvent

        try:
            event = FallbackEvent.objects.create(
                model_name=model_name,
                execution_id=execution_id,
                failure_reason=failure_reason,
                error_type=error_type,
                error_traceback=error_traceback,
                fallback_succeeded=fallback_succeeded,
                direct_execution_ms=direct_execution_ms,
                fallback_execution_ms=fallback_execution_ms,
            )

            logger.info(f"Recorded fallback event: {event.event_id}")
            return str(event.event_id)

        except Exception as e:
            logger.error(f"Failed to record fallback event: {e}")
            return None

    def check_model_allowlist(self, model_name: str) -> bool:
        """
        Check if a model is on the allowlist for direct execution.

        Args:
            model_name: The model name to check

        Returns:
            True if model is allowed for direct execution
        """
        if not self.persist_to_db:
            # Default to allowing if DB not available
            return True

        from backend.core.models.validation_models import ModelAllowlist

        try:
            # Check for exact match or wildcard match
            entries = ModelAllowlist.objects.filter(
                is_enabled=True
            ).order_by("-priority")

            for entry in entries:
                pattern = entry.model_name

                # Check for wildcard patterns
                if "*" in pattern:
                    # Convert wildcard to regex
                    import fnmatch
                    if fnmatch.fnmatch(model_name, pattern):
                        return True
                elif pattern == model_name:
                    return True

            # No match found - check if allowlist is empty (allow all)
            if not entries.exists():
                return True

            return False

        except Exception as e:
            logger.error(f"Failed to check allowlist: {e}")
            # Default to allowing if check fails
            return True

    def add_to_allowlist(
        self,
        model_name: str,
        notes: str = "",
        added_by: str = "",
        priority: int = 0,
    ) -> bool:
        """
        Add a model to the allowlist.

        Args:
            model_name: Model name or pattern
            notes: Optional notes
            added_by: Who added the entry
            priority: Matching priority

        Returns:
            True if added successfully
        """
        if not self.persist_to_db:
            return False

        from backend.core.models.validation_models import ModelAllowlist

        try:
            ModelAllowlist.objects.update_or_create(
                model_name=model_name,
                defaults={
                    "is_enabled": True,
                    "notes": notes,
                    "added_by": added_by,
                    "priority": priority,
                },
            )
            return True
        except Exception as e:
            logger.error(f"Failed to add to allowlist: {e}")
            return False

    def get_fallback_stats(
        self,
        since: Optional[datetime] = None,
    ) -> dict[str, Any]:
        """
        Get statistics about fallback events.

        Args:
            since: Filter to events after this time

        Returns:
            Dictionary with fallback statistics
        """
        if not self.persist_to_db:
            return {
                "total_fallbacks": 0,
                "successful_fallbacks": 0,
                "failed_fallbacks": 0,
                "top_error_types": [],
                "top_failing_models": [],
            }

        from backend.core.models.validation_models import FallbackEvent
        from django.db.models import Count

        queryset = FallbackEvent.objects.all()

        if since:
            queryset = queryset.filter(occurred_at__gte=since)

        total = queryset.count()
        successful = queryset.filter(fallback_succeeded=True).count()
        failed = total - successful

        # Get top error types
        top_errors = (
            queryset
            .exclude(error_type="")
            .values("error_type")
            .annotate(count=Count("error_type"))
            .order_by("-count")[:5]
        )

        # Get top failing models
        top_models = (
            queryset
            .values("model_name")
            .annotate(count=Count("model_name"))
            .order_by("-count")[:5]
        )

        return {
            "total_fallbacks": total,
            "successful_fallbacks": successful,
            "failed_fallbacks": failed,
            "fallback_success_rate": successful / total if total > 0 else 1.0,
            "top_error_types": list(top_errors),
            "top_failing_models": list(top_models),
        }

    def cleanup_old_results(
        self,
        older_than_days: int = 30,
    ) -> int:
        """
        Clean up old validation results.

        Args:
            older_than_days: Delete results older than this many days

        Returns:
            Number of records deleted
        """
        if not self.persist_to_db:
            return 0

        from backend.core.models.validation_models import (
            SQLValidationResult,
            FallbackEvent,
        )

        cutoff = timezone.now() - timedelta(days=older_than_days)

        deleted_count = 0

        with transaction.atomic():
            # Delete old validation results
            result_count, _ = SQLValidationResult.objects.filter(
                validated_at__lt=cutoff
            ).delete()
            deleted_count += result_count

            # Delete old fallback events
            event_count, _ = FallbackEvent.objects.filter(
                occurred_at__lt=cutoff
            ).delete()
            deleted_count += event_count

        logger.info(
            f"Cleaned up {deleted_count} old records "
            f"(older than {older_than_days} days)"
        )

        return deleted_count


# Global service instance
_service: Optional[ValidationStorageService] = None


def get_validation_storage_service(
    persist_to_db: bool = True,
) -> ValidationStorageService:
    """
    Get the global validation storage service.

    Args:
        persist_to_db: Whether to persist to database

    Returns:
        ValidationStorageService instance
    """
    global _service

    if _service is None:
        _service = ValidationStorageService(persist_to_db=persist_to_db)

    return _service


def reset_validation_storage_service() -> None:
    """Reset the global service (mainly for testing)."""
    global _service
    _service = None
