"""Rollback dependency validation & impact analysis.

Before a rollback can proceed, this service traverses the model
dependency graph to find downstream dependents, compares the current
version against the rollback target, and generates a structured
impact report with severity classification.
"""

import logging
import time
from typing import Any

from backend.application.validate_references import ValidateReferences
from backend.core.models.config_models import ConfigModels
from backend.core.models.model_version import ModelVersion

logger = logging.getLogger(__name__)


def validate_rollback(
    config_model: ConfigModels,
    target_version: ModelVersion,
    model_dict: dict[str, set[str]],
) -> dict[str, Any]:
    """Run pre-flight validation for a rollback operation."""
    start = time.monotonic()
    model_name = config_model.model_name
    current_data = config_model.model_data or {}
    target_data = target_version.model_data or {}

    issues: list[dict[str, Any]] = []
    affected_models = _get_affected_models(model_name, model_dict)

    issues.extend(_detect_removed_transforms(model_name, current_data, target_data))
    issues.extend(_detect_output_field_changes(model_name, current_data, target_data))
    issues.extend(_detect_reference_changes(model_name, current_data, target_data))
    issues.extend(_detect_downstream_impact(model_name, current_data, target_data, affected_models, model_dict))

    critical_count = sum(1 for i in issues if i["severity"] == "critical")
    warning_count = sum(1 for i in issues if i["severity"] == "warning")

    return {
        "can_rollback": critical_count == 0,
        "requires_confirmation": warning_count > 0,
        "model_name": model_name,
        "target_version": target_version.version_number,
        "affected_models": sorted(affected_models),
        "affected_model_count": len(affected_models),
        "issues": issues,
        "issue_summary": {"critical": critical_count, "warning": warning_count, "total": len(issues)},
        "recommendations": _generate_recommendations(issues, affected_models),
        "validation_time_ms": round((time.monotonic() - start) * 1000, 2),
    }


def _get_affected_models(model_name: str, model_dict: dict[str, set[str]]) -> list[str]:
    if model_name not in model_dict:
        return []
    validator = ValidateReferences(model_dict=model_dict, model_name=model_name)
    return sorted(validator.get_child_references())


def _detect_removed_transforms(model_name: str, current_data: dict, target_data: dict) -> list[dict[str, Any]]:
    issues = []
    current_transforms = set((current_data.get("transform") or {}).keys())
    target_transforms = set((target_data.get("transform") or {}).keys())
    for tid in sorted(current_transforms - target_transforms):
        issues.append({
            "severity": "warning", "category": "removed_transformation",
            "model_name": model_name, "transformation_path": tid,
            "message": f"Transformation '{tid}' exists in the current version but will be removed after rollback.",
        })
    return issues


def _detect_output_field_changes(model_name: str, current_data: dict, target_data: dict) -> list[dict[str, Any]]:
    issues = []
    current_model = current_data.get("model", {})
    target_model = target_data.get("model", {})
    if current_model.get("table_name") != target_model.get("table_name"):
        issues.append({
            "severity": "critical", "category": "output_table_changed", "model_name": model_name,
            "current_value": current_model.get("table_name"), "target_value": target_model.get("table_name"),
            "message": f"Output table changes from '{current_model.get('table_name')}' to '{target_model.get('table_name')}'. Downstream models may break.",
        })
    if current_model.get("schema_name") != target_model.get("schema_name"):
        issues.append({
            "severity": "critical", "category": "output_schema_changed", "model_name": model_name,
            "current_value": current_model.get("schema_name"), "target_value": target_model.get("schema_name"),
            "message": f"Output schema changes from '{current_model.get('schema_name')}' to '{target_model.get('schema_name')}'. Downstream models may break.",
        })
    return issues


def _detect_reference_changes(model_name: str, current_data: dict, target_data: dict) -> list[dict[str, Any]]:
    issues = []
    current_refs = set(current_data.get("reference", []))
    target_refs = set(target_data.get("reference", []))
    for ref in sorted(current_refs - target_refs):
        issues.append({
            "severity": "warning", "category": "reference_removed", "model_name": model_name,
            "reference_model": ref, "message": f"Reference to '{ref}' will be removed after rollback.",
        })
    for ref in sorted(target_refs - current_refs):
        issues.append({
            "severity": "warning", "category": "reference_added", "model_name": model_name,
            "reference_model": ref, "message": f"Reference to '{ref}' will be restored after rollback.",
        })
    return issues


def _detect_downstream_impact(
    model_name: str, current_data: dict, target_data: dict,
    affected_models: list[str], model_dict: dict[str, set[str]],
) -> list[dict[str, Any]]:
    issues = []
    if not affected_models:
        return issues
    current_output = (current_data.get("model", {}).get("schema_name", ""), current_data.get("model", {}).get("table_name", ""))
    target_output = (target_data.get("model", {}).get("schema_name", ""), target_data.get("model", {}).get("table_name", ""))
    if current_output != target_output:
        for child in affected_models:
            issues.append({
                "severity": "critical", "category": "downstream_source_broken",
                "model_name": child, "depends_on": model_name,
                "message": f"Model '{child}' depends on '{model_name}' whose output table/schema will change after rollback.",
            })
    else:
        for child in affected_models:
            issues.append({
                "severity": "warning", "category": "downstream_may_be_affected",
                "model_name": child, "depends_on": model_name,
                "message": f"Model '{child}' depends on '{model_name}'. Verify compatibility after rollback.",
            })
    return issues


def _generate_recommendations(issues: list[dict[str, Any]], affected_models: list[str]) -> list[str]:
    recommendations = []
    categories = {i["category"] for i in issues}
    if "output_table_changed" in categories or "output_schema_changed" in categories:
        recommendations.append("Update downstream models to use the new output table/schema before rolling back.")
    if "removed_transformation" in categories:
        recommendations.append("Review removed transformations to ensure no downstream models depend on their output columns.")
    if "downstream_source_broken" in categories:
        recommendations.append("Consider rolling back dependent models in dependency order (leaf models first).")
    if affected_models and not any(i["category"].startswith("downstream_") for i in issues):
        recommendations.append(f"This model has {len(affected_models)} dependent model(s). Verify compatibility after rollback.")
    if not issues:
        recommendations.append("No issues detected. This rollback appears safe to proceed.")
    return recommendations
