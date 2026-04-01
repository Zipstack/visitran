"""Model data diff comparison engine.

Compares two model_data dictionaries and produces a structured diff
with change categorisation at the transformation level.
"""

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

CHANGE_ADDED = "added"
CHANGE_REMOVED = "removed"
CHANGE_MODIFIED = "modified"
CHANGE_REORDERED = "reordered"


def compare_model_data(
    old_data: dict[str, Any],
    new_data: dict[str, Any],
) -> dict[str, Any]:
    """Compare two model_data dicts and return a structured diff."""
    start = time.monotonic()
    changes: list[dict[str, Any]] = []

    _diff_source(old_data.get("source", {}), new_data.get("source", {}), changes)
    _diff_model(old_data.get("model", {}), new_data.get("model", {}), changes)
    _diff_presentation(old_data.get("presentation", {}), new_data.get("presentation", {}), changes)
    _diff_references(old_data.get("reference", []), new_data.get("reference", []), changes)
    _diff_transformations(
        old_transforms=old_data.get("transform", {}),
        new_transforms=new_data.get("transform", {}),
        old_order=old_data.get("transform_order", []),
        new_order=new_data.get("transform_order", []),
        changes=changes,
    )

    stats = {CHANGE_ADDED: 0, CHANGE_REMOVED: 0, CHANGE_MODIFIED: 0, CHANGE_REORDERED: 0}
    for change in changes:
        stats[change["change_type"]] = stats.get(change["change_type"], 0) + 1

    return {
        "changes": changes,
        "summary": _build_overall_summary(stats),
        "stats": stats,
        "total_changes": len(changes),
        "comparison_time_ms": round((time.monotonic() - start) * 1000, 2),
    }


def _diff_source(old_src: dict, new_src: dict, changes: list[dict]) -> None:
    if old_src == new_src:
        return
    for key in set(list(old_src.keys()) + list(new_src.keys())):
        old_val = old_src.get(key)
        new_val = new_src.get(key)
        if old_val != new_val:
            changes.append({
                "section": "source", "path": f"source.{key}",
                "change_type": CHANGE_MODIFIED, "old_value": old_val, "new_value": new_val,
                "summary": _field_change_summary("source", key, old_val, new_val),
            })


def _diff_model(old_model: dict, new_model: dict, changes: list[dict]) -> None:
    if old_model == new_model:
        return
    for key in set(list(old_model.keys()) + list(new_model.keys())):
        old_val = old_model.get(key)
        new_val = new_model.get(key)
        if old_val != new_val:
            changes.append({
                "section": "model", "path": f"model.{key}",
                "change_type": CHANGE_MODIFIED, "old_value": old_val, "new_value": new_val,
                "summary": _field_change_summary("model", key, old_val, new_val),
            })


def _diff_presentation(old_pres: dict, new_pres: dict, changes: list[dict]) -> None:
    if old_pres == new_pres:
        return
    changes.append({
        "section": "presentation", "path": "presentation",
        "change_type": CHANGE_MODIFIED, "old_value": old_pres, "new_value": new_pres,
        "summary": "Presentation settings updated",
    })


def _diff_references(old_refs: list, new_refs: list, changes: list[dict]) -> None:
    old_set = set(old_refs)
    new_set = set(new_refs)
    for ref in sorted(new_set - old_set):
        changes.append({
            "section": "reference", "path": f"reference.{ref}",
            "change_type": CHANGE_ADDED, "old_value": None, "new_value": ref,
            "summary": f"Added reference to model '{ref}'",
        })
    for ref in sorted(old_set - new_set):
        changes.append({
            "section": "reference", "path": f"reference.{ref}",
            "change_type": CHANGE_REMOVED, "old_value": ref, "new_value": None,
            "summary": f"Removed reference to model '{ref}'",
        })


def _diff_transformations(
    old_transforms: dict, new_transforms: dict,
    old_order: list[str], new_order: list[str],
    changes: list[dict],
) -> None:
    old_keys = set(old_transforms.keys())
    new_keys = set(new_transforms.keys())

    for tid in sorted(new_keys - old_keys):
        t_data = new_transforms[tid]
        t_type = t_data.get("type", "unknown")
        changes.append({
            "section": "transform", "path": f"transform.{tid}",
            "transformation_id": tid, "transformation_type": t_type,
            "change_type": CHANGE_ADDED, "old_value": None, "new_value": t_data,
            "summary": f"Added {t_type} transformation '{tid}'",
        })

    for tid in sorted(old_keys - new_keys):
        t_data = old_transforms[tid]
        t_type = t_data.get("type", "unknown")
        changes.append({
            "section": "transform", "path": f"transform.{tid}",
            "transformation_id": tid, "transformation_type": t_type,
            "change_type": CHANGE_REMOVED, "old_value": t_data, "new_value": None,
            "summary": f"Removed {t_type} transformation '{tid}'",
        })

    for tid in sorted(old_keys & new_keys):
        old_t = old_transforms[tid]
        new_t = new_transforms[tid]
        if old_t == new_t:
            continue
        t_type = new_t.get("type", old_t.get("type", "unknown"))
        field_changes = _diff_transform_fields(old_t, new_t)
        changes.append({
            "section": "transform", "path": f"transform.{tid}",
            "transformation_id": tid, "transformation_type": t_type,
            "change_type": CHANGE_MODIFIED, "old_value": old_t, "new_value": new_t,
            "field_changes": field_changes,
            "summary": _transform_modify_summary(tid, t_type, field_changes),
        })

    common_old = [t for t in old_order if t in (old_keys & new_keys)]
    common_new = [t for t in new_order if t in (old_keys & new_keys)]
    if common_old != common_new and set(common_old) == set(common_new):
        changes.append({
            "section": "transform_order", "path": "transform_order",
            "change_type": CHANGE_REORDERED, "old_value": old_order, "new_value": new_order,
            "summary": _reorder_summary(common_old, common_new),
        })


def _diff_transform_fields(old_t: dict, new_t: dict) -> list[dict[str, Any]]:
    field_changes = []
    for key in sorted(set(list(old_t.keys()) + list(new_t.keys()))):
        old_val = old_t.get(key)
        new_val = new_t.get(key)
        if old_val == new_val:
            continue
        if old_val is None:
            change_type = CHANGE_ADDED
        elif new_val is None:
            change_type = CHANGE_REMOVED
        else:
            change_type = CHANGE_MODIFIED
        field_changes.append({"field": key, "change_type": change_type, "old_value": old_val, "new_value": new_val})
    return field_changes


def _field_change_summary(section: str, key: str, old_val: Any, new_val: Any) -> str:
    if old_val is None:
        return f"{section}.{key} set to '{new_val}'"
    if new_val is None:
        return f"{section}.{key} removed (was '{old_val}')"
    return f"{section}.{key} changed from '{old_val}' to '{new_val}'"


def _transform_modify_summary(tid: str, t_type: str, field_changes: list[dict]) -> str:
    n = len(field_changes)
    fields = ", ".join(fc["field"] for fc in field_changes[:3])
    suffix = f" and {n - 3} more" if n > 3 else ""
    return f"Modified {t_type} transformation '{tid}': changed {fields}{suffix}"


def _reorder_summary(old_order: list[str], new_order: list[str]) -> str:
    for i, (a, b) in enumerate(zip(old_order, new_order)):
        if a != b:
            return f"Transformation order changed: '{b}' moved to position {i + 1} (was '{a}' at that position)"
    return "Transformation execution order changed"


def _build_overall_summary(stats: dict[str, int]) -> str:
    parts = []
    if stats.get(CHANGE_ADDED):
        parts.append(f"{stats[CHANGE_ADDED]} added")
    if stats.get(CHANGE_REMOVED):
        parts.append(f"{stats[CHANGE_REMOVED]} removed")
    if stats.get(CHANGE_MODIFIED):
        parts.append(f"{stats[CHANGE_MODIFIED]} modified")
    if stats.get(CHANGE_REORDERED):
        parts.append(f"{stats[CHANGE_REORDERED]} reordered")
    if not parts:
        return "No changes detected"
    return f"{sum(stats.values())} change(s): {', '.join(parts)}"
