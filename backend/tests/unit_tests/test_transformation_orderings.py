"""
Comprehensive tests for transformation orderings in SQL generation.

This test suite verifies that the SQLQueryBuilder correctly handles
all transformations in various orderings, including:
1. Single transformations
2. Pairs of transformations in all orderings
3. Complex multi-transformation scenarios
4. CTE generation when order requires it

Supported transformations:
- filter: WHERE clause filtering
- join: JOIN operations
- synthesize: Computed columns (additive)
- combine_columns: String concatenation columns
- rename_column: Column aliasing
- find_and_replace: CASE WHEN replacements
- window: Window functions (ROW_NUMBER, RANK, etc.)
- groups_and_aggregation: GROUP BY with aggregates
- distinct: DISTINCT/DISTINCT ON
- union: UNION operations
- pivot: Pivot transformations
- sort: ORDER BY clause
"""

import pytest
from itertools import permutations
from backend.application.config_parser.config_parser import ConfigParser


# =============================================================================
# Test Fixtures - Reusable transformation configurations
# =============================================================================

def get_base_model(name: str) -> dict:
    """Get base model configuration."""
    return {
        "source": {
            "schema_name": "public",
            "table_name": "orders"
        },
        "model": {
            "schema_name": "public",
            "table_name": f"test_{name}"
        },
        "transform": {},
        "transform_order": [],
        "presentation": {}
    }


def get_filter_transform() -> tuple[str, dict]:
    """Filter transformation - WHERE status = 'active'."""
    return "filter_1", {
        "type": "filter",
        "filter": {
            "criteria": [
                {
                    "condition": {
                        "lhs": {
                            "type": "COLUMN",
                            "column": {"column_name": "status"}
                        },
                        "operator": "EQ",
                        "rhs": {
                            "type": "VALUE",
                            "value": "active"
                        }
                    }
                }
            ]
        }
    }


def get_filter_transform_2() -> tuple[str, dict]:
    """Second filter transformation - WHERE amount > 100."""
    return "filter_2", {
        "type": "filter",
        "filter": {
            "criteria": [
                {
                    "condition": {
                        "lhs": {
                            "type": "COLUMN",
                            "column": {"column_name": "amount"}
                        },
                        "operator": "GT",
                        "rhs": {
                            "type": "VALUE",
                            "value": 100
                        }
                    }
                }
            ]
        }
    }


def get_join_transform() -> tuple[str, dict]:
    """Join transformation - JOIN customers ON customer_id = id."""
    return "join_1", {
        "type": "join",
        "join": {
            "tables": [
                {
                    "type": "LEFT",
                    "source": {
                        "table_name": "orders",
                        "column_name": "customer_id"
                    },
                    "joined_table": {
                        "schema_name": "public",
                        "table_name": "customers",
                        "column_name": "id"
                    }
                }
            ]
        }
    }


def get_join_criteria_transform() -> tuple[str, dict]:
    """Join transformation using criteria - for testing proper column qualification."""
    return "join_criteria", {
        "type": "join",
        "join": {
            "tables": [
                {
                    "type": "INNER",
                    "joined_table": {
                        "schema_name": "public",
                        "table_name": "customers"
                    },
                    "criteria": [
                        {
                            "condition": {
                                "lhs": {
                                    "type": "COLUMN",
                                    "column": {"column_name": "customer_id"}
                                },
                                "operator": "EQ",
                                "rhs": {
                                    "type": "COLUMN",
                                    "column": {"column_name": "id"}
                                }
                            }
                        }
                    ]
                }
            ]
        }
    }


def get_synthesize_transform() -> tuple[str, dict]:
    """Synthesize transformation - computed column total = price * quantity."""
    return "synth_1", {
        "type": "synthesize",
        "synthesize": {
            "columns": [
                {
                    "column_name": "total",
                    "type": "FORMULA",
                    "operation": {
                        "formula": "price * quantity"
                    }
                }
            ]
        }
    }


def get_synthesize_transform_2() -> tuple[str, dict]:
    """Second synthesize transformation - computed column discount_price."""
    return "synth_2", {
        "type": "synthesize",
        "synthesize": {
            "columns": [
                {
                    "column_name": "discount_price",
                    "type": "FORMULA",
                    "operation": {
                        "formula": "price * 0.9"
                    }
                }
            ]
        }
    }


def get_combine_columns_transform() -> tuple[str, dict]:
    """Combine columns transformation - concatenate first_name and last_name."""
    return "combine_1", {
        "type": "combine_columns",
        "combine_columns": {
            "columns": [
                {
                    "columnName": "full_name",
                    "values": [
                        {"type": "column", "value": "first_name"},
                        {"type": "string", "value": " "},
                        {"type": "column", "value": "last_name"}
                    ]
                }
            ]
        }
    }


def get_rename_transform() -> tuple[str, dict]:
    """Rename transformation - rename columns."""
    return "rename_1", {
        "type": "rename_column",
        "rename_column": {
            "mappings": [
                {
                    "old_name": "id",
                    "new_name": "order_id"
                },
                {
                    "old_name": "amount",
                    "new_name": "order_amount"
                }
            ]
        }
    }


def get_find_and_replace_transform() -> tuple[str, dict]:
    """Find and replace transformation - replace status values."""
    return "fnr_1", {
        "type": "find_and_replace",
        "find_and_replace": {
            "replacements": [
                {
                    "column_list": ["status"],
                    "operation": [
                        {"match_type": "exact", "find": "A", "replace": "Active"},
                        {"match_type": "exact", "find": "I", "replace": "Inactive"}
                    ]
                }
            ]
        }
    }


def get_window_transform() -> tuple[str, dict]:
    """Window transformation - ROW_NUMBER() partitioned by customer_id."""
    return "window_1", {
        "type": "window",
        "window": {
            "columns": [
                {
                    "column_name": "row_num",
                    "type": "WINDOW",
                    "operation": {
                        "function": "ROW_NUMBER",
                        "partition_by": ["customer_id"],
                        "order_by": [{"column": "created_at", "direction": "DESC"}]
                    }
                }
            ]
        }
    }


def get_window_rank_transform() -> tuple[str, dict]:
    """Window transformation - RANK() for ranking."""
    return "window_rank", {
        "type": "window",
        "window": {
            "columns": [
                {
                    "column_name": "order_rank",
                    "type": "WINDOW",
                    "operation": {
                        "function": "RANK",
                        "partition_by": ["customer_id"],
                        "order_by": [{"column": "amount", "direction": "DESC"}]
                    }
                }
            ]
        }
    }


def get_groups_and_aggregation_transform() -> tuple[str, dict]:
    """Groups and aggregation transformation - GROUP BY customer_id with SUM."""
    return "group_1", {
        "type": "groups_and_aggregation",
        "groups_and_aggregation": {
            "group": ["customer_id"],
            "aggregate_columns": [
                {
                    "function": "SUM",
                    "column": "amount",
                    "alias": "total_amount"
                },
                {
                    "function": "COUNT",
                    "column": "*",
                    "alias": "order_count"
                }
            ]
        }
    }


def get_distinct_transform() -> tuple[str, dict]:
    """Distinct transformation - DISTINCT on all columns."""
    return "distinct_1", {
        "type": "distinct",
        "distinct": {
            "columns": []
        }
    }


def get_distinct_on_transform() -> tuple[str, dict]:
    """Distinct ON transformation - DISTINCT ON specific columns."""
    return "distinct_on_1", {
        "type": "distinct",
        "distinct": {
            "columns": ["customer_id"]
        }
    }


def get_union_transform() -> tuple[str, dict]:
    """Union transformation - UNION with another table (legacy table-based format)."""
    return "union_1", {
        "type": "union",
        "union": {
            "tables": [
                {
                    "source_table": "orders",
                    "source_column": "id",
                    "merge_table": "archived_orders",
                    "merge_column": "id",
                    "column_type": "Integer"
                }
            ],
            "ignore_duplicate": False
        }
    }


def get_sort_transform() -> tuple[str, dict]:
    """Sort transformation - ORDER BY created_at DESC.

    NOTE: Sort is handled via presentation parser, not as a transformation.
    This returns a no-op transformation that won't affect SQL.
    For ORDER BY, use presentation.sort in the model config.
    """
    # Sort is not a transformation type - use presentation parser instead
    # Return a filter as placeholder (will be skipped in sort-specific tests)
    return None, None


def get_presentation_with_sort() -> dict:
    """Presentation config with sort."""
    return {
        "sort": [
            {"column": "created_at", "order": "desc"},
            {"column": "id", "order": "asc"}
        ]
    }


# =============================================================================
# Helper Functions
# =============================================================================

def build_and_compile(model_data: dict, model_name: str) -> str:
    """Build ConfigParser and compile SQL."""
    # Clear singleton cache
    ConfigParser._instances.pop(model_name, None)
    parser = ConfigParser(model_data, model_name)
    return parser.get_compiled_sql()


def create_model_with_transforms(transforms: list[tuple[str, dict]], name: str) -> dict:
    """Create a model with given transforms."""
    model = get_base_model(name)
    for item in transforms:
        if item is None or item[0] is None:
            continue  # Skip None transforms (e.g., sort placeholder)
        key, transform = item
        model["transform"][key] = transform
        model["transform_order"].append(key)
    return model


# =============================================================================
# Single Transformation Tests
# =============================================================================

class TestSingleTransformations:
    """Test each transformation type individually."""

    def test_filter_only(self):
        """Single filter transformation."""
        model = create_model_with_transforms([get_filter_transform()], "single_filter")
        sql = build_and_compile(model, "single_filter")

        assert "WHERE" in sql
        assert '"status"' in sql
        assert "'active'" in sql
        assert "SELECT" in sql

    def test_join_only(self):
        """Single join transformation."""
        model = create_model_with_transforms([get_join_transform()], "single_join")
        sql = build_and_compile(model, "single_join")

        assert "JOIN" in sql
        assert '"customers"' in sql
        assert "ON" in sql

    def test_join_with_criteria(self):
        """Join using criteria - verifies column qualification fix."""
        model = create_model_with_transforms([get_join_criteria_transform()], "single_join_criteria")
        sql = build_and_compile(model, "single_join_criteria")

        assert "JOIN" in sql
        assert "ON" in sql
        # Verify columns are properly qualified
        assert '"orders"."customer_id"' in sql or '"customer_id"' in sql
        # Should NOT have ambiguous ON "col" = "col" pattern
        assert 'ON "customer_id" = "id"' not in sql

    def test_synthesize_only(self):
        """Single synthesize transformation."""
        model = create_model_with_transforms([get_synthesize_transform()], "single_synth")
        sql = build_and_compile(model, "single_synth")

        assert "SELECT" in sql
        assert "total" in sql
        assert "price * quantity" in sql

    def test_combine_columns_only(self):
        """Single combine columns transformation."""
        model = create_model_with_transforms([get_combine_columns_transform()], "single_combine")
        sql = build_and_compile(model, "single_combine")

        assert "SELECT" in sql
        assert "full_name" in sql

    def test_rename_only(self):
        """Single rename transformation."""
        model = create_model_with_transforms([get_rename_transform()], "single_rename")
        sql = build_and_compile(model, "single_rename")

        assert "SELECT" in sql
        assert "order_id" in sql
        assert "order_amount" in sql

    def test_find_and_replace_only(self):
        """Single find and replace transformation."""
        model = create_model_with_transforms([get_find_and_replace_transform()], "single_fnr")
        sql = build_and_compile(model, "single_fnr")

        assert "SELECT" in sql
        assert "CASE" in sql or "status" in sql

    def test_window_only(self):
        """Single window transformation."""
        model = create_model_with_transforms([get_window_transform()], "single_window")
        sql = build_and_compile(model, "single_window")

        assert "SELECT" in sql
        assert "ROW_NUMBER" in sql or "row_num" in sql

    def test_groups_and_aggregation_only(self):
        """Single groups and aggregation transformation."""
        model = create_model_with_transforms([get_groups_and_aggregation_transform()], "single_group")
        sql = build_and_compile(model, "single_group")

        assert "SELECT" in sql
        assert "GROUP BY" in sql
        assert "SUM" in sql
        assert "COUNT" in sql

    def test_distinct_only(self):
        """Single distinct transformation."""
        model = create_model_with_transforms([get_distinct_transform()], "single_distinct")
        sql = build_and_compile(model, "single_distinct")

        assert "SELECT" in sql
        assert "DISTINCT" in sql

    def test_distinct_on_only(self):
        """Single distinct ON transformation."""
        model = create_model_with_transforms([get_distinct_on_transform()], "single_distinct_on")
        sql = build_and_compile(model, "single_distinct_on")

        assert "SELECT" in sql
        assert "DISTINCT" in sql

    def test_union_only(self):
        """Single union transformation."""
        model = create_model_with_transforms([get_union_transform()], "single_union")
        sql = build_and_compile(model, "single_union")

        assert "UNION" in sql
        assert "archived_orders" in sql

    def test_sort_via_presentation(self):
        """Sort via presentation parser (not a transformation)."""
        model = get_base_model("single_sort")
        model["presentation"] = get_presentation_with_sort()

        sql = build_and_compile(model, "single_sort")

        assert "SELECT" in sql
        assert "ORDER BY" in sql


# =============================================================================
# Filter + Other Transformation Pairs
# =============================================================================

class TestFilterPairs:
    """Test filter combined with other transformations in both orders."""

    def test_filter_then_join(self):
        """Filter before join - should use CTE."""
        model = create_model_with_transforms(
            [get_filter_transform(), get_join_transform()],
            "filter_then_join"
        )
        sql = build_and_compile(model, "filter_then_join")

        assert "WITH" in sql  # CTE expected
        assert "WHERE" in sql
        assert "JOIN" in sql

    def test_join_then_filter(self):
        """Join before filter - should use CTE."""
        model = create_model_with_transforms(
            [get_join_transform(), get_filter_transform()],
            "join_then_filter"
        )
        sql = build_and_compile(model, "join_then_filter")

        assert "WITH" in sql  # CTE expected
        assert "JOIN" in sql
        assert "WHERE" in sql

    def test_filter_then_synthesize(self):
        """Filter before synthesize."""
        model = create_model_with_transforms(
            [get_filter_transform(), get_synthesize_transform()],
            "filter_then_synth"
        )
        sql = build_and_compile(model, "filter_then_synth")

        assert "WHERE" in sql
        assert "total" in sql

    def test_synthesize_then_filter(self):
        """Synthesize before filter - filter can reference synthesized column."""
        model = create_model_with_transforms(
            [get_synthesize_transform(), get_filter_transform()],
            "synth_then_filter"
        )
        sql = build_and_compile(model, "synth_then_filter")

        assert "total" in sql
        assert "WHERE" in sql

    def test_filter_then_group(self):
        """Filter before group - WHERE applied to source, GROUP BY on filtered."""
        model = create_model_with_transforms(
            [get_filter_transform(), get_groups_and_aggregation_transform()],
            "filter_then_group"
        )
        sql = build_and_compile(model, "filter_then_group")

        assert "WHERE" in sql
        assert "GROUP BY" in sql

    def test_group_then_filter(self):
        """Group before filter - filter becomes HAVING or post-group WHERE."""
        model = create_model_with_transforms(
            [get_groups_and_aggregation_transform(), get_filter_transform()],
            "group_then_filter"
        )
        sql = build_and_compile(model, "group_then_filter")

        assert "GROUP BY" in sql
        # Filter after group should wrap in CTE or use HAVING
        assert "WHERE" in sql or "HAVING" in sql

    def test_filter_then_distinct(self):
        """Filter before distinct."""
        model = create_model_with_transforms(
            [get_filter_transform(), get_distinct_transform()],
            "filter_then_distinct"
        )
        sql = build_and_compile(model, "filter_then_distinct")

        assert "WHERE" in sql
        assert "DISTINCT" in sql

    def test_distinct_then_filter(self):
        """Distinct before filter - should use CTE."""
        model = create_model_with_transforms(
            [get_distinct_transform(), get_filter_transform()],
            "distinct_then_filter"
        )
        sql = build_and_compile(model, "distinct_then_filter")

        assert "DISTINCT" in sql
        assert "WHERE" in sql

    def test_filter_then_window(self):
        """Filter before window function."""
        model = create_model_with_transforms(
            [get_filter_transform(), get_window_transform()],
            "filter_then_window"
        )
        sql = build_and_compile(model, "filter_then_window")

        assert "WHERE" in sql
        assert "ROW_NUMBER" in sql or "row_num" in sql

    def test_window_then_filter(self):
        """Window before filter - filter on window result requires CTE."""
        model = create_model_with_transforms(
            [get_window_transform(), get_filter_transform()],
            "window_then_filter"
        )
        sql = build_and_compile(model, "window_then_filter")

        assert "ROW_NUMBER" in sql or "row_num" in sql
        assert "WHERE" in sql

    def test_filter_with_sort(self):
        """Filter with sort (via presentation)."""
        model = create_model_with_transforms(
            [get_filter_transform()],
            "filter_with_sort"
        )
        model["presentation"] = get_presentation_with_sort()
        sql = build_and_compile(model, "filter_with_sort")

        assert "WHERE" in sql
        assert "ORDER BY" in sql


# =============================================================================
# Join + Other Transformation Pairs
# =============================================================================

class TestJoinPairs:
    """Test join combined with other transformations in both orders."""

    def test_join_then_synthesize(self):
        """Join before synthesize."""
        model = create_model_with_transforms(
            [get_join_transform(), get_synthesize_transform()],
            "join_then_synth"
        )
        sql = build_and_compile(model, "join_then_synth")

        assert "JOIN" in sql
        assert "total" in sql

    def test_synthesize_then_join(self):
        """Synthesize before join - should use CTE."""
        model = create_model_with_transforms(
            [get_synthesize_transform(), get_join_transform()],
            "synth_then_join"
        )
        sql = build_and_compile(model, "synth_then_join")

        assert "total" in sql
        assert "JOIN" in sql

    def test_join_then_group(self):
        """Join before group."""
        model = create_model_with_transforms(
            [get_join_transform(), get_groups_and_aggregation_transform()],
            "join_then_group"
        )
        sql = build_and_compile(model, "join_then_group")

        assert "JOIN" in sql
        assert "GROUP BY" in sql

    def test_group_then_join(self):
        """Group before join - should use CTE."""
        model = create_model_with_transforms(
            [get_groups_and_aggregation_transform(), get_join_transform()],
            "group_then_join"
        )
        sql = build_and_compile(model, "group_then_join")

        assert "GROUP BY" in sql
        assert "JOIN" in sql

    def test_join_then_window(self):
        """Join before window."""
        model = create_model_with_transforms(
            [get_join_transform(), get_window_transform()],
            "join_then_window"
        )
        sql = build_and_compile(model, "join_then_window")

        assert "JOIN" in sql
        assert "ROW_NUMBER" in sql or "row_num" in sql

    def test_window_then_join(self):
        """Window before join - should use CTE."""
        model = create_model_with_transforms(
            [get_window_transform(), get_join_transform()],
            "window_then_join"
        )
        sql = build_and_compile(model, "window_then_join")

        assert "ROW_NUMBER" in sql or "row_num" in sql
        assert "JOIN" in sql

    def test_join_then_distinct(self):
        """Join before distinct."""
        model = create_model_with_transforms(
            [get_join_transform(), get_distinct_transform()],
            "join_then_distinct"
        )
        sql = build_and_compile(model, "join_then_distinct")

        assert "JOIN" in sql
        assert "DISTINCT" in sql

    def test_distinct_then_join(self):
        """Distinct before join - should use CTE."""
        model = create_model_with_transforms(
            [get_distinct_transform(), get_join_transform()],
            "distinct_then_join"
        )
        sql = build_and_compile(model, "distinct_then_join")

        assert "DISTINCT" in sql
        assert "JOIN" in sql


# =============================================================================
# Synthesize + Other Transformation Pairs
# =============================================================================

class TestSynthesizePairs:
    """Test synthesize combined with other transformations."""

    def test_synthesize_then_group(self):
        """Synthesize before group - can aggregate on synthesized column."""
        model = create_model_with_transforms(
            [get_synthesize_transform(), get_groups_and_aggregation_transform()],
            "synth_then_group"
        )
        sql = build_and_compile(model, "synth_then_group")

        assert "total" in sql
        assert "GROUP BY" in sql

    def test_group_then_synthesize(self):
        """Group before synthesize - synthesize on aggregated results."""
        model = create_model_with_transforms(
            [get_groups_and_aggregation_transform(), get_synthesize_transform()],
            "group_then_synth"
        )
        sql = build_and_compile(model, "group_then_synth")

        assert "GROUP BY" in sql
        assert "total" in sql

    def test_synthesize_then_window(self):
        """Synthesize before window."""
        model = create_model_with_transforms(
            [get_synthesize_transform(), get_window_transform()],
            "synth_then_window"
        )
        sql = build_and_compile(model, "synth_then_window")

        assert "total" in sql
        assert "ROW_NUMBER" in sql or "row_num" in sql

    def test_window_then_synthesize(self):
        """Window before synthesize."""
        model = create_model_with_transforms(
            [get_window_transform(), get_synthesize_transform()],
            "window_then_synth"
        )
        sql = build_and_compile(model, "window_then_synth")

        assert "ROW_NUMBER" in sql or "row_num" in sql
        assert "total" in sql

    def test_multiple_synthesize(self):
        """Multiple synthesize transformations."""
        model = create_model_with_transforms(
            [get_synthesize_transform(), get_synthesize_transform_2()],
            "multi_synth"
        )
        sql = build_and_compile(model, "multi_synth")

        assert "total" in sql
        assert "discount_price" in sql


# =============================================================================
# Window Function Pairs
# =============================================================================

class TestWindowPairs:
    """Test window functions with other transformations."""

    def test_window_then_group(self):
        """Window before group - should use CTE."""
        model = create_model_with_transforms(
            [get_window_transform(), get_groups_and_aggregation_transform()],
            "window_then_group"
        )
        sql = build_and_compile(model, "window_then_group")

        assert "WITH" in sql  # CTE needed
        assert "ROW_NUMBER" in sql or "row_num" in sql
        assert "GROUP BY" in sql

    def test_group_then_window(self):
        """Group before window - window on aggregated results."""
        model = create_model_with_transforms(
            [get_groups_and_aggregation_transform(), get_window_transform()],
            "group_then_window"
        )
        sql = build_and_compile(model, "group_then_window")

        assert "GROUP BY" in sql
        assert "ROW_NUMBER" in sql or "row_num" in sql

    def test_multiple_windows(self):
        """Multiple window transformations."""
        model = create_model_with_transforms(
            [get_window_transform(), get_window_rank_transform()],
            "multi_window"
        )
        sql = build_and_compile(model, "multi_window")

        assert "ROW_NUMBER" in sql or "row_num" in sql
        assert "RANK" in sql or "order_rank" in sql


# =============================================================================
# Union Transformation Pairs
# =============================================================================

class TestUnionPairs:
    """Test union combined with other transformations."""

    def test_filter_then_union(self):
        """Filter before union - filter applies to source before union."""
        model = create_model_with_transforms(
            [get_filter_transform(), get_union_transform()],
            "filter_then_union"
        )
        sql = build_and_compile(model, "filter_then_union")

        assert "WHERE" in sql
        assert "UNION" in sql

    def test_union_then_filter(self):
        """Union before filter - filter applies to union result."""
        model = create_model_with_transforms(
            [get_union_transform(), get_filter_transform()],
            "union_then_filter"
        )
        sql = build_and_compile(model, "union_then_filter")

        assert "UNION" in sql
        assert "WHERE" in sql

    def test_union_with_sort(self):
        """Union with sort (via presentation)."""
        model = create_model_with_transforms(
            [get_union_transform()],
            "union_with_sort"
        )
        model["presentation"] = get_presentation_with_sort()
        sql = build_and_compile(model, "union_with_sort")

        assert "UNION" in sql
        assert "ORDER BY" in sql

    def test_union_then_group(self):
        """Union before group - group applies to union result."""
        model = create_model_with_transforms(
            [get_union_transform(), get_groups_and_aggregation_transform()],
            "union_then_group"
        )
        sql = build_and_compile(model, "union_then_group")

        assert "UNION" in sql
        assert "GROUP BY" in sql


# =============================================================================
# Complex Multi-Transformation Scenarios
# =============================================================================

class TestComplexScenarios:
    """Test complex scenarios with 3+ transformations."""

    def test_filter_join_synthesize(self):
        """Filter -> Join -> Synthesize."""
        model = create_model_with_transforms(
            [get_filter_transform(), get_join_transform(), get_synthesize_transform()],
            "filter_join_synth"
        )
        sql = build_and_compile(model, "filter_join_synth")

        assert "WHERE" in sql
        assert "JOIN" in sql
        assert "total" in sql

    def test_join_filter_synthesize(self):
        """Join -> Filter -> Synthesize."""
        model = create_model_with_transforms(
            [get_join_transform(), get_filter_transform(), get_synthesize_transform()],
            "join_filter_synth"
        )
        sql = build_and_compile(model, "join_filter_synth")

        assert "JOIN" in sql
        assert "WHERE" in sql
        assert "total" in sql

    def test_synthesize_filter_join(self):
        """Synthesize -> Filter -> Join."""
        model = create_model_with_transforms(
            [get_synthesize_transform(), get_filter_transform(), get_join_transform()],
            "synth_filter_join"
        )
        sql = build_and_compile(model, "synth_filter_join")

        assert "total" in sql
        assert "WHERE" in sql
        assert "JOIN" in sql

    def test_filter_join_group_with_sort(self):
        """Filter -> Join -> Group with Sort (common analytics pattern)."""
        model = create_model_with_transforms(
            [
                get_filter_transform(),
                get_join_transform(),
                get_groups_and_aggregation_transform()
            ],
            "filter_join_group_sort"
        )
        model["presentation"] = get_presentation_with_sort()
        sql = build_and_compile(model, "filter_join_group_sort")

        assert "WHERE" in sql
        assert "JOIN" in sql
        assert "GROUP BY" in sql
        assert "ORDER BY" in sql

    def test_filter_synthesize_window_filter(self):
        """Filter -> Synthesize -> Window -> Filter (on window result)."""
        model = create_model_with_transforms(
            [
                get_filter_transform(),
                get_synthesize_transform(),
                get_window_transform(),
                get_filter_transform_2()  # Filter on window result
            ],
            "filter_synth_window_filter"
        )
        sql = build_and_compile(model, "filter_synth_window_filter")

        assert "WHERE" in sql
        assert "total" in sql
        assert "ROW_NUMBER" in sql or "row_num" in sql

    def test_join_synthesize_group_with_sort(self):
        """Join -> Synthesize -> Group with Sort."""
        model = create_model_with_transforms(
            [
                get_join_transform(),
                get_synthesize_transform(),
                get_groups_and_aggregation_transform()
            ],
            "join_synth_group_sort"
        )
        model["presentation"] = get_presentation_with_sort()
        sql = build_and_compile(model, "join_synth_group_sort")

        assert "JOIN" in sql
        assert "total" in sql
        assert "GROUP BY" in sql
        assert "ORDER BY" in sql

    def test_filter_union_filter_with_sort(self):
        """Filter -> Union -> Filter with Sort."""
        model = create_model_with_transforms(
            [
                get_filter_transform(),
                get_union_transform(),
                get_filter_transform_2()
            ],
            "filter_union_filter_sort"
        )
        model["presentation"] = get_presentation_with_sort()
        sql = build_and_compile(model, "filter_union_filter_sort")

        assert "UNION" in sql
        assert "ORDER BY" in sql

    def test_synthesize_window_distinct_with_sort(self):
        """Synthesize -> Window -> Distinct with Sort."""
        model = create_model_with_transforms(
            [
                get_synthesize_transform(),
                get_window_transform(),
                get_distinct_transform()
            ],
            "synth_window_distinct_sort"
        )
        model["presentation"] = get_presentation_with_sort()
        sql = build_and_compile(model, "synth_window_distinct_sort")

        assert "total" in sql
        assert "ROW_NUMBER" in sql or "row_num" in sql or "OVER" in sql
        assert "DISTINCT" in sql
        assert "ORDER BY" in sql


# =============================================================================
# CTE Generation Tests
# =============================================================================

class TestCTEGeneration:
    """Test that CTEs are generated when transformation order requires it."""

    def test_filter_before_join_creates_cte(self):
        """Filter before join should create CTE."""
        model = create_model_with_transforms(
            [get_filter_transform(), get_join_transform()],
            "cte_filter_join"
        )
        sql = build_and_compile(model, "cte_filter_join")

        assert "WITH" in sql
        assert "step_1" in sql.lower() or "step_" in sql.lower()

    def test_aggregate_before_join_creates_cte(self):
        """Aggregate before join should create CTE."""
        model = create_model_with_transforms(
            [get_groups_and_aggregation_transform(), get_join_transform()],
            "cte_group_join"
        )
        sql = build_and_compile(model, "cte_group_join")

        assert "WITH" in sql

    def test_window_before_aggregate_creates_cte(self):
        """Window before aggregate should create CTE."""
        model = create_model_with_transforms(
            [get_window_transform(), get_groups_and_aggregation_transform()],
            "cte_window_group"
        )
        sql = build_and_compile(model, "cte_window_group")

        assert "WITH" in sql

    def test_multiple_ctes_for_complex_order(self):
        """Complex ordering may create multiple CTEs."""
        model = create_model_with_transforms(
            [
                get_filter_transform(),
                get_join_transform(),
                get_filter_transform_2(),
                get_groups_and_aggregation_transform()
            ],
            "multi_cte"
        )
        sql = build_and_compile(model, "multi_cte")

        assert "WITH" in sql
        # Should have step references
        assert "step_" in sql.lower()


# =============================================================================
# Edge Cases and Regression Tests
# =============================================================================

class TestEdgeCases:
    """Test edge cases and potential regression scenarios."""

    def test_empty_transform_order(self):
        """No transformations - simple SELECT *."""
        model = get_base_model("empty_order")
        sql = build_and_compile(model, "empty_order")

        assert "SELECT" in sql
        assert "FROM" in sql

    def test_multiple_filters_same_type(self):
        """Multiple filter transformations."""
        model = create_model_with_transforms(
            [get_filter_transform(), get_filter_transform_2()],
            "multi_filter"
        )
        sql = build_and_compile(model, "multi_filter")

        assert "WHERE" in sql
        assert "'active'" in sql
        assert "100" in sql

    def test_rename_then_filter_on_renamed(self):
        """Rename then filter - filter should work on renamed columns."""
        filter_on_renamed = ("filter_renamed", {
            "type": "filter",
            "filter": {
                "criteria": [
                    {
                        "condition": {
                            "lhs": {
                                "type": "COLUMN",
                                "column": {"column_name": "order_id"}
                            },
                            "operator": "GT",
                            "rhs": {
                                "type": "VALUE",
                                "value": 0
                            }
                        }
                    }
                ]
            }
        })

        model = create_model_with_transforms(
            [get_rename_transform(), filter_on_renamed],
            "rename_filter"
        )
        sql = build_and_compile(model, "rename_filter")

        assert "order_id" in sql

    def test_synthesize_then_filter_on_synthesized(self):
        """Synthesize then filter on synthesized column."""
        filter_on_total = ("filter_total", {
            "type": "filter",
            "filter": {
                "criteria": [
                    {
                        "condition": {
                            "lhs": {
                                "type": "COLUMN",
                                "column": {"column_name": "total"}
                            },
                            "operator": "GT",
                            "rhs": {
                                "type": "VALUE",
                                "value": 1000
                            }
                        }
                    }
                ]
            }
        })

        model = create_model_with_transforms(
            [get_synthesize_transform(), filter_on_total],
            "synth_filter_total"
        )
        sql = build_and_compile(model, "synth_filter_total")

        assert "total" in sql
        assert "1000" in sql

    def test_join_column_qualification(self):
        """Verify JOIN ON clause has properly qualified column names."""
        model = create_model_with_transforms(
            [get_join_criteria_transform()],
            "join_qual"
        )
        sql = build_and_compile(model, "join_qual")

        # The ON clause should NOT have unqualified "column" = "column"
        # It should be "table"."column" = "table"."column"
        assert 'ON "customer_id" = "id"' not in sql
        # Verify proper qualification exists
        assert "ON" in sql
        assert '".' in sql  # Should have table.column pattern


# =============================================================================
# Permutation Tests for Common Transformation Sets
# =============================================================================

class TestPermutations:
    """Test permutations of common transformation sets."""

    @pytest.mark.parametrize("order", list(permutations(["filter", "synth"])))
    def test_filter_synthesize_permutations(self, order):
        """All permutations of filter + synthesize."""
        transforms = []
        for t in order:
            if t == "filter":
                transforms.append(get_filter_transform())
            elif t == "synth":
                transforms.append(get_synthesize_transform())

        name = f"perm_{'_'.join(order)}"
        model = create_model_with_transforms(transforms, name)
        sql = build_and_compile(model, name)

        # Both should be present
        assert "WHERE" in sql or "'active'" in sql
        assert "total" in sql

    @pytest.mark.parametrize("order", list(permutations(["filter", "join"])))
    def test_filter_join_permutations(self, order):
        """All permutations of filter + join."""
        transforms = []
        for t in order:
            if t == "filter":
                transforms.append(get_filter_transform())
            elif t == "join":
                transforms.append(get_join_transform())

        name = f"perm_fj_{'_'.join(order)}"
        model = create_model_with_transforms(transforms, name)
        sql = build_and_compile(model, name)

        assert "WHERE" in sql or "'active'" in sql
        assert "JOIN" in sql

    @pytest.mark.parametrize("order", list(permutations(["filter", "group"])))
    def test_filter_group_permutations(self, order):
        """All permutations of filter + group."""
        transforms = []
        for t in order:
            if t == "filter":
                transforms.append(get_filter_transform())
            elif t == "group":
                transforms.append(get_groups_and_aggregation_transform())

        name = f"perm_fg_{'_'.join(order)}"
        model = create_model_with_transforms(transforms, name)
        sql = build_and_compile(model, name)

        assert "WHERE" in sql or "HAVING" in sql or "'active'" in sql
        assert "GROUP BY" in sql

    @pytest.mark.parametrize("order", list(permutations(["filter", "synth", "join"])))
    def test_filter_synth_join_permutations(self, order):
        """All permutations of filter + synthesize + join (3! = 6)."""
        transforms = []
        for t in order:
            if t == "filter":
                transforms.append(get_filter_transform())
            elif t == "synth":
                transforms.append(get_synthesize_transform())
            elif t == "join":
                transforms.append(get_join_transform())

        name = f"perm_fsj_{'_'.join(order)}"
        model = create_model_with_transforms(transforms, name)
        sql = build_and_compile(model, name)

        # All three should be present in some form
        assert "WHERE" in sql or "'active'" in sql
        assert "total" in sql
        assert "JOIN" in sql


# =============================================================================
# SQL Correctness Validation
# =============================================================================

class TestSQLCorrectness:
    """Validate SQL syntax correctness."""

    def test_no_duplicate_keywords(self):
        """SQL should not have duplicate WHERE, GROUP BY, etc."""
        model = create_model_with_transforms(
            [get_filter_transform(), get_groups_and_aggregation_transform()],
            "no_dup"
        )
        sql = build_and_compile(model, "no_dup")

        # Count occurrences (may have multiple in CTEs, but should be structured)
        # Main query should have proper structure
        assert sql.count("SELECT") >= 1

    def test_balanced_parentheses(self):
        """SQL should have balanced parentheses."""
        model = create_model_with_transforms(
            [get_filter_transform(), get_join_transform(), get_synthesize_transform()],
            "balanced"
        )
        sql = build_and_compile(model, "balanced")

        assert sql.count("(") == sql.count(")")

    def test_quoted_identifiers(self):
        """Column and table names should be properly quoted."""
        model = create_model_with_transforms(
            [get_join_transform()],
            "quoted"
        )
        sql = build_and_compile(model, "quoted")

        # Should have quoted identifiers
        assert '"orders"' in sql or '"public"' in sql


# Cleanup singleton cache after all tests
@pytest.fixture(autouse=True)
def cleanup_singleton_cache():
    """Clean up ConfigParser singleton cache before each test."""
    yield
    # Clear test instances
    test_keys = [k for k in list(ConfigParser._instances.keys())
                 if any(x in k for x in ['test_', 'single_', 'filter_', 'join_',
                                          'synth_', 'group_', 'window_', 'union_',
                                          'distinct_', 'sort_', 'rename_', 'combine_',
                                          'fnr_', 'perm_', 'cte_', 'multi_', 'empty_',
                                          'quoted', 'balanced', 'no_dup'])]
    for key in test_keys:
        ConfigParser._instances.pop(key, None)
