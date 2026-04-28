"""
Unit tests for SQLQueryBuilder - transformation order aware SQL generation.

These tests verify that the SQL builder respects the transform_order from YAML
and generates correct SQL, including CTEs when needed to maintain order semantics.
"""

import pytest

from backend.application.config_parser.config_parser import ConfigParser
from backend.application.config_parser.sql_builder import (
    SQLQueryBuilder,
    QueryState,
    FilterHandler,
    JoinHandler,
    SynthesizeHandler,
)


class TestQueryState:
    """Test the QueryState dataclass."""

    def test_get_source_ref_no_cte(self):
        """Source ref should return schema.table when no CTEs."""
        state = QueryState(from_table="orders", from_schema="public")
        assert state.get_source_ref() == '"public"."orders"'

    def test_get_source_ref_no_schema(self):
        """Source ref should return just table when no schema."""
        state = QueryState(from_table="orders", from_schema="")
        assert state.get_source_ref() == '"orders"'

    def test_get_source_ref_with_cte(self):
        """Source ref should return latest CTE name when CTEs exist."""
        state = QueryState(from_table="orders", from_schema="public")
        state.create_cte("SELECT * FROM orders")
        assert state.get_source_ref() == '"step_1"'

    def test_create_cte_increments_counter(self):
        """Each CTE creation should increment the step counter."""
        state = QueryState(from_table="orders")
        state.create_cte("SELECT * FROM orders WHERE x > 1")
        state.create_cte("SELECT * FROM step_1 JOIN other ON ...")
        assert state.step_counter == 2
        assert len(state.cte_steps) == 2
        assert state.cte_steps[0].name == "step_1"
        assert state.cte_steps[1].name == "step_2"

    def test_create_cte_resets_state(self):
        """Creating a CTE should reset accumulating state."""
        state = QueryState(from_table="orders")
        state.select_columns = ["id", "name"]
        state.where_conditions = ["x > 1"]
        state.create_cte("SELECT id, name FROM orders WHERE x > 1")

        assert state.select_columns == []
        assert state.where_conditions == []


class TestFilterHandler:
    """Test the FilterHandler."""

    def test_format_sql_value_string(self):
        """String values should be quoted."""
        handler = FilterHandler()
        assert handler._format_sql_value("hello") == "'hello'"

    def test_format_sql_value_number(self):
        """Numeric values should not be quoted."""
        handler = FilterHandler()
        assert handler._format_sql_value(42) == "42"
        assert handler._format_sql_value(3.14) == "3.14"

    def test_format_sql_value_boolean(self):
        """Boolean values should be TRUE/FALSE."""
        handler = FilterHandler()
        assert handler._format_sql_value(True) == "TRUE"
        assert handler._format_sql_value(False) == "FALSE"

    def test_format_sql_value_null(self):
        """None should be NULL."""
        handler = FilterHandler()
        assert handler._format_sql_value(None) == "NULL"

    def test_format_sql_value_list_in_operator(self):
        """List with IN operator should be parenthesized."""
        handler = FilterHandler()
        result = handler._format_sql_value([1, 2, 3], "IN")
        assert result == "(1, 2, 3)"

    def test_format_sql_value_contains_operator(self):
        """CONTAINS should add wildcards."""
        handler = FilterHandler()
        assert handler._format_sql_value("test", "CONTAINS") == "'%test%'"

    def test_format_sql_value_startswith_operator(self):
        """STARTSWITH should add trailing wildcard."""
        handler = FilterHandler()
        assert handler._format_sql_value("test", "STARTSWITH") == "'test%'"

    def test_format_sql_value_endswith_operator(self):
        """ENDSWITH should add leading wildcard."""
        handler = FilterHandler()
        assert handler._format_sql_value("test", "ENDSWITH") == "'%test'"

    def test_requires_cte_before_with_joins(self):
        """Filter after join should require CTE."""
        handler = FilterHandler()
        state = QueryState(from_table="orders")
        state.join_clauses = ["INNER JOIN customers ON orders.customer_id = customers.id"]

        assert handler.requires_cte_before(state, None, None) is True

    def test_requires_cte_before_without_joins(self):
        """Filter without prior joins should not require CTE."""
        handler = FilterHandler()
        state = QueryState(from_table="orders")

        assert handler.requires_cte_before(state, None, None) is False


class TestJoinHandler:
    """Test the JoinHandler."""

    def test_requires_cte_before_with_where(self):
        """Join after filter should require CTE to preserve order."""
        handler = JoinHandler()
        state = QueryState(from_table="orders")
        state.where_conditions = ["status = 'active'"]

        assert handler.requires_cte_before(state, None, None) is True

    def test_requires_cte_before_without_where(self):
        """Join without prior filter should not require CTE."""
        handler = JoinHandler()
        state = QueryState(from_table="orders")

        assert handler.requires_cte_before(state, None, None) is False


class TestSQLQueryBuilder:
    """Test the main SQLQueryBuilder class."""

    def test_simple_select_all(self):
        """Simple model with no transforms should SELECT * FROM table."""
        model_data = {
            "source": {
                "schema_name": "public",
                "table_name": "orders"
            },
            "model": {
                "schema_name": "public",
                "table_name": "orders_output"
            },
            "transform": {},
            "transform_order": [],
            "presentation": {}
        }
        parser = ConfigParser(model_data, "test_simple")
        # Clear the singleton cache for fresh test
        ConfigParser._instances.pop("test_simple", None)
        parser = ConfigParser(model_data, "test_simple")

        sql = parser.get_compiled_sql()
        assert "SELECT" in sql
        assert '"public"."orders"' in sql or '"orders"' in sql

    def test_filter_transformation(self):
        """Filter should generate WHERE clause."""
        model_data = {
            "source": {
                "schema_name": "public",
                "table_name": "orders"
            },
            "model": {
                "schema_name": "public",
                "table_name": "filtered_orders"
            },
            "transform": {
                "filter_1": {
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
            },
            "transform_order": ["filter_1"],
            "presentation": {}
        }
        # Clear singleton cache
        ConfigParser._instances.pop("test_filter", None)
        parser = ConfigParser(model_data, "test_filter")

        sql = parser.get_compiled_sql()
        assert "WHERE" in sql
        assert '"status"' in sql
        assert "'active'" in sql

    def test_join_transformation(self):
        """Join should generate JOIN clause."""
        model_data = {
            "source": {
                "schema_name": "public",
                "table_name": "orders"
            },
            "model": {
                "schema_name": "public",
                "table_name": "orders_with_customers"
            },
            "transform": {
                "join_1": {
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
            },
            "transform_order": ["join_1"],
            "presentation": {}
        }
        # Clear singleton cache
        ConfigParser._instances.pop("test_join", None)
        parser = ConfigParser(model_data, "test_join")

        sql = parser.get_compiled_sql()
        assert "JOIN" in sql
        assert '"customers"' in sql

    def test_filter_before_join_uses_cte(self):
        """
        Filter before join should use CTE to ensure filter is applied first.

        Legacy behavior: Filter is applied to source table, THEN join is applied.
        This requires wrapping the filter in a CTE so join operates on filtered data.
        """
        model_data = {
            "source": {
                "schema_name": "public",
                "table_name": "orders"
            },
            "model": {
                "schema_name": "public",
                "table_name": "active_orders_with_customers"
            },
            "transform": {
                "filter_1": {
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
                },
                "join_1": {
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
            },
            "transform_order": ["filter_1", "join_1"],
            "presentation": {}
        }
        # Clear singleton cache
        ConfigParser._instances.pop("test_filter_join", None)
        parser = ConfigParser(model_data, "test_filter_join")

        sql = parser.get_compiled_sql()

        # Should have CTE with filter
        assert "WITH" in sql
        assert "'active'" in sql

        # Should have JOIN
        assert "JOIN" in sql
        assert '"customers"' in sql

    def test_join_before_filter_uses_cte(self):
        """
        Join before filter uses CTE to ensure transformation order is respected.

        When filter comes after join, we wrap the join in a CTE first, then
        the filter applies to the joined result.
        """
        model_data = {
            "source": {
                "schema_name": "public",
                "table_name": "orders"
            },
            "model": {
                "schema_name": "public",
                "table_name": "orders_filtered_after_join"
            },
            "transform": {
                "join_1": {
                    "type": "join",
                    "join": {
                        "tables": [
                            {
                                "type": "INNER",
                                "source": {
                                    "table_name": "orders",
                                    "column_name": "customer_id"
                                },
                                "joined_table": {
                                    "table_name": "customers",
                                    "column_name": "id"
                                }
                            }
                        ]
                    }
                },
                "filter_1": {
                    "type": "filter",
                    "filter": {
                        "criteria": [
                            {
                                "condition": {
                                    "lhs": {
                                        "type": "COLUMN",
                                        "column": {
                                            "column_name": "total",
                                            "table_name": "orders"
                                        }
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
            },
            "transform_order": ["join_1", "filter_1"],
            "presentation": {}
        }
        # Clear singleton cache
        ConfigParser._instances.pop("test_join_filter", None)
        parser = ConfigParser(model_data, "test_join_filter")

        sql = parser.get_compiled_sql()

        # Should have CTE with join, then filter on CTE
        assert "WITH" in sql
        assert "JOIN" in sql
        assert "WHERE" in sql

    def test_synthesize_adds_column(self):
        """Synthesize should add computed column to SELECT."""
        model_data = {
            "source": {
                "schema_name": "public",
                "table_name": "orders"
            },
            "model": {
                "schema_name": "public",
                "table_name": "orders_with_total"
            },
            "transform": {
                "synth_1": {
                    "type": "synthesize",
                    "synthesize": {
                        "columns": [
                            {
                                "column_name": "total_with_tax",
                                "formula": "price * quantity * 1.1"
                            }
                        ]
                    }
                }
            },
            "transform_order": ["synth_1"],
            "presentation": {}
        }
        # Clear singleton cache
        ConfigParser._instances.pop("test_synthesize", None)
        parser = ConfigParser(model_data, "test_synthesize")

        sql = parser.get_compiled_sql()
        assert "total_with_tax" in sql
        assert "price * quantity * 1.1" in sql

    def test_groups_and_aggregation(self):
        """Group by should generate GROUP BY clause with aggregates."""
        model_data = {
            "source": {
                "schema_name": "public",
                "table_name": "orders"
            },
            "model": {
                "schema_name": "public",
                "table_name": "orders_summary"
            },
            "transform": {
                "group_1": {
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
            },
            "transform_order": ["group_1"],
            "presentation": {}
        }
        # Clear singleton cache
        ConfigParser._instances.pop("test_group", None)
        parser = ConfigParser(model_data, "test_group")

        sql = parser.get_compiled_sql()
        assert "GROUP BY" in sql
        assert '"customer_id"' in sql
        assert "SUM" in sql
        assert "COUNT" in sql

    def test_distinct_transformation(self):
        """Distinct should add DISTINCT keyword."""
        model_data = {
            "source": {
                "schema_name": "public",
                "table_name": "orders"
            },
            "model": {
                "schema_name": "public",
                "table_name": "distinct_customers"
            },
            "transform": {
                "distinct_1": {
                    "type": "distinct",
                    "distinct": {
                        "columns": []  # Empty means DISTINCT on all columns
                    }
                }
            },
            "transform_order": ["distinct_1"],
            "presentation": {}
        }
        # Clear singleton cache
        ConfigParser._instances.pop("test_distinct", None)
        parser = ConfigParser(model_data, "test_distinct")

        sql = parser.get_compiled_sql()
        assert "DISTINCT" in sql

    def test_distinct_on_specific_columns(self):
        """DISTINCT ON should work with specific columns."""
        model_data = {
            "source": {
                "schema_name": "public",
                "table_name": "orders"
            },
            "model": {
                "schema_name": "public",
                "table_name": "distinct_customers"
            },
            "transform": {
                "distinct_1": {
                    "type": "distinct",
                    "distinct": {
                        "columns": ["customer_id"]
                    }
                }
            },
            "transform_order": ["distinct_1"],
            "presentation": {}
        }
        # Clear singleton cache
        ConfigParser._instances.pop("test_distinct_cols", None)
        parser = ConfigParser(model_data, "test_distinct_cols")

        sql = parser.get_compiled_sql()
        assert "DISTINCT ON" in sql
        assert '"customer_id"' in sql


class TestTransformationOrderSemantics:
    """
    Test cases for complex transformation order scenarios.

    These tests verify that the SQL builder produces semantically correct
    SQL that matches the legacy Ibis behavior for various transformation orders.
    """

    def test_multiple_filters_in_order(self):
        """Multiple filters should all be applied in order."""
        model_data = {
            "source": {
                "table_name": "orders"
            },
            "model": {
                "table_name": "filtered_orders"
            },
            "transform": {
                "filter_active": {
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
                },
                "filter_amount": {
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
            },
            "transform_order": ["filter_active", "filter_amount"],
            "presentation": {}
        }
        # Clear singleton cache
        ConfigParser._instances.pop("test_multi_filter", None)
        parser = ConfigParser(model_data, "test_multi_filter")

        sql = parser.get_compiled_sql()
        # Both filters should be present
        assert "'active'" in sql
        assert "100" in sql

    def test_synthesize_then_filter_on_synthesized(self):
        """Filter on synthesized column should work when synthesize comes first."""
        model_data = {
            "source": {
                "table_name": "orders"
            },
            "model": {
                "table_name": "high_value_orders"
            },
            "transform": {
                "calc_total": {
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
                },
                "filter_high": {
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
                }
            },
            "transform_order": ["calc_total", "filter_high"],
            "presentation": {}
        }
        # Clear singleton cache
        ConfigParser._instances.pop("test_synth_filter", None)
        parser = ConfigParser(model_data, "test_synth_filter")

        sql = parser.get_compiled_sql()
        # Should have both synthesize and filter
        assert "price * quantity" in sql
        assert "total" in sql


# Cleanup singleton cache after all tests
@pytest.fixture(autouse=True)
def cleanup_singleton_cache():
    """Clean up ConfigParser singleton cache before each test."""
    yield
    # Clear all test instances
    test_keys = [k for k in ConfigParser._instances.keys() if k.startswith("test_")]
    for key in test_keys:
        ConfigParser._instances.pop(key, None)
