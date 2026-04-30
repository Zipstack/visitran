"""
Unit tests for ExecutionMetrics pipeline — BaseModel.execute() routing,
_get_row_count_safe fallback, per-adapter _upsert_metrics threading,
and celery result JSON aggregation.
"""

import datetime
import pytest
from unittest.mock import Mock

from visitran.adapters.model import ExecutionMetrics, BaseModel
from visitran.events.printer import BaseResult, ExecStatus
from visitran.materialization import Materialization


# ============================================================================
# Helpers
# ============================================================================

def _make_mock_model(materialization=Materialization.TABLE):
    model = Mock()
    model.materialization = materialization
    model.destination_schema_name = "test_schema"
    model.destination_table_name = "test_table"
    model.select.return_value = Mock()
    model.select_statement = None
    return model


def _make_concrete_model(conn, model):
    """Concrete subclass of abstract BaseModel for testing."""

    class ConcreteModel(BaseModel):
        def __init__(self, db_connection, m):
            super().__init__(db_connection, m)

        @property
        def db_connection(self):
            return self._db_connection

        def execute_ephemeral(self):
            pass

        def execute_table(self):
            pass

        def execute_view(self):
            pass

        def execute_incremental(self):
            pass

    return ConcreteModel(conn, model)


# ============================================================================
# BaseModel._get_row_count_safe()
# ============================================================================

class TestGetRowCountSafe:

    def test_returns_row_count_on_success(self):
        conn = Mock()
        conn.get_table_row_count.return_value = 250
        model = _make_mock_model()
        concrete = _make_concrete_model(conn, model)

        assert concrete._get_row_count_safe() == 250
        conn.get_table_row_count.assert_called_once_with(
            schema_name="test_schema",
            table_name="test_table",
        )

    def test_returns_none_on_exception(self):
        conn = Mock()
        conn.get_table_row_count.side_effect = Exception("connection lost")
        concrete = _make_concrete_model(conn, _make_mock_model())

        assert concrete._get_row_count_safe() is None

    def test_returns_zero(self):
        conn = Mock()
        conn.get_table_row_count.return_value = 0
        concrete = _make_concrete_model(conn, _make_mock_model())

        assert concrete._get_row_count_safe() == 0


# ============================================================================
# BaseModel.execute() — routing per materialization
# ============================================================================

class TestBaseModelExecute:

    def test_ephemeral_skips_row_count(self):
        conn = Mock()
        concrete = _make_concrete_model(conn, _make_mock_model(Materialization.EPHEMERAL))

        metrics = concrete.execute()

        assert metrics.rows_affected is None
        assert metrics.materialization == "ephemeral"
        conn.get_table_row_count.assert_not_called()

    def test_table_all_rows_are_inserted(self):
        conn = Mock()
        conn.get_table_row_count.return_value = 500
        concrete = _make_concrete_model(conn, _make_mock_model(Materialization.TABLE))

        metrics = concrete.execute()

        assert metrics.rows_affected == 500
        assert metrics.rows_inserted == 500
        assert metrics.rows_updated == 0
        assert metrics.rows_deleted == 0
        assert metrics.materialization == "table"

    def test_table_with_count_failure(self):
        conn = Mock()
        conn.get_table_row_count.side_effect = Exception("fail")
        concrete = _make_concrete_model(conn, _make_mock_model(Materialization.TABLE))

        metrics = concrete.execute()

        assert metrics.rows_affected is None
        assert metrics.rows_inserted is None
        assert metrics.rows_updated == 0

    def test_view_skips_row_count(self):
        conn = Mock()
        concrete = _make_concrete_model(conn, _make_mock_model(Materialization.VIEW))

        metrics = concrete.execute()

        assert metrics.rows_affected is None
        assert metrics.materialization == "view"
        conn.get_table_row_count.assert_not_called()

    def test_incremental_without_upsert_metrics(self):
        conn = Mock()
        conn.get_table_row_count.return_value = 1000
        concrete = _make_concrete_model(conn, _make_mock_model(Materialization.INCREMENTAL))

        metrics = concrete.execute()

        assert metrics.rows_affected == 1000
        assert metrics.rows_inserted is None
        assert metrics.rows_updated is None
        assert metrics.rows_deleted is None
        assert metrics.materialization == "incremental"

    def test_incremental_with_upsert_metrics(self):
        conn = Mock()
        conn.get_table_row_count.return_value = 1000
        concrete = _make_concrete_model(conn, _make_mock_model(Materialization.INCREMENTAL))

        def mock_incremental():
            concrete._upsert_metrics = {
                "rows_inserted": 150,
                "rows_updated": 30,
                "rows_deleted": 20,
            }

        concrete.execute_incremental = mock_incremental

        metrics = concrete.execute()

        assert metrics.rows_affected == 1000  # from COUNT
        assert metrics.rows_inserted == 150   # from upsert
        assert metrics.rows_updated == 30
        assert metrics.rows_deleted == 20

    def test_incremental_with_partial_upsert_metrics(self):
        """Adapter only returns rows_affected, no breakdown."""
        conn = Mock()
        conn.get_table_row_count.return_value = 500
        concrete = _make_concrete_model(conn, _make_mock_model(Materialization.INCREMENTAL))

        def mock_incremental():
            concrete._upsert_metrics = {"rows_affected": 100}

        concrete.execute_incremental = mock_incremental

        metrics = concrete.execute()

        assert metrics.rows_affected == 500  # COUNT, not upsert
        assert metrics.rows_inserted is None
        assert metrics.rows_updated is None


# ============================================================================
# Per-adapter model _upsert_metrics threading
# ============================================================================

def _make_incremental_model():
    model = _make_mock_model(Materialization.INCREMENTAL)
    model.destination_table_exists = True
    model.primary_key = "id"
    model.select_if_incremental.return_value = Mock()
    return model


class TestPostgresModelMetrics:

    def test_merge_sets_upsert_metrics(self):
        from visitran.adapters.postgres.model import PostgresModel

        conn = Mock()
        conn.upsert_into_table.return_value = {"rows_affected": 42}

        db_model = PostgresModel(conn, _make_incremental_model())
        db_model._has_schema_changed = Mock(return_value=False)
        db_model.execute_incremental()

        assert db_model._upsert_metrics == {"rows_affected": 42}

    def test_append_no_upsert_metrics(self):
        from visitran.adapters.postgres.model import PostgresModel

        conn = Mock()
        model = _make_incremental_model()
        model.primary_key = None

        db_model = PostgresModel(conn, model)
        db_model._has_schema_changed = Mock(return_value=False)
        db_model.execute_incremental()

        assert db_model._upsert_metrics is None


class TestSnowflakeModelMetrics:

    def test_merge_sets_upsert_metrics(self):
        from visitran.adapters.snowflake.model import SnowflakeModel

        conn = Mock()
        conn.upsert_into_table.return_value = {"rows_affected": 88}

        db_model = SnowflakeModel(conn, _make_incremental_model())
        db_model._has_schema_changed = Mock(return_value=False)
        db_model.execute_incremental()

        assert db_model._upsert_metrics == {"rows_affected": 88}


class TestDatabricksModelMetrics:

    def test_merge_sets_upsert_metrics(self):
        from visitran.adapters.databricks.model import DatabricksModel

        conn = Mock()
        conn.upsert_into_table.return_value = {"rows_affected": 300}

        db_model = DatabricksModel(conn, _make_incremental_model())
        db_model._has_schema_changed = Mock(return_value=False)
        db_model.execute_incremental()

        assert db_model._upsert_metrics == {"rows_affected": 300}


class TestTrinoModelMetrics:

    def test_merge_sets_upsert_metrics(self):
        from visitran.adapters.trino.model import TrinoModel

        conn = Mock()
        conn.upsert_into_table.return_value = {
            "rows_affected": 80,
            "rows_inserted": 50,
            "rows_deleted": 30,
        }

        db_model = TrinoModel(conn, _make_incremental_model())
        db_model._has_schema_changed = Mock(return_value=False)
        db_model.execute_incremental()

        assert db_model._upsert_metrics["rows_inserted"] == 50
        assert db_model._upsert_metrics["rows_deleted"] == 30


class TestBigQueryModelMetrics:

    def test_merge_sets_upsert_metrics(self):
        pytest.importorskip("google.cloud.bigquery", reason="google-cloud-bigquery not installed")
        from visitran.adapters.bigquery.model import BigQueryModel

        conn = Mock()
        conn.merge_into_table.return_value = {"rows_affected": None}

        db_model = BigQueryModel(conn, _make_incremental_model())
        db_model.execute_incremental()

        assert db_model._upsert_metrics == {"rows_affected": None}


class TestDuckDbModelMetrics:

    def test_incremental_no_upsert_metrics(self):
        pytest.importorskip("duckdb", reason="duckdb not installed")
        from visitran.adapters.duckdb.model import DuckDbModel

        conn = Mock()
        db_model = DuckDbModel(conn, _make_incremental_model())
        db_model.execute_incremental()

        assert db_model._upsert_metrics is None


# ============================================================================
# Celery result JSON aggregation
# ============================================================================

class TestCeleryResultAggregation:
    """Test the aggregation logic replicated from celery_tasks.py."""

    def _aggregate(self, user_results):
        def _clean_name(n):
            if "'" in n:
                return n.split("'")[1].split(".")[-1]
            return n

        return {
            "models": [
                {
                    "name": _clean_name(r.node_name),
                    "rows_affected": getattr(r, "rows_affected", None),
                    "rows_inserted": getattr(r, "rows_inserted", None),
                    "rows_updated": getattr(r, "rows_updated", None),
                    "rows_deleted": getattr(r, "rows_deleted", None),
                    "type": getattr(r, "materialization", "") or "",
                    "duration_ms": getattr(r, "duration_ms", None),
                }
                for r in user_results
            ],
            "total": len(user_results),
            "passed": sum(1 for r in user_results if r.end_status == "OK"),
            "failed": sum(1 for r in user_results if r.end_status == "FAIL"),
            "rows_processed": sum(getattr(r, "rows_affected", 0) or 0 for r in user_results) or None,
            "rows_added": sum(getattr(r, "rows_inserted", 0) or 0 for r in user_results) or None,
            "rows_modified": sum(getattr(r, "rows_updated", 0) or 0 for r in user_results) or None,
            "rows_deleted": sum(getattr(r, "rows_deleted", 0) or 0 for r in user_results) or None,
        }

    def _result(self, **kwargs):
        defaults = dict(
            node_name="model", status="SUCCESS", info_message="",
            failures=False, ending_time=datetime.datetime.now(),
            sequence_num=1, end_status="OK",
        )
        defaults.update(kwargs)
        return BaseResult(**defaults)

    def test_mixed_table_and_incremental(self):
        results = [
            self._result(rows_affected=500, rows_inserted=500, rows_updated=0, rows_deleted=0, materialization="table"),
            self._result(rows_affected=200, rows_inserted=100, rows_updated=80, rows_deleted=20, materialization="incremental", sequence_num=2),
        ]
        j = self._aggregate(results)

        assert j["rows_processed"] == 700
        assert j["rows_added"] == 600
        assert j["rows_modified"] == 80
        assert j["rows_deleted"] == 20
        assert j["passed"] == 2

    def test_all_none_views_returns_none_not_zero(self):
        results = [
            self._result(rows_affected=None, materialization="view"),
            self._result(rows_affected=None, materialization="ephemeral", sequence_num=2),
        ]
        j = self._aggregate(results)

        assert j["rows_processed"] is None
        assert j["rows_added"] is None

    def test_class_name_cleaned(self):
        r = self._result(node_name="<class 'project.models.stg_orders.StgOrders'>")
        j = self._aggregate([r])

        assert j["models"][0]["name"] == "StgOrders"

    def test_failed_model_counted(self):
        results = [
            self._result(rows_affected=100, rows_inserted=100),
            self._result(node_name="bad", status="ERROR", failures=True, end_status="FAIL", sequence_num=2),
        ]
        j = self._aggregate(results)

        assert j["passed"] == 1
        assert j["failed"] == 1
        assert j["rows_processed"] == 100
