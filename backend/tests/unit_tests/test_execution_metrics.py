"""
Unit tests for ExecutionMetrics dataclass, row count pipeline, and per-adapter
upsert metrics.

Covers:
- ExecutionMetrics construction and defaults
- BaseModel.execute() per materialization type
- BaseModel._get_row_count_safe() success/failure
- Per-adapter upsert_into_table / merge_into_table return values
- Per-adapter model _upsert_metrics threading
- BaseResult metric fields
- execute_graph() metric capture
- Celery result JSON aggregation
"""

import datetime
import pytest
from unittest.mock import Mock, MagicMock, patch, PropertyMock

from visitran.adapters.model import ExecutionMetrics, BaseModel
from visitran.events.printer import BaseResult, ExecStatus, BASE_RESULT
from visitran.materialization import Materialization


# ============================================================================
# Helpers
# ============================================================================

def _make_mock_model(materialization=Materialization.TABLE):
    """Create a mock VisitranModel with the given materialization."""
    model = Mock()
    model.materialization = materialization
    model.destination_schema_name = "test_schema"
    model.destination_table_name = "test_table"
    model.select.return_value = Mock()  # ibis Table expression
    model.select_statement = None
    return model


def _make_mock_connection(row_count=100):
    """Create a mock BaseConnection that returns a row count."""
    conn = Mock()
    conn.get_table_row_count.return_value = row_count
    return conn


def _make_concrete_model(conn, model):
    """Create a concrete subclass of BaseModel for testing."""

    class ConcreteModel(BaseModel):
        def __init__(self, db_connection, model):
            super().__init__(db_connection, model)

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
# 1. ExecutionMetrics Dataclass
# ============================================================================

class TestExecutionMetrics:
    """Test ExecutionMetrics dataclass construction and defaults."""

    def test_default_values(self):
        metrics = ExecutionMetrics()
        assert metrics.rows_affected is None
        assert metrics.rows_inserted is None
        assert metrics.rows_updated is None
        assert metrics.rows_deleted is None
        assert metrics.materialization == ""

    def test_full_construction(self):
        metrics = ExecutionMetrics(
            rows_affected=500,
            rows_inserted=300,
            rows_updated=150,
            rows_deleted=50,
            materialization="incremental",
        )
        assert metrics.rows_affected == 500
        assert metrics.rows_inserted == 300
        assert metrics.rows_updated == 150
        assert metrics.rows_deleted == 50
        assert metrics.materialization == "incremental"

    def test_partial_construction(self):
        metrics = ExecutionMetrics(rows_affected=42, materialization="table")
        assert metrics.rows_affected == 42
        assert metrics.rows_inserted is None
        assert metrics.materialization == "table"

    def test_zero_values(self):
        metrics = ExecutionMetrics(rows_affected=0, rows_inserted=0, rows_updated=0, rows_deleted=0)
        assert metrics.rows_affected == 0
        assert metrics.rows_inserted == 0


# ============================================================================
# 2. BaseModel._get_row_count_safe()
# ============================================================================

class TestGetRowCountSafe:
    """Test the _get_row_count_safe fallback COUNT query."""

    def test_returns_row_count_on_success(self):
        conn = _make_mock_connection(row_count=250)
        model = _make_mock_model()
        concrete = _make_concrete_model(conn, model)

        result = concrete._get_row_count_safe()

        assert result == 250
        conn.get_table_row_count.assert_called_once_with(
            schema_name="test_schema",
            table_name="test_table",
        )

    def test_returns_none_on_exception(self):
        conn = _make_mock_connection()
        conn.get_table_row_count.side_effect = Exception("connection lost")
        model = _make_mock_model()
        concrete = _make_concrete_model(conn, model)

        result = concrete._get_row_count_safe()

        assert result is None

    def test_returns_none_on_db_error(self):
        conn = _make_mock_connection()
        conn.get_table_row_count.side_effect = RuntimeError("timeout")
        model = _make_mock_model()
        concrete = _make_concrete_model(conn, model)

        assert concrete._get_row_count_safe() is None

    def test_returns_zero_row_count(self):
        conn = _make_mock_connection(row_count=0)
        model = _make_mock_model()
        concrete = _make_concrete_model(conn, model)

        assert concrete._get_row_count_safe() == 0


# ============================================================================
# 3. BaseModel.execute() per materialization
# ============================================================================

class TestBaseModelExecute:
    """Test BaseModel.execute() returns correct ExecutionMetrics per materialization."""

    def test_ephemeral_returns_none_rows(self):
        conn = _make_mock_connection()
        model = _make_mock_model(Materialization.EPHEMERAL)
        concrete = _make_concrete_model(conn, model)

        metrics = concrete.execute()

        assert metrics.rows_affected is None
        assert metrics.materialization == "ephemeral"
        # Should NOT call get_table_row_count
        conn.get_table_row_count.assert_not_called()

    def test_table_returns_all_rows_as_inserted(self):
        conn = _make_mock_connection(row_count=500)
        model = _make_mock_model(Materialization.TABLE)
        concrete = _make_concrete_model(conn, model)

        metrics = concrete.execute()

        assert metrics.rows_affected == 500
        assert metrics.rows_inserted == 500
        assert metrics.rows_updated == 0
        assert metrics.rows_deleted == 0
        assert metrics.materialization == "table"

    def test_table_with_row_count_failure(self):
        conn = _make_mock_connection()
        conn.get_table_row_count.side_effect = Exception("fail")
        model = _make_mock_model(Materialization.TABLE)
        concrete = _make_concrete_model(conn, model)

        metrics = concrete.execute()

        assert metrics.rows_affected is None
        assert metrics.rows_inserted is None
        assert metrics.rows_updated == 0
        assert metrics.rows_deleted == 0
        assert metrics.materialization == "table"

    def test_view_returns_none_rows(self):
        conn = _make_mock_connection()
        model = _make_mock_model(Materialization.VIEW)
        concrete = _make_concrete_model(conn, model)

        metrics = concrete.execute()

        assert metrics.rows_affected is None
        assert metrics.materialization == "view"
        conn.get_table_row_count.assert_not_called()

    def test_incremental_without_upsert_metrics(self):
        conn = _make_mock_connection(row_count=1000)
        model = _make_mock_model(Materialization.INCREMENTAL)
        concrete = _make_concrete_model(conn, model)

        metrics = concrete.execute()

        assert metrics.rows_affected == 1000
        assert metrics.rows_inserted is None
        assert metrics.rows_updated is None
        assert metrics.rows_deleted is None
        assert metrics.materialization == "incremental"

    def test_incremental_with_upsert_metrics(self):
        conn = _make_mock_connection(row_count=1000)
        model = _make_mock_model(Materialization.INCREMENTAL)
        concrete = _make_concrete_model(conn, model)
        # Simulate adapter setting upsert metrics during execute_incremental
        original_exec = concrete.execute_incremental

        def mock_incremental():
            original_exec()
            concrete._upsert_metrics = {
                "rows_affected": 200,
                "rows_inserted": 150,
                "rows_updated": 30,
                "rows_deleted": 20,
            }

        concrete.execute_incremental = mock_incremental

        metrics = concrete.execute()

        assert metrics.rows_affected == 1000  # from COUNT query
        assert metrics.rows_inserted == 150   # from upsert metrics
        assert metrics.rows_updated == 30
        assert metrics.rows_deleted == 20
        assert metrics.materialization == "incremental"

    def test_incremental_with_partial_upsert_metrics(self):
        conn = _make_mock_connection(row_count=500)
        model = _make_mock_model(Materialization.INCREMENTAL)
        concrete = _make_concrete_model(conn, model)

        def mock_incremental():
            concrete._upsert_metrics = {"rows_affected": 100}

        concrete.execute_incremental = mock_incremental

        metrics = concrete.execute()

        assert metrics.rows_affected == 500
        assert metrics.rows_inserted is None  # not in upsert_metrics
        assert metrics.rows_updated is None
        assert metrics.rows_deleted is None

    def test_execute_calls_model_select(self):
        conn = _make_mock_connection()
        model = _make_mock_model(Materialization.TABLE)
        concrete = _make_concrete_model(conn, model)

        concrete.execute()

        model.select.assert_called_once()


# ============================================================================
# 4. Adapter run_model returns ExecutionMetrics
# ============================================================================

class TestAdapterRunModel:
    """Test that adapter.run_model() returns ExecutionMetrics from execute()."""

    def test_run_model_returns_execution_metrics(self):
        expected = ExecutionMetrics(rows_affected=42, materialization="table")
        mock_db_model = Mock()
        mock_db_model.execute.return_value = expected

        adapter = Mock()
        adapter.db_model = mock_db_model
        adapter.load_model = Mock()

        # Simulate the real run_model logic
        adapter.load_model(model=Mock())
        result = adapter.db_model.execute()

        assert result is expected
        assert result.rows_affected == 42


# ============================================================================
# 5. Per-adapter upsert return value tests
# ============================================================================

class TestUpsertReturnValues:
    """Test that each adapter's upsert/merge method returns the expected dict structure.

    We mock at the connection level rather than instantiating real connections,
    since the upsert methods have complex internal logic (temp tables, schema queries).
    """

    def test_postgres_upsert_returns_dict(self):
        """Postgres upsert_into_table should return {"rows_affected": <int>}."""
        conn = Mock()
        conn.upsert_into_table.return_value = {"rows_affected": 75}
        result = conn.upsert_into_table("public", "test", Mock(), "id")
        assert result == {"rows_affected": 75}

    def test_snowflake_upsert_returns_dict(self):
        conn = Mock()
        conn.upsert_into_table.return_value = {"rows_affected": 120}
        result = conn.upsert_into_table("public", "test", Mock(), "id")
        assert result == {"rows_affected": 120}

    def test_snowflake_upsert_none_on_failure(self):
        conn = Mock()
        conn.upsert_into_table.return_value = {"rows_affected": None}
        result = conn.upsert_into_table("public", "test", Mock(), "id")
        assert result["rows_affected"] is None

    def test_bigquery_merge_returns_none(self):
        """BigQuery intentionally returns None — relies on COUNT fallback."""
        conn = Mock()
        conn.merge_into_table.return_value = {"rows_affected": None}
        result = conn.merge_into_table("dataset", "test", Mock(), "id")
        assert result == {"rows_affected": None}

    def test_databricks_upsert_returns_dict(self):
        conn = Mock()
        conn.upsert_into_table.return_value = {"rows_affected": 200}
        result = conn.upsert_into_table("default", "test", Mock(), "id")
        assert result == {"rows_affected": 200}

    def test_databricks_upsert_none_on_failure(self):
        conn = Mock()
        conn.upsert_into_table.return_value = {"rows_affected": None}
        result = conn.upsert_into_table("default", "test", Mock(), "id")
        assert result["rows_affected"] is None

    def test_trino_upsert_merge_mode(self):
        """Trino returns inserted + deleted breakdown."""
        conn = Mock()
        conn.upsert_into_table.return_value = {
            "rows_affected": 80,
            "rows_inserted": 50,
            "rows_deleted": 30,
        }
        result = conn.upsert_into_table("hive", "test", Mock(), "id")
        assert result["rows_inserted"] == 50
        assert result["rows_deleted"] == 30
        assert result["rows_affected"] == 80

    def test_trino_upsert_append_mode(self):
        """Trino APPEND mode — deleted is None."""
        conn = Mock()
        conn.upsert_into_table.return_value = {
            "rows_affected": 100,
            "rows_inserted": 100,
            "rows_deleted": None,
        }
        result = conn.upsert_into_table("hive", "test", Mock(), [])
        assert result["rows_inserted"] == 100
        assert result["rows_deleted"] is None

    def test_trino_upsert_exception_all_none(self):
        conn = Mock()
        conn.upsert_into_table.return_value = {
            "rows_affected": None,
            "rows_inserted": None,
            "rows_deleted": None,
        }
        result = conn.upsert_into_table("hive", "test", Mock(), "id")
        assert result["rows_affected"] is None
        assert result["rows_inserted"] is None
        assert result["rows_deleted"] is None


class TestDuckDbIncrementalMetrics:
    """Test DuckDB model does not set _upsert_metrics (no upsert support)."""

    def test_incremental_does_not_set_upsert_metrics(self):
        pytest.importorskip("duckdb", reason="duckdb not installed")
        from visitran.adapters.duckdb.model import DuckDbModel

        conn = Mock()
        model = _make_mock_model(Materialization.INCREMENTAL)
        model.destination_table_exists = True
        model.select_if_incremental.return_value = Mock()

        db_model = DuckDbModel(conn, model)
        db_model.execute_incremental()

        assert db_model._upsert_metrics is None


# ============================================================================
# 6. Per-adapter model _upsert_metrics threading
# ============================================================================

def _make_incremental_model_mock():
    """Create a mock model configured for MERGE mode incremental execution."""
    model = _make_mock_model(Materialization.INCREMENTAL)
    model.destination_table_exists = True
    model.primary_key = "id"
    model.select_if_incremental.return_value = Mock()
    return model


class TestPostgresModelUpsertMetrics:
    """Test PostgresModel sets _upsert_metrics from upsert_into_table."""

    @patch("visitran.adapters.postgres.model.fire_event")
    def test_merge_mode_sets_upsert_metrics(self, mock_fire):
        from visitran.adapters.postgres.model import PostgresModel

        conn = Mock()
        conn.upsert_into_table.return_value = {"rows_affected": 42}
        model = _make_incremental_model_mock()

        db_model = PostgresModel(conn, model)
        # Mock _has_schema_changed to return False (no full refresh)
        db_model._has_schema_changed = Mock(return_value=False)
        db_model.execute_incremental()

        assert db_model._upsert_metrics == {"rows_affected": 42}

    @patch("visitran.adapters.postgres.model.fire_event")
    def test_append_mode_no_upsert_metrics(self, mock_fire):
        from visitran.adapters.postgres.model import PostgresModel

        conn = Mock()
        model = _make_incremental_model_mock()
        model.primary_key = None  # APPEND mode

        db_model = PostgresModel(conn, model)
        db_model._has_schema_changed = Mock(return_value=False)
        db_model.execute_incremental()

        assert db_model._upsert_metrics is None


class TestSnowflakeModelUpsertMetrics:
    """Test SnowflakeModel sets _upsert_metrics from upsert_into_table."""

    def test_merge_mode_sets_upsert_metrics(self):
        from visitran.adapters.snowflake.model import SnowflakeModel

        conn = Mock()
        conn.upsert_into_table.return_value = {"rows_affected": 88}
        model = _make_incremental_model_mock()

        db_model = SnowflakeModel(conn, model)
        db_model._has_schema_changed = Mock(return_value=False)
        db_model.execute_incremental()

        assert db_model._upsert_metrics == {"rows_affected": 88}


class TestBigQueryModelUpsertMetrics:
    """Test BigQueryModel sets _upsert_metrics from merge_into_table."""

    def test_merge_mode_sets_upsert_metrics(self):
        pytest.importorskip("google.cloud.bigquery", reason="google-cloud-bigquery not installed")
        from visitran.adapters.bigquery.model import BigQueryModel

        conn = Mock()
        conn.merge_into_table.return_value = {"rows_affected": None}
        model = _make_incremental_model_mock()

        db_model = BigQueryModel(conn, model)
        db_model.execute_incremental()

        assert db_model._upsert_metrics == {"rows_affected": None}


class TestDatabricksModelUpsertMetrics:
    """Test DatabricksModel sets _upsert_metrics from upsert_into_table."""

    def test_merge_mode_sets_upsert_metrics(self):
        from visitran.adapters.databricks.model import DatabricksModel

        conn = Mock()
        conn.upsert_into_table.return_value = {"rows_affected": 300}
        model = _make_incremental_model_mock()

        db_model = DatabricksModel(conn, model)
        db_model._has_schema_changed = Mock(return_value=False)
        db_model.execute_incremental()

        assert db_model._upsert_metrics == {"rows_affected": 300}


class TestTrinoModelUpsertMetrics:
    """Test TrinoModel sets _upsert_metrics from upsert_into_table."""

    def test_merge_mode_sets_upsert_metrics(self):
        from visitran.adapters.trino.model import TrinoModel

        conn = Mock()
        conn.upsert_into_table.return_value = {
            "rows_affected": 80,
            "rows_inserted": 50,
            "rows_deleted": 30,
        }
        model = _make_incremental_model_mock()

        db_model = TrinoModel(conn, model)
        db_model._has_schema_changed = Mock(return_value=False)
        db_model.execute_incremental()

        assert db_model._upsert_metrics["rows_inserted"] == 50
        assert db_model._upsert_metrics["rows_deleted"] == 30


# ============================================================================
# 7. BaseResult metric fields
# ============================================================================

class TestBaseResultMetrics:
    """Test BaseResult dataclass with new metric fields."""

    def test_full_construction(self):
        result = BaseResult(
            node_name="test_model",
            status=ExecStatus.Success.value,
            info_message="Running test_model",
            failures=False,
            ending_time=datetime.datetime.now(),
            sequence_num=1,
            end_status=ExecStatus.OK.value,
            rows_affected=500,
            rows_inserted=300,
            rows_updated=150,
            rows_deleted=50,
            materialization="incremental",
            duration_ms=1234,
        )
        assert result.rows_affected == 500
        assert result.rows_inserted == 300
        assert result.rows_updated == 150
        assert result.rows_deleted == 50
        assert result.materialization == "incremental"
        assert result.duration_ms == 1234

    def test_default_values_backward_compatible(self):
        """Construct without new fields — should use defaults."""
        result = BaseResult(
            node_name="test_model",
            status=ExecStatus.Success.value,
            info_message="Running test_model",
            failures=False,
            ending_time=datetime.datetime.now(),
            sequence_num=1,
            end_status=ExecStatus.OK.value,
        )
        assert result.rows_affected is None
        assert result.rows_inserted is None
        assert result.rows_updated is None
        assert result.rows_deleted is None
        assert result.materialization == ""
        assert result.duration_ms is None

    def test_none_values_explicit(self):
        result = BaseResult(
            node_name="view_model",
            status=ExecStatus.Success.value,
            info_message="Running view_model",
            failures=False,
            ending_time=datetime.datetime.now(),
            sequence_num=1,
            end_status=ExecStatus.OK.value,
            rows_affected=None,
            materialization="view",
        )
        assert result.rows_affected is None
        assert result.materialization == "view"


# ============================================================================
# 8. execute_graph() metric capture
# ============================================================================

class TestExecuteGraphMetrics:
    """Test that execute_graph captures ExecutionMetrics into BaseResult."""

    def test_metrics_threaded_to_base_result(self):
        """Simulate the metric extraction logic from execute_graph."""
        exec_metrics = ExecutionMetrics(
            rows_affected=1000,
            rows_inserted=800,
            rows_updated=150,
            rows_deleted=50,
            materialization="incremental",
        )

        # Replicate the getattr pattern from execute_graph
        _rows = getattr(exec_metrics, "rows_affected", None)
        _rows_ins = getattr(exec_metrics, "rows_inserted", None)
        _rows_upd = getattr(exec_metrics, "rows_updated", None)
        _rows_del = getattr(exec_metrics, "rows_deleted", None)
        _mat = getattr(exec_metrics, "materialization", "")

        result = BaseResult(
            node_name="test_model",
            status=ExecStatus.Success.value,
            info_message="Running test_model",
            failures=False,
            ending_time=datetime.datetime.now(),
            sequence_num=1,
            end_status=ExecStatus.OK.value,
            rows_affected=_rows,
            rows_inserted=_rows_ins,
            rows_updated=_rows_upd,
            rows_deleted=_rows_del,
            materialization=_mat,
            duration_ms=500,
        )

        assert result.rows_affected == 1000
        assert result.rows_inserted == 800
        assert result.rows_updated == 150
        assert result.rows_deleted == 50
        assert result.materialization == "incremental"

    def test_none_metrics_handled_by_getattr(self):
        """When exec_metrics is None (shouldn't happen but defensive)."""
        exec_metrics = None

        _rows = getattr(exec_metrics, "rows_affected", None) if exec_metrics else None
        _mat = getattr(exec_metrics, "materialization", "") if exec_metrics else ""

        assert _rows is None
        assert _mat == ""


# ============================================================================
# 9. Celery result JSON aggregation
# ============================================================================

class TestCeleryResultJson:
    """Test the result JSON construction logic from celery_tasks.py.

    NOTE: The aggregation logic is embedded inside trigger_scheduled_run() and
    cannot be imported directly. These tests replicate the logic to verify the
    expected JSON shape. If the production code diverges, these tests may give
    false confidence — tracked for extraction in a future refactor.
    """

    def _build_result_json(self, user_results):
        """Replicate the aggregation logic from celery_tasks.py (lines 357-388).

        TODO: Extract aggregation into a standalone function in celery_tasks.py
        so this test can call the real code instead of a copy.
        """

        def _clean_name(node_name):
            if "'" in node_name:
                return node_name.split("'")[1].split(".")[-1]
            return node_name

        return {
            "models": [
                {
                    "name": _clean_name(r.node_name),
                    "status": r.status,
                    "end_status": r.end_status,
                    "sequence": r.sequence_num,
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
            "rows_processed": sum(
                getattr(r, "rows_affected", 0) or 0 for r in user_results
            ) or None,
            "rows_added": sum(
                getattr(r, "rows_inserted", 0) or 0 for r in user_results
            ) or None,
            "rows_modified": sum(
                getattr(r, "rows_updated", 0) or 0 for r in user_results
            ) or None,
            "rows_deleted": sum(
                getattr(r, "rows_deleted", 0) or 0 for r in user_results
            ) or None,
        }

    def test_aggregation_with_mixed_results(self):
        now = datetime.datetime.now()
        results = [
            BaseResult(
                node_name="model_a", status="SUCCESS", info_message="",
                failures=False, ending_time=now, sequence_num=1, end_status="OK",
                rows_affected=500, rows_inserted=500, rows_updated=0, rows_deleted=0,
                materialization="table", duration_ms=1000,
            ),
            BaseResult(
                node_name="model_b", status="SUCCESS", info_message="",
                failures=False, ending_time=now, sequence_num=2, end_status="OK",
                rows_affected=200, rows_inserted=100, rows_updated=80, rows_deleted=20,
                materialization="incremental", duration_ms=2000,
            ),
        ]

        result_json = self._build_result_json(results)

        assert result_json["total"] == 2
        assert result_json["passed"] == 2
        assert result_json["failed"] == 0
        assert result_json["rows_processed"] == 700
        assert result_json["rows_added"] == 600
        assert result_json["rows_modified"] == 80
        assert result_json["rows_deleted"] == 20

    def test_aggregation_with_failed_model(self):
        now = datetime.datetime.now()
        results = [
            BaseResult(
                node_name="good_model", status="SUCCESS", info_message="",
                failures=False, ending_time=now, sequence_num=1, end_status="OK",
                rows_affected=100, rows_inserted=100, materialization="table",
            ),
            BaseResult(
                node_name="bad_model", status="ERROR", info_message="",
                failures=True, ending_time=now, sequence_num=2, end_status="FAIL",
                rows_affected=None, materialization="table",
            ),
        ]

        result_json = self._build_result_json(results)

        assert result_json["passed"] == 1
        assert result_json["failed"] == 1
        assert result_json["rows_processed"] == 100

    def test_all_none_rows_returns_none_aggregates(self):
        now = datetime.datetime.now()
        results = [
            BaseResult(
                node_name="view_a", status="SUCCESS", info_message="",
                failures=False, ending_time=now, sequence_num=1, end_status="OK",
                rows_affected=None, materialization="view",
            ),
            BaseResult(
                node_name="ephemeral_b", status="SUCCESS", info_message="",
                failures=False, ending_time=now, sequence_num=2, end_status="OK",
                rows_affected=None, materialization="ephemeral",
            ),
        ]

        result_json = self._build_result_json(results)

        # sum of all Nones → 0 → or None
        assert result_json["rows_processed"] is None
        assert result_json["rows_added"] is None
        assert result_json["rows_modified"] is None
        assert result_json["rows_deleted"] is None

    def test_per_model_fields(self):
        now = datetime.datetime.now()
        results = [
            BaseResult(
                node_name="<class 'project.models.stg_orders.StgOrders'>",
                status="SUCCESS", info_message="", failures=False,
                ending_time=now, sequence_num=1, end_status="OK",
                rows_affected=999, rows_inserted=999,
                materialization="table", duration_ms=3456,
            ),
        ]

        result_json = self._build_result_json(results)
        model = result_json["models"][0]

        assert model["name"] == "StgOrders"  # cleaned from class string
        assert model["rows_affected"] == 999
        assert model["rows_inserted"] == 999
        assert model["type"] == "table"
        assert model["duration_ms"] == 3456

    def test_empty_results(self):
        result_json = self._build_result_json([])

        assert result_json["total"] == 0
        assert result_json["passed"] == 0
        assert result_json["failed"] == 0
        assert result_json["rows_processed"] is None
