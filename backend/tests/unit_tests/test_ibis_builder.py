"""Unit tests for IbisBuilder."""

from unittest.mock import MagicMock, patch
import pytest

from backend.application.config_parser.ibis_builder import (
    RefResolution,
    CompilationResult,
    TransformationStep,
    IbisBuildError,
    MissingReferenceError,
    IbisBuilder,
    create_ibis_builder,
    REF_PATTERN,
)
from backend.application.config_parser.model_registry import ModelRegistry
from backend.application.config_parser.config_parser import ConfigParser


class TestRefResolution:
    """Tests for RefResolution dataclass."""

    def test_creation(self):
        """Test creating RefResolution with all fields."""
        ref = RefResolution(
            schema="public",
            model="orders",
            qualified_name="public.orders",
            original_text="ref('public', 'orders')",
            start_pos=0,
            end_pos=24,
        )

        assert ref.schema == "public"
        assert ref.model == "orders"
        assert ref.qualified_name == "public.orders"
        assert ref.original_text == "ref('public', 'orders')"
        assert ref.start_pos == 0
        assert ref.end_pos == 24


class TestCompilationResult:
    """Tests for CompilationResult dataclass."""

    def test_creation_minimal(self):
        """Test creating CompilationResult with minimal fields."""
        mock_table = MagicMock()
        result = CompilationResult(table=mock_table)

        assert result.table is mock_table
        assert result.resolved_refs == []
        assert result.sql_original == ""
        assert result.sql_resolved == ""
        assert result.warnings == []

    def test_creation_full(self):
        """Test creating CompilationResult with all fields."""
        mock_table = MagicMock()
        refs = [
            RefResolution(
                schema="p", model="m", qualified_name="p.m",
                original_text="ref('p', 'm')", start_pos=0, end_pos=13
            )
        ]
        result = CompilationResult(
            table=mock_table,
            resolved_refs=refs,
            sql_original="SELECT * FROM ref('p', 'm')",
            sql_resolved="SELECT * FROM p.m",
            warnings=["test warning"],
        )

        assert len(result.resolved_refs) == 1
        assert result.sql_original == "SELECT * FROM ref('p', 'm')"
        assert result.sql_resolved == "SELECT * FROM p.m"
        assert len(result.warnings) == 1


class TestTransformationStep:
    """Tests for TransformationStep dataclass."""

    def test_creation_minimal(self):
        """Test creating TransformationStep with minimal fields."""
        step = TransformationStep(operation="sql", sql="SELECT * FROM orders")

        assert step.operation == "sql"
        assert step.sql == "SELECT * FROM orders"
        assert step.params == {}
        assert step.line_number is None

    def test_creation_full(self):
        """Test creating TransformationStep with all fields."""
        step = TransformationStep(
            operation="join",
            sql="ref('p', 'customers')",
            params={"on": "customer_id", "how": "inner"},
            line_number=42,
        )

        assert step.operation == "join"
        assert step.params["on"] == "customer_id"
        assert step.line_number == 42


class TestIbisBuildError:
    """Tests for IbisBuildError exception."""

    def test_creation_minimal(self):
        """Test creating IbisBuildError with minimal fields."""
        error = IbisBuildError("Test error")

        assert error.message == "Test error"
        assert error.sql is None
        assert error.line_number is None
        assert error.column_number is None

    def test_creation_full(self):
        """Test creating IbisBuildError with all fields."""
        error = IbisBuildError(
            "Test error",
            sql="SELECT * FROM broken",
            line_number=10,
            column_number=5,
        )

        assert error.message == "Test error"
        assert error.sql == "SELECT * FROM broken"
        assert error.line_number == 10
        assert error.column_number == 5

    def test_to_transformation_error(self):
        """Test conversion to TransformationError."""
        error = IbisBuildError(
            "SQL failed",
            sql="SELECT *",
            line_number=15,
        )
        te = error.to_transformation_error("test_model")

        assert te.model_name == "test_model"
        assert "SQL failed" in te.error_message
        assert te.line_number == 15


class TestMissingReferenceError:
    """Tests for MissingReferenceError exception."""

    def test_creation(self):
        """Test creating MissingReferenceError."""
        error = MissingReferenceError("public", "orders")

        assert error.schema == "public"
        assert error.model == "orders"
        assert "public" in error.message
        assert "orders" in error.message

    def test_creation_with_sql(self):
        """Test creating MissingReferenceError with SQL context."""
        error = MissingReferenceError(
            "analytics",
            "summary",
            sql="SELECT * FROM ref('analytics', 'summary')",
            line_number=25,
        )

        assert error.sql is not None
        assert error.line_number == 25


class TestRefPattern:
    """Tests for the REF_PATTERN regex."""

    def test_single_quotes(self):
        """Test matching ref with single quotes."""
        match = REF_PATTERN.search("ref('public', 'orders')")
        assert match is not None
        assert match.group(1) == "public"
        assert match.group(2) == "orders"

    def test_double_quotes(self):
        """Test matching ref with double quotes."""
        match = REF_PATTERN.search('ref("analytics", "summary")')
        assert match is not None
        assert match.group(1) == "analytics"
        assert match.group(2) == "summary"

    def test_with_spaces(self):
        """Test matching ref with extra spaces."""
        match = REF_PATTERN.search("ref(  'public'  ,  'orders'  )")
        assert match is not None
        assert match.group(1) == "public"
        assert match.group(2) == "orders"

    def test_case_insensitive(self):
        """Test matching REF with different cases."""
        match = REF_PATTERN.search("REF('public', 'orders')")
        assert match is not None

        match = REF_PATTERN.search("Ref('public', 'orders')")
        assert match is not None

    def test_no_match_invalid(self):
        """Test that invalid patterns don't match."""
        # Missing quotes
        assert REF_PATTERN.search("ref(public, orders)") is None

        # Missing comma
        assert REF_PATTERN.search("ref('public' 'orders')") is None


class TestIbisBuilderInit:
    """Tests for IbisBuilder initialization."""

    def setup_method(self):
        """Reset singletons before each test."""
        ModelRegistry.reset_instance()
        ConfigParser._instances.clear()

    def test_init_empty(self):
        """Test initializing without arguments."""
        builder = IbisBuilder()

        assert builder.registry is None
        assert builder.connection is None

    def test_init_with_registry(self):
        """Test initializing with registry."""
        registry = ModelRegistry()
        builder = IbisBuilder(registry=registry)

        assert builder.registry is registry

    def test_init_with_connection(self):
        """Test initializing with connection."""
        mock_conn = MagicMock()
        builder = IbisBuilder(connection=mock_conn)

        assert builder.connection is mock_conn

    def test_set_connection(self):
        """Test setting connection after init."""
        builder = IbisBuilder()
        mock_conn = MagicMock()
        builder.set_connection(mock_conn)

        assert builder.connection is mock_conn

    def test_set_registry(self):
        """Test setting registry after init."""
        builder = IbisBuilder()
        registry = ModelRegistry()
        builder.set_registry(registry)

        assert builder.registry is registry


class TestIbisBuilderFindRefs:
    """Tests for IbisBuilder.find_refs()."""

    def test_no_refs(self):
        """Test SQL with no refs."""
        builder = IbisBuilder()
        refs = builder.find_refs("SELECT * FROM orders")

        assert refs == []

    def test_single_ref(self):
        """Test SQL with single ref."""
        builder = IbisBuilder()
        refs = builder.find_refs("SELECT * FROM ref('public', 'orders')")

        assert len(refs) == 1
        assert refs[0].schema == "public"
        assert refs[0].model == "orders"
        assert refs[0].qualified_name == "public.orders"

    def test_multiple_refs(self):
        """Test SQL with multiple refs."""
        builder = IbisBuilder()
        sql = "SELECT * FROM ref('p', 'a') JOIN ref('p', 'b') ON a.id = b.id"
        refs = builder.find_refs(sql)

        assert len(refs) == 2
        assert refs[0].model == "a"
        assert refs[1].model == "b"

    def test_ref_positions(self):
        """Test that ref positions are correct."""
        builder = IbisBuilder()
        sql = "SELECT * FROM ref('public', 'orders')"
        refs = builder.find_refs(sql)

        assert len(refs) == 1
        assert refs[0].start_pos == 14
        assert sql[refs[0].start_pos : refs[0].end_pos] == "ref('public', 'orders')"


class TestIbisBuilderResolveRefsInSql:
    """Tests for IbisBuilder.resolve_refs_in_sql()."""

    def setup_method(self):
        """Reset singletons before each test."""
        ModelRegistry.reset_instance()
        ConfigParser._instances.clear()

    def _create_mock_config(self, schema, model):
        """Create a mock ConfigParser."""
        config = MagicMock(spec=ConfigParser)
        config.destination_schema_name = schema
        config.model_name = model
        return config

    def test_no_refs_returns_unchanged(self):
        """Test that SQL without refs is returned unchanged."""
        builder = IbisBuilder()
        sql = "SELECT * FROM orders"
        resolved, refs = builder.resolve_refs_in_sql(sql)

        assert resolved == sql
        assert refs == []

    def test_resolves_single_ref(self):
        """Test resolving single ref in SQL."""
        registry = ModelRegistry()
        config = self._create_mock_config("public", "orders")
        registry.register("public", "orders", config)

        builder = IbisBuilder(registry=registry)
        sql = "SELECT * FROM ref('public', 'orders')"
        resolved, refs = builder.resolve_refs_in_sql(sql)

        assert resolved == "SELECT * FROM public.orders"
        assert len(refs) == 1

    def test_resolves_multiple_refs(self):
        """Test resolving multiple refs in SQL."""
        registry = ModelRegistry()
        registry.register("p", "a", self._create_mock_config("p", "a"))
        registry.register("p", "b", self._create_mock_config("p", "b"))

        builder = IbisBuilder(registry=registry)
        sql = "SELECT * FROM ref('p', 'a') JOIN ref('p', 'b')"
        resolved, refs = builder.resolve_refs_in_sql(sql)

        assert "p.a" in resolved
        assert "p.b" in resolved
        assert "ref(" not in resolved
        assert len(refs) == 2

    def test_missing_ref_raises_error(self):
        """Test that missing ref raises MissingReferenceError."""
        registry = ModelRegistry()
        builder = IbisBuilder(registry=registry)

        with pytest.raises(MissingReferenceError) as exc_info:
            builder.resolve_refs_in_sql("SELECT * FROM ref('p', 'missing')")

        assert exc_info.value.schema == "p"
        assert exc_info.value.model == "missing"

    def test_no_registry_raises_error(self):
        """Test that missing registry raises IbisBuildError."""
        builder = IbisBuilder()

        with pytest.raises(IbisBuildError) as exc_info:
            builder.resolve_refs_in_sql("SELECT * FROM ref('p', 'a')")

        assert "ModelRegistry" in exc_info.value.message


class TestIbisBuilderResolveRef:
    """Tests for IbisBuilder.resolve_ref()."""

    def setup_method(self):
        """Reset singletons before each test."""
        ModelRegistry.reset_instance()
        ConfigParser._instances.clear()

    def test_no_registry_raises_error(self):
        """Test that missing registry raises error."""
        builder = IbisBuilder()

        with pytest.raises(IbisBuildError):
            builder.resolve_ref("p", "m")

    def test_missing_model_raises_error(self):
        """Test that missing model raises MissingReferenceError."""
        registry = ModelRegistry()
        builder = IbisBuilder(registry=registry)

        with pytest.raises(MissingReferenceError):
            builder.resolve_ref("p", "missing")

    def test_no_connection_raises_error(self):
        """Test that missing connection raises error."""
        registry = ModelRegistry()
        config = MagicMock(spec=ConfigParser)
        registry.register("p", "m", config)

        builder = IbisBuilder(registry=registry)

        with pytest.raises(IbisBuildError) as exc_info:
            builder.resolve_ref("p", "m")

        assert "no connection" in exc_info.value.message.lower()

    def test_caches_resolved_tables(self):
        """Test that resolved tables are cached."""
        registry = ModelRegistry()
        config = MagicMock(spec=ConfigParser)
        registry.register("p", "m", config)

        mock_conn = MagicMock()
        mock_table = MagicMock()
        mock_conn.table.return_value = mock_table

        builder = IbisBuilder(registry=registry, connection=mock_conn)

        # First call
        table1 = builder.resolve_ref("p", "m")
        # Second call should use cache
        table2 = builder.resolve_ref("p", "m")

        assert table1 is mock_table
        assert table2 is mock_table
        # Connection.table should only be called once
        mock_conn.table.assert_called_once()

    def test_clear_cache(self):
        """Test clearing the table cache."""
        registry = ModelRegistry()
        config = MagicMock(spec=ConfigParser)
        registry.register("p", "m", config)

        mock_conn = MagicMock()
        mock_table = MagicMock()
        mock_conn.table.return_value = mock_table

        builder = IbisBuilder(registry=registry, connection=mock_conn)

        # First call
        builder.resolve_ref("p", "m")
        builder.clear_cache()
        # Second call after cache clear
        builder.resolve_ref("p", "m")

        # Connection.table should be called twice
        assert mock_conn.table.call_count == 2


class TestIbisBuilderCompileTransformation:
    """Tests for IbisBuilder.compile_transformation()."""

    def setup_method(self):
        """Reset singletons before each test."""
        ModelRegistry.reset_instance()
        ConfigParser._instances.clear()

    def test_no_connection_raises_error(self):
        """Test that missing connection raises error."""
        builder = IbisBuilder()

        with pytest.raises(IbisBuildError) as exc_info:
            builder.compile_transformation("SELECT 1")

        assert "connection" in exc_info.value.message.lower()

    def test_successful_compilation(self):
        """Test successful SQL compilation."""
        mock_conn = MagicMock()
        mock_table = MagicMock()
        mock_conn.sql.return_value = mock_table

        builder = IbisBuilder(connection=mock_conn)
        result = builder.compile_transformation("SELECT 1")

        assert result.table is mock_table
        assert result.sql_original == "SELECT 1"
        assert result.sql_resolved == "SELECT 1"
        mock_conn.sql.assert_called_once_with("SELECT 1")

    def test_compilation_with_refs(self):
        """Test SQL compilation with model refs."""
        registry = ModelRegistry()
        config = MagicMock(spec=ConfigParser)
        registry.register("p", "orders", config)

        mock_conn = MagicMock()
        mock_table = MagicMock()
        mock_conn.sql.return_value = mock_table

        builder = IbisBuilder(registry=registry, connection=mock_conn)
        result = builder.compile_transformation("SELECT * FROM ref('p', 'orders')")

        assert result.table is mock_table
        assert len(result.resolved_refs) == 1
        assert result.resolved_refs[0].model == "orders"
        # Should compile with resolved ref
        mock_conn.sql.assert_called_once_with("SELECT * FROM p.orders")

    def test_compilation_failure(self):
        """Test that compilation failure raises error."""
        mock_conn = MagicMock()
        mock_conn.sql.side_effect = Exception("Parse error")

        builder = IbisBuilder(connection=mock_conn)

        with pytest.raises(IbisBuildError) as exc_info:
            builder.compile_transformation("INVALID SQL")

        assert "compilation failed" in exc_info.value.message.lower()


class TestIbisBuilderCompileChain:
    """Tests for IbisBuilder.compile_chain()."""

    def setup_method(self):
        """Reset singletons before each test."""
        ModelRegistry.reset_instance()
        ConfigParser._instances.clear()

    def test_empty_chain_raises_error(self):
        """Test that empty chain raises error."""
        builder = IbisBuilder()

        with pytest.raises(IbisBuildError) as exc_info:
            builder.compile_chain([])

        assert "No transformations" in exc_info.value.message

    def test_single_sql_transformation(self):
        """Test chain with single SQL transformation."""
        mock_conn = MagicMock()
        mock_table = MagicMock()
        mock_conn.sql.return_value = mock_table

        builder = IbisBuilder(connection=mock_conn)
        steps = [TransformationStep(operation="sql", sql="SELECT 1")]
        result = builder.compile_chain(steps)

        assert result.table is mock_table

    def test_chained_transformations(self):
        """Test multiple chained transformations."""
        mock_conn = MagicMock()
        mock_table1 = MagicMock()
        mock_table2 = MagicMock()
        mock_table1.filter.return_value = mock_table2
        mock_conn.sql.return_value = mock_table1

        builder = IbisBuilder(connection=mock_conn)
        steps = [
            TransformationStep(operation="sql", sql="SELECT * FROM orders"),
            TransformationStep(operation="select", sql="id, name"),  # Use select instead
        ]

        # Verify chaining works
        result = builder.compile_chain(steps)
        assert result.table is not None

    def test_unknown_operation_warning(self):
        """Test that unknown operations produce warnings."""
        mock_conn = MagicMock()
        mock_table = MagicMock()
        mock_conn.sql.return_value = mock_table

        builder = IbisBuilder(connection=mock_conn)
        steps = [
            TransformationStep(operation="sql", sql="SELECT 1"),
            TransformationStep(operation="unknown_op", sql="something"),
        ]
        result = builder.compile_chain(steps)

        assert len(result.warnings) > 0
        assert "unknown" in result.warnings[0].lower()


class TestIbisBuilderSelectOperation:
    """Tests for IbisBuilder._apply_select()."""

    def test_select_single_column(self):
        """Test selecting a single column."""
        builder = IbisBuilder()
        mock_table = MagicMock()
        mock_col = MagicMock()
        mock_table.__getitem__ = MagicMock(return_value=mock_col)
        mock_result = MagicMock()
        mock_table.select.return_value = mock_result

        result = builder._apply_select(mock_table, "id", {})

        mock_table.__getitem__.assert_called_with("id")
        mock_table.select.assert_called_once()

    def test_select_star(self):
        """Test selecting all columns."""
        builder = IbisBuilder()
        mock_table = MagicMock()

        result = builder._apply_select(mock_table, "*", {})

        assert result is mock_table

    def test_select_multiple_columns(self):
        """Test selecting multiple columns."""
        builder = IbisBuilder()
        mock_table = MagicMock()
        mock_table.__getitem__ = MagicMock(return_value=MagicMock())
        mock_table.select.return_value = MagicMock()

        result = builder._apply_select(mock_table, "id, name, amount", {})

        assert mock_table.__getitem__.call_count == 3
        mock_table.select.assert_called_once()


class TestIbisBuilderJoinOperation:
    """Tests for IbisBuilder._apply_join()."""

    def setup_method(self):
        """Reset singletons before each test."""
        ModelRegistry.reset_instance()
        ConfigParser._instances.clear()

    def test_join_with_ref(self):
        """Test join operation with model reference."""
        registry = ModelRegistry()
        config = MagicMock(spec=ConfigParser)
        registry.register("p", "customers", config)

        mock_conn = MagicMock()
        mock_right_table = MagicMock()
        mock_conn.table.return_value = mock_right_table

        mock_left_table = MagicMock()
        mock_left_col = MagicMock()
        mock_right_col = MagicMock()
        mock_left_table.__getitem__ = MagicMock(return_value=mock_left_col)
        mock_right_table.__getitem__ = MagicMock(return_value=mock_right_col)
        mock_join_result = MagicMock()
        mock_left_table.join.return_value = mock_join_result

        builder = IbisBuilder(registry=registry, connection=mock_conn)
        result = builder._apply_join(
            mock_left_table,
            "ref('p', 'customers')",
            {"on": "customer_id", "how": "inner"},
        )

        mock_left_table.join.assert_called_once()

    def test_join_without_left_returns_right(self):
        """Test join without left table returns right table."""
        registry = ModelRegistry()
        config = MagicMock(spec=ConfigParser)
        registry.register("p", "customers", config)

        mock_conn = MagicMock()
        mock_table = MagicMock()
        mock_conn.table.return_value = mock_table

        builder = IbisBuilder(registry=registry, connection=mock_conn)
        result = builder._apply_join(
            None,
            "ref('p', 'customers')",
            {},
        )

        assert result is mock_table

    def test_join_invalid_ref_raises_error(self):
        """Test that invalid ref raises error."""
        builder = IbisBuilder()

        with pytest.raises(IbisBuildError) as exc_info:
            builder._apply_join(MagicMock(), "invalid_ref", {})

        assert "Invalid join reference" in exc_info.value.message


class TestIbisBuilderAggregateOperation:
    """Tests for IbisBuilder._apply_aggregate()."""

    def test_aggregate_requires_metrics(self):
        """Test that aggregate requires at least one metric."""
        builder = IbisBuilder()
        mock_table = MagicMock()

        with pytest.raises(IbisBuildError) as exc_info:
            builder._apply_aggregate(mock_table, {})

        assert "metric" in exc_info.value.message.lower()

    def test_aggregate_with_group_by(self):
        """Test aggregate with group_by."""
        builder = IbisBuilder()
        mock_table = MagicMock()
        mock_grouped = MagicMock()
        mock_result = MagicMock()
        mock_table.group_by.return_value = mock_grouped
        mock_grouped.aggregate.return_value = mock_result

        mock_col = MagicMock()
        mock_col.sum.return_value = MagicMock()
        mock_table.__getitem__ = MagicMock(return_value=mock_col)

        result = builder._apply_aggregate(
            mock_table,
            {
                "group_by": ["category"],
                "metrics": {"total": {"column": "amount", "func": "sum"}},
            },
        )

        mock_table.group_by.assert_called_once_with(["category"])


class TestCreateIbisBuilder:
    """Tests for create_ibis_builder convenience function."""

    def setup_method(self):
        """Reset singletons before each test."""
        ModelRegistry.reset_instance()
        ConfigParser._instances.clear()

    def test_creates_builder(self):
        """Test that function creates IbisBuilder."""
        builder = create_ibis_builder()

        assert isinstance(builder, IbisBuilder)
        assert builder.registry is None
        assert builder.connection is None

    def test_creates_builder_with_args(self):
        """Test that function creates IbisBuilder with arguments."""
        registry = ModelRegistry()
        mock_conn = MagicMock()

        builder = create_ibis_builder(registry=registry, connection=mock_conn)

        assert builder.registry is registry
        assert builder.connection is mock_conn


class TestIbisBuilderFilterOperation:
    """Tests for IbisBuilder._apply_filter()."""

    def test_filter_raises_on_failure(self):
        """Test that filter raises error on failure."""
        builder = IbisBuilder()
        mock_table = MagicMock()
        mock_table.filter.side_effect = Exception("Filter failed")

        with pytest.raises(IbisBuildError) as exc_info:
            builder._apply_filter(mock_table, "invalid condition")

        assert "filter" in exc_info.value.message.lower()
