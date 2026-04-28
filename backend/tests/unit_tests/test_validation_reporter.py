"""Unit tests for Validation Reporter."""

import json
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from backend.application.config_parser.sql_validator import (
    ValidationResult,
    ValidationResultStore,
)
from backend.application.config_parser.validation_storage_service import (
    ValidationStorageService,
)
from backend.application.config_parser.validation_reporter import (
    ModelValidationDetail,
    ValidationReport,
    ValidationReporter,
    YAMLErrorMapper,
    get_validation_reporter,
    get_error_mapper,
    reset_reporter,
    reset_error_mapper,
)


class TestModelValidationDetail:
    """Tests for ModelValidationDetail dataclass."""

    def test_creation(self):
        """Test creating a detail object."""
        detail = ModelValidationDetail(
            model_name="test_model",
            match_status=True,
            match_type="exact",
        )

        assert detail.model_name == "test_model"
        assert detail.match_status is True
        assert detail.match_type == "exact"

    def test_to_dict(self):
        """Test conversion to dictionary."""
        detail = ModelValidationDetail(
            model_name="test_model",
            match_status=False,
            match_type="no_match",
            diff="- old\n+ new",
            execution_time_legacy_ms=100.0,
            execution_time_direct_ms=50.0,
        )

        d = detail.to_dict()

        assert d["model_name"] == "test_model"
        assert d["match_status"] is False
        assert d["has_diff"] is True
        assert d["execution_time_legacy_ms"] == 100.0

    def test_to_dict_no_diff(self):
        """Test to_dict when there's no diff."""
        detail = ModelValidationDetail(
            model_name="test_model",
            match_status=True,
        )

        d = detail.to_dict()

        assert d["has_diff"] is False


class TestValidationReport:
    """Tests for ValidationReport dataclass."""

    def test_creation(self):
        """Test creating a report."""
        report = ValidationReport(
            execution_id="test-exec-123",
            total_models=10,
            equivalent_count=8,
            non_equivalent_count=2,
            equivalence_rate=0.8,
        )

        assert report.execution_id == "test-exec-123"
        assert report.total_models == 10
        assert report.equivalence_rate == 0.8

    def test_to_dict(self):
        """Test conversion to dictionary."""
        report = ValidationReport(
            execution_id="test-123",
            total_models=5,
            equivalent_count=4,
            non_equivalent_count=1,
            equivalence_rate=0.8,
            non_equivalent_models=["bad_model"],
        )

        d = report.to_dict()

        assert d["execution_id"] == "test-123"
        assert d["summary"]["total_models"] == 5
        assert d["summary"]["equivalence_rate"] == 0.8
        assert "bad_model" in d["non_equivalent_models"]

    def test_to_json(self):
        """Test JSON serialization."""
        report = ValidationReport(
            execution_id="test-json",
            total_models=3,
            equivalent_count=3,
            non_equivalent_count=0,
            equivalence_rate=1.0,
        )

        json_str = report.to_json()
        parsed = json.loads(json_str)

        assert parsed["execution_id"] == "test-json"
        assert parsed["summary"]["equivalence_rate"] == 1.0

    def test_to_markdown_success(self):
        """Test Markdown generation for successful validation."""
        report = ValidationReport(
            execution_id="test-md",
            total_models=5,
            equivalent_count=5,
            non_equivalent_count=0,
            equivalence_rate=1.0,
        )

        md = report.to_markdown()

        assert "# SQL Validation Report" in md
        assert "test-md" in md
        assert "100.0%" in md
        assert "SUCCESS" in md

    def test_to_markdown_with_failures(self):
        """Test Markdown generation with failures."""
        detail = ModelValidationDetail(
            model_name="failing_model",
            match_status=False,
            match_type="no_match",
            diff="- SELECT 1\n+ SELECT 2",
        )

        report = ValidationReport(
            execution_id="test-fail",
            total_models=3,
            equivalent_count=2,
            non_equivalent_count=1,
            equivalence_rate=0.667,
            model_details=[detail],
            non_equivalent_models=["failing_model"],
        )

        md = report.to_markdown()

        assert "failing_model" in md
        assert "Non-Equivalent Models" in md
        assert "Detailed Discrepancies" in md
        assert "```diff" in md

    def test_to_markdown_with_performance(self):
        """Test Markdown generation with performance data."""
        report = ValidationReport(
            execution_id="test-perf",
            total_models=10,
            equivalent_count=10,
            non_equivalent_count=0,
            equivalence_rate=1.0,
            performance_summary={
                "total_legacy_ms": 1000.0,
                "total_direct_ms": 500.0,
                "speedup_factor": 2.0,
            },
        )

        md = report.to_markdown()

        assert "Performance" in md
        assert "1000.00 ms" in md
        assert "500.00 ms" in md
        assert "2.00x" in md

    def test_to_console(self):
        """Test console output generation."""
        report = ValidationReport(
            execution_id="test-console-12345678",
            total_models=10,
            equivalent_count=8,
            non_equivalent_count=2,
            equivalence_rate=0.8,
            non_equivalent_models=["model_a", "model_b"],
        )

        console = report.to_console()

        assert "SQL VALIDATION REPORT" in console
        assert "FAIL" in console
        assert "80.0%" in console
        assert "model_a" in console
        assert "model_b" in console

    def test_to_console_success(self):
        """Test console output for successful validation."""
        report = ValidationReport(
            execution_id="test-pass",
            total_models=5,
            equivalent_count=5,
            non_equivalent_count=0,
            equivalence_rate=1.0,
        )

        console = report.to_console()

        assert "PASS" in console
        assert "✓" in console

    def test_to_console_many_failures(self):
        """Test console output truncates long failure lists."""
        non_equiv = [f"model_{i}" for i in range(15)]

        report = ValidationReport(
            execution_id="test-many",
            total_models=15,
            equivalent_count=0,
            non_equivalent_count=15,
            equivalence_rate=0.0,
            non_equivalent_models=non_equiv,
        )

        console = report.to_console()

        assert "... and 5 more" in console


class TestValidationReporter:
    """Tests for ValidationReporter."""

    def test_init_defaults(self):
        """Test default initialization."""
        reporter = ValidationReporter()

        assert reporter._storage_service is None

    def test_init_with_service(self):
        """Test initialization with storage service."""
        service = ValidationStorageService(persist_to_db=False)
        reporter = ValidationReporter(storage_service=service)

        assert reporter._storage_service is service

    def test_storage_service_property(self):
        """Test storage service property creates default."""
        reset_reporter()
        reporter = ValidationReporter()

        service = reporter.storage_service

        assert service is not None
        assert isinstance(service, ValidationStorageService)

    def test_generate_report_empty(self):
        """Test generating report with no results."""
        service = ValidationStorageService(persist_to_db=False)
        reporter = ValidationReporter(storage_service=service)

        report = reporter.generate_report("empty-exec")

        assert report.total_models == 0
        assert report.equivalence_rate == 1.0  # Default to 100% when no data

    def test_generate_report_with_results(self):
        """Test generating report with validation results."""
        service = ValidationStorageService(persist_to_db=False)

        # Add results
        for i in range(5):
            result = ValidationResult(
                execution_id=f"exec-{i}",
                model_name=f"model_{i}",
                legacy_sql="SELECT 1",
                direct_sql="SELECT 1" if i < 4 else "SELECT 2",
                match_status=i < 4,
                discrepancy_details="Exact match" if i < 4 else "Mismatch",
            )
            service.store_result(result)

        reporter = ValidationReporter(storage_service=service)
        report = reporter.generate_report("test-exec")

        assert report.total_models == 5
        assert report.equivalent_count == 4
        assert report.non_equivalent_count == 1
        assert report.equivalence_rate == 0.8
        assert "model_4" in report.non_equivalent_models

    def test_generate_report_include_details(self):
        """Test generating report with details."""
        service = ValidationStorageService(persist_to_db=False)

        result = ValidationResult(
            execution_id="exec-1",
            model_name="test_model",
            legacy_sql="SELECT 1",
            direct_sql="SELECT 1",
            match_status=True,
            discrepancy_details="Exact match",
        )
        service.store_result(result)

        reporter = ValidationReporter(storage_service=service)
        report = reporter.generate_report("test", include_details=True)

        assert len(report.model_details) == 1
        assert report.model_details[0].model_name == "test_model"

    def test_generate_report_exclude_details(self):
        """Test generating report without details."""
        service = ValidationStorageService(persist_to_db=False)

        result = ValidationResult(
            execution_id="exec-1",
            model_name="test_model",
            legacy_sql="SELECT 1",
            direct_sql="SELECT 1",
            match_status=True,
        )
        service.store_result(result)

        reporter = ValidationReporter(storage_service=service)
        report = reporter.generate_report("test", include_details=False)

        assert len(report.model_details) == 0

    def test_generate_report_from_results(self):
        """Test generating report from a list of results."""
        reporter = ValidationReporter()

        results = [
            ValidationResult(
                execution_id="1",
                model_name="model_a",
                legacy_sql="SELECT a",
                direct_sql="SELECT a",
                match_status=True,
                discrepancy_details="Exact match",
            ),
            ValidationResult(
                execution_id="2",
                model_name="model_b",
                legacy_sql="SELECT b",
                direct_sql="SELECT c",
                match_status=False,
                discrepancy_details="Diff",
            ),
        ]

        report = reporter.generate_report_from_results(
            results=results,
            execution_id="manual-test",
            performance_data={"speedup_factor": 1.5},
        )

        assert report.total_models == 2
        assert report.equivalent_count == 1
        assert report.non_equivalent_count == 1
        assert report.equivalence_rate == 0.5
        assert report.performance_summary["speedup_factor"] == 1.5

    def test_match_type_determination(self):
        """Test match type is correctly determined."""
        service = ValidationStorageService(persist_to_db=False)

        # Exact match
        service.store_result(
            ValidationResult(
                execution_id="1",
                model_name="exact_model",
                legacy_sql="SELECT 1",
                direct_sql="SELECT 1",
                match_status=True,
                discrepancy_details="Exact match",
            )
        )

        # Normalized match
        service.store_result(
            ValidationResult(
                execution_id="2",
                model_name="normalized_model",
                legacy_sql="SELECT 1",
                direct_sql="select 1",
                match_status=True,
                discrepancy_details="Normalized match",
            )
        )

        reporter = ValidationReporter(storage_service=service)
        report = reporter.generate_report("test", include_details=True)

        # Find exact match
        exact = next(d for d in report.model_details if d.model_name == "exact_model")
        assert exact.match_type == "exact"

        # Find normalized match
        normalized = next(
            d for d in report.model_details if d.model_name == "normalized_model"
        )
        assert normalized.match_type == "normalized"


class TestYAMLErrorMapper:
    """Tests for YAMLErrorMapper."""

    def test_register_and_get_source(self):
        """Test registering and retrieving source locations."""
        mapper = YAMLErrorMapper()

        mapper.register_source(
            model_name="my_model",
            element_id="column_a",
            yaml_file="/path/to/model.yaml",
            line_number=42,
        )

        location = mapper.get_source_location("my_model", "column_a")

        assert location == ("/path/to/model.yaml", 42)

    def test_get_source_not_found(self):
        """Test getting source for unregistered element."""
        mapper = YAMLErrorMapper()

        location = mapper.get_source_location("unknown", "unknown")

        assert location is None

    def test_format_error_location_with_source(self):
        """Test formatting error with known source."""
        mapper = YAMLErrorMapper()

        mapper.register_source(
            model_name="model",
            element_id="col",
            yaml_file="model.yaml",
            line_number=10,
        )

        formatted = mapper.format_error_location(
            "model", "col", "Invalid type"
        )

        assert formatted == "model.yaml:10: Invalid type"

    def test_format_error_location_without_source(self):
        """Test formatting error without known source."""
        mapper = YAMLErrorMapper()

        formatted = mapper.format_error_location(
            "model", "col", "Invalid type"
        )

        assert formatted == "model/col: Invalid type"

    def test_map_traceback(self):
        """Test mapping a traceback to YAML source."""
        mapper = YAMLErrorMapper()

        mapper.register_source(
            model_name="my_model",
            element_id="transform_1",
            yaml_file="models/my_model.yaml",
            line_number=25,
        )

        original_tb = "Traceback (most recent call last):\n  ..."
        mapped = mapper.map_traceback(original_tb, "my_model")

        assert "Error in model 'my_model'" in mapped
        assert "Source: models/my_model.yaml" in mapped
        assert "Traceback" in mapped

    def test_map_traceback_no_source(self):
        """Test mapping traceback when no source is registered."""
        mapper = YAMLErrorMapper()

        original_tb = "Traceback..."
        mapped = mapper.map_traceback(original_tb, "unknown_model")

        assert "Error in model 'unknown_model'" in mapped
        assert "Traceback" in mapped

    def test_clear(self):
        """Test clearing all mappings."""
        mapper = YAMLErrorMapper()

        mapper.register_source("m", "e", "f.yaml", 1)
        mapper.clear()

        location = mapper.get_source_location("m", "e")
        assert location is None

    def test_multiple_elements_same_model(self):
        """Test multiple elements for the same model."""
        mapper = YAMLErrorMapper()

        mapper.register_source("model", "col_a", "model.yaml", 10)
        mapper.register_source("model", "col_b", "model.yaml", 20)
        mapper.register_source("model", "transform", "model.yaml", 30)

        assert mapper.get_source_location("model", "col_a") == ("model.yaml", 10)
        assert mapper.get_source_location("model", "col_b") == ("model.yaml", 20)
        assert mapper.get_source_location("model", "transform") == ("model.yaml", 30)


class TestGlobalFunctions:
    """Tests for global convenience functions."""

    def test_get_validation_reporter(self):
        """Test getting global reporter."""
        reset_reporter()

        reporter = get_validation_reporter()

        assert reporter is not None
        assert isinstance(reporter, ValidationReporter)

    def test_get_validation_reporter_singleton(self):
        """Test reporter is singleton."""
        reset_reporter()

        reporter1 = get_validation_reporter()
        reporter2 = get_validation_reporter()

        assert reporter1 is reporter2

    def test_get_error_mapper(self):
        """Test getting global error mapper."""
        reset_error_mapper()

        mapper = get_error_mapper()

        assert mapper is not None
        assert isinstance(mapper, YAMLErrorMapper)

    def test_get_error_mapper_singleton(self):
        """Test error mapper is singleton."""
        reset_error_mapper()

        mapper1 = get_error_mapper()
        mapper2 = get_error_mapper()

        assert mapper1 is mapper2

    def test_reset_reporter(self):
        """Test resetting reporter."""
        reset_reporter()

        reporter1 = get_validation_reporter()
        reset_reporter()
        reporter2 = get_validation_reporter()

        assert reporter1 is not reporter2

    def test_reset_error_mapper(self):
        """Test resetting error mapper."""
        reset_error_mapper()

        mapper1 = get_error_mapper()
        reset_error_mapper()
        mapper2 = get_error_mapper()

        assert mapper1 is not mapper2


class TestReportIntegration:
    """Integration tests for report generation."""

    def test_full_workflow(self):
        """Test a complete validation and reporting workflow."""
        # Setup
        service = ValidationStorageService(persist_to_db=False)
        reporter = ValidationReporter(storage_service=service)

        # Simulate validation results
        results = [
            ("schema.model_orders", "SELECT * FROM orders", "SELECT * FROM orders", True),
            ("schema.model_users", "SELECT * FROM users", "SELECT * FROM users", True),
            ("schema.model_products", "SELECT id FROM products", "SELECT product_id FROM products", False),
        ]

        for name, legacy, direct, match in results:
            result = ValidationResult(
                execution_id="workflow-test",
                model_name=name,
                legacy_sql=legacy,
                direct_sql=direct,
                match_status=match,
                discrepancy_details="Exact match" if match else "Column mismatch",
            )
            service.store_result(result)

        # Generate report
        report = reporter.generate_report("workflow-test")

        # Verify
        assert report.total_models == 3
        assert report.equivalent_count == 2
        assert report.non_equivalent_count == 1
        assert report.equivalence_rate == pytest.approx(0.667, rel=0.01)
        assert "schema.model_products" in report.non_equivalent_models

        # Test all output formats
        md = report.to_markdown()
        assert "model_products" in md

        console = report.to_console()
        assert "FAIL" in console

        json_out = report.to_json()
        parsed = json.loads(json_out)
        assert parsed["summary"]["non_equivalent_count"] == 1

    def test_error_mapping_integration(self):
        """Test error mapping with validation reporting."""
        mapper = YAMLErrorMapper()

        # Register sources for a model
        mapper.register_source(
            "sales_model",
            "revenue_column",
            "models/sales.yaml",
            15,
        )
        mapper.register_source(
            "sales_model",
            "filter_transform",
            "models/sales.yaml",
            25,
        )

        # Simulate an error
        error_msg = mapper.format_error_location(
            "sales_model",
            "revenue_column",
            "Invalid aggregation function",
        )

        assert error_msg == "models/sales.yaml:15: Invalid aggregation function"
