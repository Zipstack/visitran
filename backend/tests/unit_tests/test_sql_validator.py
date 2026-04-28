"""Unit tests for SQL Validator."""

import pytest

from backend.application.config_parser.sql_validator import (
    SQLNormalizationOptions,
    SQLComparisonResult,
    ValidationResult,
    SQLNormalizer,
    SQLValidator,
    ValidationResultStore,
    get_validator,
    get_validation_store,
    validate_sql_equivalence,
)


class TestSQLNormalizationOptions:
    """Tests for SQLNormalizationOptions."""

    def test_default_options(self):
        """Test default normalization options."""
        options = SQLNormalizationOptions()

        assert options.strip_whitespace is True
        assert options.strip_comments is True
        assert options.lowercase_keywords is True
        assert options.normalize_aliases is False
        assert options.ignore_semicolons is True


class TestSQLNormalizer:
    """Tests for SQLNormalizer."""

    def test_normalize_whitespace(self):
        """Test whitespace normalization."""
        normalizer = SQLNormalizer()
        sql = "SELECT   col1,\n  col2   FROM   table1"
        result = normalizer.normalize(sql)

        assert "  " not in result
        assert "\n" not in result

    def test_strip_single_line_comments(self):
        """Test stripping single-line comments."""
        normalizer = SQLNormalizer()
        sql = "SELECT col1 -- this is a comment\nFROM table1"
        result = normalizer.normalize(sql)

        assert "--" not in result
        assert "comment" not in result

    def test_strip_multiline_comments(self):
        """Test stripping multi-line comments."""
        normalizer = SQLNormalizer()
        sql = "SELECT col1 /* multi\nline\ncomment */ FROM table1"
        result = normalizer.normalize(sql)

        assert "/*" not in result
        assert "comment" not in result

    def test_lowercase_keywords(self):
        """Test keyword lowercasing."""
        normalizer = SQLNormalizer()
        sql = "SELECT Col1 FROM Table1 WHERE Col2 = 'VALUE'"
        result = normalizer.normalize(sql)

        assert "select" in result
        assert "from" in result
        assert "where" in result
        # Non-keywords should preserve case (though whitespace normalized)
        assert "Col1" in result or "col1" in result.lower()

    def test_ignore_semicolons(self):
        """Test semicolon removal."""
        normalizer = SQLNormalizer()
        sql = "SELECT * FROM table1;"
        result = normalizer.normalize(sql)

        assert not result.endswith(";")

    def test_empty_string(self):
        """Test normalizing empty string."""
        normalizer = SQLNormalizer()
        result = normalizer.normalize("")

        assert result == ""

    def test_compute_hash(self):
        """Test hash computation."""
        normalizer = SQLNormalizer()

        # Same SQL with different formatting should have same hash
        sql1 = "SELECT  col1  FROM  table1"
        sql2 = "select col1 from table1"

        hash1 = normalizer.compute_hash(sql1)
        hash2 = normalizer.compute_hash(sql2)

        assert hash1 == hash2

    def test_different_sql_different_hash(self):
        """Test different SQL produces different hash."""
        normalizer = SQLNormalizer()

        hash1 = normalizer.compute_hash("SELECT col1 FROM table1")
        hash2 = normalizer.compute_hash("SELECT col2 FROM table1")

        assert hash1 != hash2


class TestSQLValidator:
    """Tests for SQLValidator."""

    def test_exact_match(self):
        """Test exact SQL match."""
        validator = SQLValidator()
        sql = "SELECT * FROM table1"

        result = validator.compare(sql, sql)

        assert result.match_status is True
        assert "Exact match" in result.discrepancy_details

    def test_normalized_match(self):
        """Test normalized SQL match."""
        validator = SQLValidator()
        sql1 = "SELECT   col1   FROM   table1"
        sql2 = "select col1 from table1"

        result = validator.compare(sql1, sql2)

        assert result.match_status is True
        assert "Normalized match" in result.discrepancy_details

    def test_no_match(self):
        """Test SQL that doesn't match."""
        validator = SQLValidator()
        sql1 = "SELECT col1 FROM table1"
        sql2 = "SELECT col2 FROM table2"

        result = validator.compare(sql1, sql2, model_name="test_model")

        assert result.match_status is False
        assert result.model_name == "test_model"
        assert len(result.discrepancy_details) > 0

    def test_strict_mode(self):
        """Test strict mode requires exact match."""
        validator = SQLValidator(strict_mode=True)
        sql1 = "SELECT col1 FROM table1"
        sql2 = "select col1 from table1"

        result = validator.compare(sql1, sql2)

        assert result.match_status is False
        assert "Strict mode" in result.discrepancy_details

    def test_generates_diff(self):
        """Test that diff is generated for mismatches."""
        validator = SQLValidator()
        sql1 = "SELECT col1 FROM table1"
        sql2 = "SELECT col2 FROM table1"

        result = validator.compare(sql1, sql2)

        assert "col1" in result.discrepancy_details or "-" in result.discrepancy_details

    def test_result_has_execution_id(self):
        """Test that result has unique execution ID."""
        validator = SQLValidator()

        result1 = validator.compare("SELECT 1", "SELECT 1")
        result2 = validator.compare("SELECT 2", "SELECT 2")

        assert result1.execution_id != result2.execution_id


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_to_dict(self):
        """Test serialization to dictionary."""
        result = ValidationResult(
            execution_id="test-id",
            model_name="my_model",
            legacy_sql="SELECT 1",
            direct_sql="SELECT 1",
            match_status=True,
        )

        d = result.to_dict()

        assert d["execution_id"] == "test-id"
        assert d["model_name"] == "my_model"
        assert d["match_status"] is True
        assert "execution_timestamp" in d


class TestValidationResultStore:
    """Tests for ValidationResultStore."""

    def test_store_and_retrieve(self):
        """Test storing and retrieving results."""
        store = ValidationResultStore()
        result = ValidationResult(
            execution_id="id1",
            model_name="model1",
            legacy_sql="SELECT 1",
            direct_sql="SELECT 1",
            match_status=True,
        )

        store.store(result)
        all_results = store.get_all()

        assert len(all_results) == 1
        assert all_results[0].execution_id == "id1"

    def test_get_by_model(self):
        """Test filtering by model name."""
        store = ValidationResultStore()
        store.store(ValidationResult(
            execution_id="id1",
            model_name="model1",
            legacy_sql="",
            direct_sql="",
            match_status=True,
        ))
        store.store(ValidationResult(
            execution_id="id2",
            model_name="model2",
            legacy_sql="",
            direct_sql="",
            match_status=True,
        ))

        results = store.get_by_model("model1")

        assert len(results) == 1
        assert results[0].model_name == "model1"

    def test_get_discrepancies(self):
        """Test getting only discrepancies."""
        store = ValidationResultStore()
        store.store(ValidationResult(
            execution_id="id1",
            model_name="model1",
            legacy_sql="",
            direct_sql="",
            match_status=True,
        ))
        store.store(ValidationResult(
            execution_id="id2",
            model_name="model2",
            legacy_sql="",
            direct_sql="",
            match_status=False,
        ))

        discrepancies = store.get_discrepancies()

        assert len(discrepancies) == 1
        assert discrepancies[0].match_status is False

    def test_get_summary(self):
        """Test summary statistics."""
        store = ValidationResultStore()
        store.store(ValidationResult(
            execution_id="id1",
            model_name="m1",
            legacy_sql="",
            direct_sql="",
            match_status=True,
        ))
        store.store(ValidationResult(
            execution_id="id2",
            model_name="m2",
            legacy_sql="",
            direct_sql="",
            match_status=True,
        ))
        store.store(ValidationResult(
            execution_id="id3",
            model_name="m3",
            legacy_sql="",
            direct_sql="",
            match_status=False,
        ))

        summary = store.get_summary()

        assert summary["total_executions"] == 3
        assert summary["matches"] == 2
        assert summary["discrepancies"] == 1
        assert summary["match_rate"] == pytest.approx(2/3)

    def test_clear(self):
        """Test clearing the store."""
        store = ValidationResultStore()
        store.store(ValidationResult(
            execution_id="id1",
            model_name="m1",
            legacy_sql="",
            direct_sql="",
            match_status=True,
        ))

        store.clear()

        assert len(store.get_all()) == 0


class TestConvenienceFunctions:
    """Tests for module-level convenience functions."""

    def test_get_validator_singleton(self):
        """Test get_validator returns singleton."""
        v1 = get_validator()
        v2 = get_validator()

        assert v1 is v2

    def test_get_validation_store_singleton(self):
        """Test get_validation_store returns singleton."""
        s1 = get_validation_store()
        s2 = get_validation_store()

        assert s1 is s2

    def test_validate_sql_equivalence(self):
        """Test validate_sql_equivalence convenience function."""
        # Clear the store first
        get_validation_store().clear()

        result = validate_sql_equivalence(
            legacy_sql="SELECT * FROM t",
            direct_sql="select * from t",
            model_name="test",
            store_result=True,
            log_discrepancy=False,
        )

        assert result.match_status is True
        assert len(get_validation_store().get_all()) == 1

    def test_validate_sql_equivalence_no_store(self):
        """Test validate without storing."""
        get_validation_store().clear()

        result = validate_sql_equivalence(
            legacy_sql="SELECT 1",
            direct_sql="SELECT 1",
            store_result=False,
        )

        assert result.match_status is True
        assert len(get_validation_store().get_all()) == 0
