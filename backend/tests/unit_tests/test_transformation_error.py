"""Unit tests for TransformationError exception class."""

import pytest

from visitran.errors import TransformationError


class TestTransformationError:
    """Tests for TransformationError exception with YAML source tracking."""

    def test_basic_error_creation(self):
        """Test that TransformationError captures all required attributes."""
        error = TransformationError(
            model_name="test_model",
            transformation_id="transform_001",
            line_number=10,
            column_number=5,
            error_message="Invalid column reference",
            yaml_snippet="9 | select:\n10 |   - invalid_col\n     ^",
        )

        assert error.model_name == "test_model"
        assert error.transformation_id == "transform_001"
        assert error.line_number == 10
        assert error.column_number == 5
        assert error.error_message == "Invalid column reference"
        assert error.yaml_snippet == "9 | select:\n10 |   - invalid_col\n     ^"
        assert error.original_exception is None
        assert error.suggested_fix is None

    def test_error_message_format(self):
        """Test that error message format matches specification."""
        error = TransformationError(
            model_name="orders_model",
            transformation_id="filter_transform",
            line_number=25,
            column_number=12,
            error_message="Column 'price' not found",
            yaml_snippet="24 | filters:\n25 |   - column: price\n              ^",
        )

        error_str = str(error)

        # Check the main error line format
        assert "Error in orders_model at line 25:12: Column 'price' not found" in error_str
        # Check that the snippet is included
        assert "24 | filters:" in error_str
        assert "25 |   - column: price" in error_str

    def test_snippet_extraction_middle_of_file(self):
        """Test snippet extraction for errors in the middle of a file."""
        yaml_content = """name: test_model
source:
  schema: public
  table: orders
transform:
  - select:
      columns:
        - order_id
        - invalid_column
        - amount
model:
  table: output"""

        snippet = TransformationError.extract_yaml_snippet(yaml_content, 9, 9)

        # Should include lines 8, 9, 10 (1-based)
        assert "8 |" in snippet
        assert "9 |" in snippet
        assert "10 |" in snippet
        assert "invalid_column" in snippet
        # Should have column marker
        assert "^" in snippet

    def test_snippet_extraction_first_line(self):
        """Test snippet extraction when error is on the first line."""
        yaml_content = """invalid_syntax: [
second_line: value
third_line: value"""

        snippet = TransformationError.extract_yaml_snippet(yaml_content, 1, 1)

        # Should include lines 1, 2 (no line before first)
        assert "1 |" in snippet
        assert "2 |" in snippet
        assert "invalid_syntax" in snippet
        assert "^" in snippet

    def test_snippet_extraction_last_line(self):
        """Test snippet extraction when error is on the last line."""
        yaml_content = """first_line: value
second_line: value
last_line_error"""

        snippet = TransformationError.extract_yaml_snippet(yaml_content, 3, 5)

        # Should include lines 2, 3 (no line after last)
        assert "2 |" in snippet
        assert "3 |" in snippet
        assert "last_line_error" in snippet
        assert "^" in snippet

    def test_snippet_extraction_single_line_file(self):
        """Test snippet extraction for single-line files."""
        yaml_content = "single_line: with_error"

        snippet = TransformationError.extract_yaml_snippet(yaml_content, 1, 14)

        assert "1 |" in snippet
        assert "single_line" in snippet
        assert "^" in snippet

    def test_snippet_extraction_empty_content(self):
        """Test snippet extraction with empty content."""
        snippet = TransformationError.extract_yaml_snippet("", 1, 1)
        assert snippet == ""

    def test_exception_chaining(self):
        """Test that exception chaining preserves original exception cause."""
        original = ValueError("Original validation error")

        error = TransformationError(
            model_name="test_model",
            transformation_id="transform_001",
            line_number=10,
            column_number=5,
            error_message="Wrapped error",
            yaml_snippet="",
            original_exception=original,
        )

        assert error.__cause__ is original
        assert error.original_exception is original
        assert isinstance(error.__cause__, ValueError)
        assert str(error.__cause__) == "Original validation error"

    def test_suggested_fix_appending(self):
        """Test that suggested_fix is appended to formatted message."""
        error = TransformationError(
            model_name="test_model",
            transformation_id="transform_001",
            line_number=10,
            column_number=5,
            error_message="Column not found",
            yaml_snippet="10 | invalid_col\n     ^",
            suggested_fix="Check if column name is spelled correctly",
        )

        error_str = str(error)

        assert "Suggested fix: Check if column name is spelled correctly" in error_str
        assert error.suggested_fix == "Check if column name is spelled correctly"

    def test_column_marker_alignment(self):
        """Test that column marker is aligned correctly for different positions."""
        yaml_content = """transform:
  - select:
      columns:
        - order_id"""

        # Test column position at different offsets
        snippet_col_5 = TransformationError.extract_yaml_snippet(yaml_content, 2, 5)
        snippet_col_10 = TransformationError.extract_yaml_snippet(yaml_content, 2, 10)

        # Both should have markers at different positions
        assert "^" in snippet_col_5
        assert "^" in snippet_col_10

        # The marker in snippet_col_10 should be further right
        lines_5 = snippet_col_5.split("\n")
        lines_10 = snippet_col_10.split("\n")

        marker_line_5 = [l for l in lines_5 if "^" in l][0]
        marker_line_10 = [l for l in lines_10 if "^" in l][0]

        assert marker_line_10.index("^") > marker_line_5.index("^")

    def test_error_response_dictionary(self):
        """Test the error_response method returns correct structure."""
        error = TransformationError(
            model_name="test_model",
            transformation_id="transform_001",
            line_number=10,
            column_number=5,
            error_message="Test error",
            yaml_snippet="snippet",
            original_exception=ValueError("original"),
            suggested_fix="fix suggestion",
        )

        response = error.error_response()

        assert response["status"] == "failed"
        assert response["error_type"] == "TransformationError"
        assert response["model_name"] == "test_model"
        assert response["transformation_id"] == "transform_001"
        assert response["location"]["line"] == 10
        assert response["location"]["column"] == 5
        assert response["error_message"] == "Test error"
        assert response["yaml_snippet"] == "snippet"
        assert response["severity"] == "Error"
        assert response["suggested_fix"] == "fix suggestion"
        assert response["original_error"] == "original"

    def test_error_response_without_optional_fields(self):
        """Test error_response when optional fields are not provided."""
        error = TransformationError(
            model_name="test_model",
            transformation_id="transform_001",
            line_number=10,
            column_number=5,
            error_message="Test error",
            yaml_snippet="snippet",
        )

        response = error.error_response()

        assert "suggested_fix" not in response
        assert "original_error" not in response

    def test_severity_property(self):
        """Test that severity property returns 'Error'."""
        error = TransformationError(
            model_name="test_model",
            transformation_id="transform_001",
            line_number=10,
            column_number=5,
            error_message="Test error",
            yaml_snippet="",
        )

        assert error.severity == "Error"

    def test_exception_can_be_raised_and_caught(self):
        """Test that the exception can be raised and caught properly."""
        with pytest.raises(TransformationError) as exc_info:
            raise TransformationError(
                model_name="test_model",
                transformation_id="transform_001",
                line_number=10,
                column_number=5,
                error_message="Test error",
                yaml_snippet="",
            )

        assert exc_info.value.model_name == "test_model"
        assert "Error in test_model at line 10:5" in str(exc_info.value)

    def test_nested_exception_chaining(self):
        """Test that nested exception chaining works correctly."""
        root_cause = IOError("File not found")
        intermediate = ValueError("Config parsing failed")
        intermediate.__cause__ = root_cause

        error = TransformationError(
            model_name="test_model",
            transformation_id="transform_001",
            line_number=10,
            column_number=5,
            error_message="Top level error",
            yaml_snippet="",
            original_exception=intermediate,
        )

        # Check the chain
        assert error.__cause__ is intermediate
        assert error.__cause__.__cause__ is root_cause
