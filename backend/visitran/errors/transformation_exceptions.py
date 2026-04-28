from typing import Optional

from visitran.errors.base_exceptions import VisitranBaseExceptions
from visitran.errors.error_codes import ErrorCodeConstants


class TransformationError(Exception):
    """
    Custom exception for transformation errors with precise YAML source location tracking.

    This exception captures detailed error information including the exact position
    in the YAML file where the error occurred, along with a contextual snippet.

    Attributes:
        model_name: Name of the model where the error occurred
        transformation_id: Identifier of the specific transformation
        line_number: Line number in the YAML file (1-based)
        column_number: Column number in the YAML file (1-based)
        error_message: Description of the error
        yaml_snippet: 3-line context showing the error location
        original_exception: Optional chained exception that caused this error
        suggested_fix: Optional recommendation for fixing the error
    """

    def __init__(
        self,
        model_name: str,
        transformation_id: str,
        line_number: int,
        column_number: int,
        error_message: str,
        yaml_snippet: str,
        original_exception: Optional[Exception] = None,
        suggested_fix: Optional[str] = None,
    ) -> None:
        self.model_name = model_name
        self.transformation_id = transformation_id
        self.line_number = line_number
        self.column_number = column_number
        self.error_message = error_message
        self.yaml_snippet = yaml_snippet
        self.original_exception = original_exception
        self.suggested_fix = suggested_fix

        # Build the formatted message
        formatted_message = self._format_message()

        # Initialize with exception chaining if original_exception is provided
        if original_exception is not None:
            super().__init__(formatted_message)
            self.__cause__ = original_exception
        else:
            super().__init__(formatted_message)

    def _format_message(self) -> str:
        """
        Format the error message with YAML source location and contextual snippet.

        Returns:
            Formatted error message with location info, snippet, and optional fix.
        """
        # Base error message format
        message_parts = [
            f"Error in {self.model_name} at line {self.line_number}:{self.column_number}: {self.error_message}"
        ]

        # Add the YAML snippet with column marker
        if self.yaml_snippet:
            message_parts.append("")
            message_parts.append(self.yaml_snippet)

        # Add suggested fix if provided
        if self.suggested_fix:
            message_parts.append("")
            message_parts.append(f"Suggested fix: {self.suggested_fix}")

        return "\n".join(message_parts)

    @staticmethod
    def extract_yaml_snippet(
        yaml_content: str,
        line_number: int,
        column_number: int,
    ) -> str:
        """
        Extract a 3-line snippet from YAML content with a column position marker.

        Extracts the line where the error occurred, one line before, and one line
        after (when available), and adds a visual marker pointing to the column.

        Args:
            yaml_content: The full YAML file content
            line_number: The 1-based line number of the error
            column_number: The 1-based column number of the error

        Returns:
            Formatted string with 3 lines of context and a column marker
        """
        lines = yaml_content.splitlines()
        total_lines = len(lines)

        if total_lines == 0:
            return ""

        # Convert to 0-based index
        error_line_idx = line_number - 1

        # Clamp to valid range
        if error_line_idx < 0:
            error_line_idx = 0
        elif error_line_idx >= total_lines:
            error_line_idx = total_lines - 1

        # Determine the range of lines to include
        start_idx = max(0, error_line_idx - 1)
        end_idx = min(total_lines, error_line_idx + 2)

        # Calculate line number width for alignment
        max_line_num = end_idx
        line_num_width = len(str(max_line_num))

        snippet_lines = []
        for idx in range(start_idx, end_idx):
            line_num = idx + 1  # 1-based line number
            line_content = lines[idx]
            prefix = f"{line_num:>{line_num_width}} | "

            if idx == error_line_idx:
                # This is the error line - add marker on next line
                snippet_lines.append(f"{prefix}{line_content}")
                # Create the column marker
                marker_padding = " " * (len(prefix) + column_number - 1)
                snippet_lines.append(f"{marker_padding}^")
            else:
                snippet_lines.append(f"{prefix}{line_content}")

        return "\n".join(snippet_lines)

    @property
    def severity(self) -> str:
        """Return the severity level of this exception."""
        return "Error"

    def error_response(self) -> dict:
        """
        Return a structured error response dictionary.

        Returns:
            Dictionary containing error details for API responses.
        """
        response = {
            "status": "failed",
            "error_type": "TransformationError",
            "model_name": self.model_name,
            "transformation_id": self.transformation_id,
            "location": {
                "line": self.line_number,
                "column": self.column_number,
            },
            "error_message": self.error_message,
            "yaml_snippet": self.yaml_snippet,
            "severity": self.severity,
        }

        if self.suggested_fix:
            response["suggested_fix"] = self.suggested_fix

        if self.original_exception:
            response["original_error"] = str(self.original_exception)

        return response


class ColumnNotExist(VisitranBaseExceptions):
    """
    Raised if the column is not found
    """

    def __init__(self, column_name: str, transformation_name: str, model_name: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.COLUMN_NOT_EXIST,
            column_name=column_name,
            model_name=model_name,
            transformation_name=transformation_name,
        )


class SynthesisColumnNotExist(VisitranBaseExceptions):
    """Raised if the column name specified in formula fields are not in the
    source table."""

    def __init__(self, column_name: str, model_name: str, transformation_name: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.SYNTHESIS_COLUMN_NOT_EXIST,
            column_name=column_name,
            model_name=model_name,
            transformation_name=transformation_name,
        )


class TransformationFailed(VisitranBaseExceptions):
    """Raised if the transformation fails."""

    def __init__(self, transformation_name: str, model_name: str, error_message: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.TRANSFORMATION_FAILED,
            transformation_name=transformation_name,
            model_name=model_name,
            error_message=error_message,
        )
