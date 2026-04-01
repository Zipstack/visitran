from visitran.errors.base_exceptions import VisitranBaseExceptions
from visitran.errors.error_codes import ErrorCodeConstants


class InvalidSnapshotFields(VisitranBaseExceptions):
    """Raised if the configurations file passed for snapshot is not adequate."""

    def __init__(self, invalid_fields: list[str]) -> None:
        super().__init__(error_code=ErrorCodeConstants.INVALID_SNAPSHOT_FIELDS, invalid_fields=invalid_fields)


class InvalidSnapshotColumns(VisitranBaseExceptions):
    """Raised if the columns specified in the check cols attribute for snapshot
    is restricted or not exist in table DB."""

    def __init__(self, invalid_cols: list[str]) -> None:
        super().__init__(error_code=ErrorCodeConstants.INVALID_SNAPSHOT_COLUMNS, invalid_cols=invalid_cols)


class TableNotFound(VisitranBaseExceptions):
    """Raised if mentioned table in schema doesn't exist."""

    def __init__(self, table_name: str, schema_name: str, failure_reason: str = "") -> None:
        super().__init__(
            error_code=ErrorCodeConstants.TABLE_NOT_FOUND,
            table_name=table_name,
            schema_name=schema_name,
            failure_reason=failure_reason,
        )


class InvalidCSVHeaders(VisitranBaseExceptions):
    """Raised if the headers in the CSV file are not valid."""

    def __init__(self, csv_file_name: str, column_name: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.INVALID_CSV_HEADERS,
            csv_file_name=csv_file_name,
            column_name=column_name,
        )


class DatabasePermissionDeniedError(VisitranBaseExceptions):
    """Raised when permission denied while creating db schema."""

    def __init__(self, schema_name: str, error_message: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.SCHEMA_CREATION_PERMISSION_DENIED,
            schema_name=schema_name,
            error_message=error_message,
        )


class SchemaAlreadyExist(VisitranBaseExceptions):
    """Raised when db schema arleray exist"""

    def __init__(self, schema_name: str, error_message: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.SCHEMA_ALREADY_EXIST,
            schema_name=schema_name,
            error_message=error_message,
        )


class SchemaCreationFailed(VisitranBaseExceptions):

    def __init__(self, schema_name: str, error_message: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.SCHEMA_CREATION_FAILED,
            schema_name=schema_name,
            error_message=error_message,
        )


class InvalidConnectionUrlException(VisitranBaseExceptions):
    """Raised when the connection URL is invalid."""

    def __init__(self, error_message: str) -> None:
        super().__init__(error_code=ErrorCodeConstants.INVALID_CONNECTION_URL, error_message=error_message)


class ConnectionFieldMissingException(VisitranBaseExceptions):
    """Raised when the connection missing required fields."""

    def __init__(self, missing_fields: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.MISSING_REQUIRED_CONNECTION_FIELDS, missing_fields=missing_fields
        )
