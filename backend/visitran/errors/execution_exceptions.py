from visitran.errors.base_exceptions import VisitranBaseExceptions
from visitran.errors.error_codes import ErrorCodeConstants


class ModelNotFound(VisitranBaseExceptions):
    """Raised when a python module is not found."""

    def __init__(self, module_name: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.MODEL_NOT_FOUND,
            module_name=module_name,
        )


class ModelImportError(VisitranBaseExceptions):
    """Raised when a python model is unable to import."""

    def __init__(self, model_name: str, error_message: str):
        super().__init__(
            error_code=ErrorCodeConstants.MODEL_IMPORT_ERROR, model_name=model_name, error_message=error_message
        )


class ModelExecutionFailed(VisitranBaseExceptions):
    """Raised when a node execution fails."""

    def __init__(self, schema_name: str, table_name: str, model_name: str, error_message: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.MODEL_EXECUTION_FAILED,
            schema_name=schema_name,
            table_name=table_name,
            model_name=model_name,
            error_message=error_message,
        )


class SqlQueryFailed(VisitranBaseExceptions):
    """Raised when a node transformation fails."""

    def __init__(self, query_statements: list[str], error_message: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.SQL_TRANSFORMATION_QUERY,
            query_statements=query_statements,
            error_message=error_message,
        )


class ConnectionFailedError(VisitranBaseExceptions):
    """Raised when unable to establish a connection from the profile connection
    configuration."""

    def __init__(self, db_type: str, error_message: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.CONNECTION_FAILED_ERROR,
            db_type=db_type,
            error_message=error_message,
        )


class SeedFailureException(VisitranBaseExceptions):
    """Raised if the CSV file is unable to process !!!"""

    def __init__(self, seed_file_name: str, error_message: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.SEED_FILE_EXECUTION_ERROR,
            file_name=seed_file_name,
            error_message=error_message,
        )


class RunSeedFailedException(VisitranBaseExceptions):
    def __init__(self, file_name: str, error_message: str):
        super().__init__(
            error_code=ErrorCodeConstants.RUN_SEED_FILE_FAILED_ERROR, file_name=file_name, error_message=error_message
        )
