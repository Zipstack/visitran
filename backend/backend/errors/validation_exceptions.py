from rest_framework import status

from backend.errors.error_codes import BackendErrorMessages
from backend.errors.visitran_backend_base_exceptions import VisitranBackendBaseException


class SourceTableDoesNotExist(VisitranBackendBaseException):
    """
    Raised if the model is configured with invalid source table.
    """

    def __init__(self, schema_name: str, table_name: str, model_name: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.SOURCE_TABLE_NOT_EXIST,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            schema_name=schema_name,
            table_name=table_name,
            model_name=model_name,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class DestinationTableAlreadyExist(VisitranBackendBaseException):
    """
    Raised if the model is configured with the same destination table name.
    """

    def __init__(self, schema_name: str, table_name: str, current_model_name: str, conflicting_model_name: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.DESTINATION_TABLE_ALREADY_EXIST,
            http_status_code=status.HTTP_409_CONFLICT,
            schema_name=schema_name,
            table_name=table_name,
            current_model_name=current_model_name,
            conflicting_model_name=conflicting_model_name
        )

    @property
    def severity(self) -> str:
        return "Warning"


class JoinTableDoesNotExist(VisitranBackendBaseException):
    """
    Raised if the model is configured with the same destination table name.
    """

    def __init__(self, table_name: str, model_name: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.JOIN_TABLE_NOT_EXIST,
            http_status_code=status.HTTP_404_NOT_FOUND,
            table_name=table_name,
            model_name=model_name,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class MergeTableDoesNotExist(VisitranBackendBaseException):
    """
    Raised if the model is configured with the same destination table name.
    """

    def __init__(self, table_name: str, model_name: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.MERGE_TABLE_NOT_EXIST,
            http_status_code=status.HTTP_404_NOT_FOUND,
            table_name=table_name,
            model_name=model_name,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class CircularDependencyReference(VisitranBackendBaseException):
    """
    Raised if the model is configured with the same destination table name.
    """

    def __init__(self, model_name: str, traversed_path: list[str]) -> None:
        super().__init__(
            error_code=BackendErrorMessages.CIRCULAR_DEPENDENCY_IN_REFERENCES,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            model_name=model_name,
            traversed_path=traversed_path,
        )


class InvalidSQLQuery(VisitranBackendBaseException):
    """
    Raised if the given sql query is invalid.
    """

    def __init__(self, sql_query: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.INVALID_SQL_QUERY,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            sql_query=sql_query,
        )

    @property
    def severity(self) -> str:
        return "Warning"

class SQLExtractionError(VisitranBackendBaseException):
    """
    Raised when no SQL query can be extracted from the given text.
    """

    def __init__(self, text: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.SQL_EXTRACTION_FAILED,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            provided_text=text,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class ProhibitedSqlQuery(VisitranBackendBaseException):
    """
    Raised if the given sql query is prohibited, if it contains any prohibited keywords.
    """

    def __init__(self, prohibited_action: str, prohibited_actions: list[str]) -> None:
        super().__init__(
            error_code=BackendErrorMessages.PROHIBITED_QUERY,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            prohibited_action=prohibited_action,
            prohibited_actions=prohibited_actions,
        )


class EnvironmentInUse(VisitranBackendBaseException):
    """
    Raised when trying to delete an environment that is referenced by scheduled jobs.
    """

    def __init__(self, environment_name: str, job_names: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.ENVIRONMENT_IN_USE,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            environment_name=environment_name,
            job_names=job_names,
        )

    @property
    def severity(self) -> str:
        return "Warning"
