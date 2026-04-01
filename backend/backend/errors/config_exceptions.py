from rest_framework import status

from backend.errors.error_codes import BackendErrorMessages
from backend.errors.visitran_backend_base_exceptions import VisitranBackendBaseException


class InvalidSourceTable(VisitranBackendBaseException):
    """
    Raised if the model is configured with invalid source table.
    """

    def __init__(self, table_name: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.INVALID_SOURCE_TABLE,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            table_name=table_name,
        )


class InvalidDestinationTable(VisitranBackendBaseException):
    """
    Raised if the model is configured with invalid source table.
    """

    def __init__(self, table_name: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.INVALID_DESTINATION_TABLE,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            table_name=table_name,
        )


class InvalidMaterialization(VisitranBackendBaseException):
    """
    Raised if the model is configured with invalid source table.
    """

    def __init__(
        self,
        materialization: str,
        supported_materializations: list[str],
    ) -> None:
        super().__init__(
            error_code=BackendErrorMessages.INVALID_MATERIALIZATION,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            materialization=materialization,
            supported_materializations=supported_materializations,
        )


class ReferenceNotFound(VisitranBackendBaseException):
    """
    Raise if the reference is not found.
    """

    def __init__(self, missing_references: list[str]):
        super().__init__(
            error_code=BackendErrorMessages.REFERENCE_MODEL_NOT_FOUND,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            missing_references=missing_references,
        )


class InvalidModelConfigError(VisitranBackendBaseException):
    """
        Raise if the model config is invalid.
    """

    def __init__(self, failure_reason: str):
        super().__init__(
            error_code=BackendErrorMessages.INVALID_MODEL_CONFIGURATION_DATA,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            failure_reason=failure_reason,
        )

class InvalidModelReferenceError(VisitranBackendBaseException):
    """
        Raise if the model config is invalid.
    """

    def __init__(self, failure_reason: str):
        super().__init__(
            error_code=BackendErrorMessages.INVALID_MODEL_REFERENCE_DATA,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            failure_reason=failure_reason,
        )