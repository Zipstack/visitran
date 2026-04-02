from datetime import datetime

from rest_framework import status

from backend.errors.error_codes import BackendErrorMessages
from backend.errors.visitran_backend_base_exceptions import VisitranBackendBaseException


class UnhandledErrorMessage(VisitranBackendBaseException):
    """Raised if any unhandled exceptions are captured."""

    def __init__(self, error_obj: Exception):
        super().__init__(
            error_code=BackendErrorMessages.DEFAULT_ERROR_MSG,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            error_message=error_obj.__str__(),
        )


class VisitranCoreExceptions(VisitranBackendBaseException):
    """
    This is a wrapper for all the exceptions raised from visitran
    """

    def __init__(self, error_message: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.CORE_EXCEPTIONS,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            error_message=error_message,
        )


class ProjectNotExist(VisitranBackendBaseException):
    """
    Raised if the project is not found.
    """

    def __init__(self, project_id: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.PROJECT_NOT_EXISTS,
            http_status_code=status.HTTP_404_NOT_FOUND,
            project_id=project_id,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class ProjectNameReservedError(VisitranBackendBaseException):
    """
    Raised when attempting to create a project with a reserved name.
    """

    def __init__(self, project_name: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.PROJECT_NAME_RESERVED,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            project_name=project_name,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class ProjectAlreadyExists(VisitranBackendBaseException):
    """
    Raised if the project already exists with the same name.
    """

    def __init__(self, project_name: str, created_at) -> None:
        super().__init__(
            error_code=BackendErrorMessages.PROJECT_ALREADY_EXISTS,
            http_status_code=status.HTTP_409_CONFLICT,
            project_name=project_name,
            created_at=created_at,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class ConnectionAlreadyExists(VisitranBackendBaseException):
    """
    Raised if the connection already exists with the same name.
    """

    def __init__(self, connection_name: str, created_at: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.CONNECTION_ALREADY_EXISTS,
            http_status_code=status.HTTP_409_CONFLICT,
            connection_name=connection_name,
            created_at=created_at,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class ConnectionNotExists(VisitranBackendBaseException):
    """
    Raised if the connection does not exist.
    """

    def __init__(self, connection_id: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.CONNECTION_NOT_EXISTS,
            http_status_code=status.HTTP_404_NOT_FOUND,
            connection_id=connection_id,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class ConnectionDependencyError(VisitranBackendBaseException):
    """
    Raised if the connection has any dependency with any projects
    """

    def __init__(self, connection_id: str, connection_name: str, affected_projects: list[str]) -> None:
        super().__init__(
            error_code=BackendErrorMessages.CONNECTION_DEPENDENCY,
            http_status_code=status.HTTP_409_CONFLICT,
            connection_id=connection_id,
            connection_name=connection_name,
            affected_projects=affected_projects,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class ModelAlreadyExists(VisitranBackendBaseException):
    """
    Raised if the model already exists with the same name.
    """

    def __init__(self, model_name: str, created_at: datetime) -> None:
        super().__init__(
            error_code=BackendErrorMessages.MODEL_ALREADY_EXISTS,
            http_status_code=status.HTTP_409_CONFLICT,
            model_name=model_name,
            created_at=created_at,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class ModelNotExists(VisitranBackendBaseException):
    """
    Exception raised when a specific model does not exist.

    This exception is used to indicate that the requested model cannot be found
    within the current context or configuration. It may be helpful to handle
    this exception in cases where dynamic or user-defined models are being
    accessed.

    :type model_name: str
    """

    def __init__(self, model_name: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.MODEL_NOT_EXISTS,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            model_name=model_name,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class CSVRenameFailed(VisitranBackendBaseException):

    def __init__(self, csv_name: str, reason: str):
        super().__init__(
            error_code=BackendErrorMessages.CSV_RENAME_FAILED,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            csv_name=csv_name,
            reason=reason,
        )


class CSVFileAlreadyExists(VisitranBackendBaseException):
    """
    Raised if the csv file already exists with the same name.
    """

    def __init__(self, csv_name: str, created_at: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.CSV_FILE_ALREADY_EXISTS,
            http_status_code=status.HTTP_409_CONFLICT,
            csv_name=csv_name,
            created_at=created_at,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class CSVFileNotUploaded(VisitranBackendBaseException):
    """
    Raised if the csv file already exists with the same name.
    """

    def __init__(self, csv_name: str, reason: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.CSV_FILE_NOT_UPLOADED,
            csv_name=csv_name,
            reason=reason,
        )


class CSVFileNotExists(VisitranBackendBaseException):
    """
    Raised if the csv file does not exist.
    """

    def __init__(self, csv_name: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.CSV_FILE_NOT_EXISTS,
            http_status_code=status.HTTP_404_NOT_FOUND,
            csv_name=csv_name,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class InvalidUserException(VisitranBackendBaseException):
    """
    Raised if the user is invalid.
    """

    def __init__(self) -> None:
        super().__init__(error_code=BackendErrorMessages.INVALID_USER, http_status_code=status.HTTP_401_UNAUTHORIZED)


class BackupNotExistException(VisitranBackendBaseException):
    """
    Raised if the backup does not exist.
    """

    def __init__(self, model_name) -> None:
        super().__init__(
            error_code=BackendErrorMessages.BACKUP_MODEL_NOT_EXIST,
            http_status_code=status.HTTP_404_NOT_FOUND,
            model_name=model_name,
        )


class EnvironmentNotExists(VisitranBackendBaseException):
    """
    Raised if the environment does not exist.
    """

    def __init__(self, environment_id: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.ENVIRONMENT_NOT_EXIST,
            http_status_code=status.HTTP_404_NOT_FOUND,
            environment_id=environment_id,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class EnvironmentAlreadyExist(VisitranBackendBaseException):
    """
    Raised if the environment already exists.
    """

    def __init__(self, env_name: str, created_at: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.ENVIRONMENT_ALREADY_EXIST,
            http_status_code=status.HTTP_409_CONFLICT,
            env_name=env_name,
            created_at=created_at,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class SampleProjectLimitExceed(VisitranBackendBaseException):
    """
    Raised if sample project limit exceed
    """

    def __init__(self, project_base_name: str, sample_project_count: str, sample_project_limit: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.SAMPLE_PROJECT_LIMIT_EXCEED,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            sample_project_count=sample_project_count,
            sample_project_limit=sample_project_limit,
            project_base_name=project_base_name,
        )


class SampleProjectConnectionFailed(VisitranBackendBaseException):
    """
    Raised if the connection data is invalid.
    """

    def __init__(self) -> None:
        super().__init__(
            error_code=BackendErrorMessages.SAMPLE_PROJECT_CONNECTION_FAILED,
            http_status_code=status.HTTP_400_BAD_REQUEST,
        )


class MasterDbNotExist(VisitranBackendBaseException):
    """
    Raised if the master db does not exist.
    """

    def __init__(self) -> None:
        super().__init__(
            error_code="Master database for sample project does not exist",
            http_status_code=status.HTTP_400_BAD_REQUEST,
        )


class ResourcePermissionDeniedException(VisitranBackendBaseException):
    def __init__(self) -> None:
        super().__init__(
            error_code=BackendErrorMessages.RESOURCE_PERMISSION_DENIED,
            http_status_code=status.HTTP_403_FORBIDDEN,
        )


class TableContentIssue(VisitranBackendBaseException):
    def __init__(self, table_name: str, reason: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.TABLE_CONTENT_ISSUE,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            table_name=table_name,
            reason=reason,
        )


class LLMModelFailure(VisitranBackendBaseException):
    def __init__(self, error_message: str, failure_reason: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.LLM_SERVER_FAILURE,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            error_message=error_message,
            failure_reason=failure_reason,
        )


class AIRaisedException(VisitranBackendBaseException):
    def __init__(self, error_message: str, failure_reason: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.AIRaisedException,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            error_message=error_message,
            failure_reason=failure_reason,
        )


class ScheduleJobFailure(VisitranBackendBaseException):
    def __init__(self, error_message: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.SCHEDULE_JOB_FAILURE,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            error_message=error_message,
        )


class SchemaMissingInSeedUpload(VisitranBackendBaseException):

    def __init__(self):
        super().__init__(error_code=BackendErrorMessages.SCHEMA_MISSING_IN_SEED_UPLOAD)


class CsvDownloadFailed(VisitranBackendBaseException):
    """
    Raised when CSV download/export fails.
    """

    def __init__(self, table_name: str, reason: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.CSV_DOWNLOAD_FAILED,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            table_name=table_name,
            reason=reason,
        )


class SchemaNotFoundError(VisitranBackendBaseException):
    """Raised if mentioned table in schema dosen't exists."""

    def __init__(self, project_name: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.SCHEMA_NOT_FOUND,
            project_name=project_name,
        )


# Project Connection Exceptions
class ProjectConnectionGetFailed(VisitranBackendBaseException):
    """Raised when unable to retrieve project connection details."""

    def __init__(self, project_id: str, error_message: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.PROJECT_CONNECTION_GET_FAILED,
            http_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            project_id=project_id,
            error_message=error_message,
        )

    @property
    def severity(self) -> str:
        return "Error"


class ProjectConnectionUpdateFailed(VisitranBackendBaseException):
    """Raised when unable to update project connection details."""

    def __init__(self, project_id: str, error_message: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.PROJECT_CONNECTION_UPDATE_FAILED,
            http_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            project_id=project_id,
            error_message=error_message,
        )

    @property
    def severity(self) -> str:
        return "Error"


class ProjectConnectionTestFailed(VisitranBackendBaseException):
    """Raised when unable to test project connection."""

    def __init__(self, project_id: str, error_message: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.PROJECT_CONNECTION_TEST_FAILED,
            http_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            project_id=project_id,
            error_message=error_message,
        )

    @property
    def severity(self) -> str:
        return "Error"


class ProjectConnectionMissingField(VisitranBackendBaseException):
    """Raised when required field is missing from request."""

    def __init__(self, field_name: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.PROJECT_CONNECTION_MISSING_FIELD,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            field_name=field_name,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class ProjectConnectionInvalidData(VisitranBackendBaseException):
    """Raised when provided connection data is invalid."""

    def __init__(self, error_message: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.PROJECT_CONNECTION_INVALID_DATA,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            error_message=error_message,
        )

    @property
    def severity(self) -> str:
        return "Warning"


# ------------------------------------------------------------------
# Version Control Exceptions
# ------------------------------------------------------------------


class VersionNotFoundException(VisitranBackendBaseException):
    def __init__(self, version_number: int = 0) -> None:
        super().__init__(
            error_code=BackendErrorMessages.VERSION_NOT_FOUND,
            http_status_code=status.HTTP_404_NOT_FOUND,
            version_number=version_number,
        )


class CommitFailedException(VisitranBackendBaseException):
    def __init__(self, model_name: str = "") -> None:
        super().__init__(
            error_code=BackendErrorMessages.COMMIT_FAILED,
            http_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            model_name=model_name,
        )


class VersionConflictException(VisitranBackendBaseException):
    def __init__(self, current_version: int = 0, expected_version: int = 0) -> None:
        super().__init__(
            error_code=BackendErrorMessages.VERSION_CONFLICT,
            http_status_code=status.HTTP_409_CONFLICT,
            current_version=current_version,
            expected_version=expected_version,
        )


class DuplicateContentCommitException(VisitranBackendBaseException):
    def __init__(self, existing_version: int = 0) -> None:
        super().__init__(
            error_code=BackendErrorMessages.DUPLICATE_CONTENT_COMMIT,
            http_status_code=status.HTTP_409_CONFLICT,
            existing_version=existing_version,
        )


class ConcurrentModificationException(VisitranBackendBaseException):
    def __init__(self, model_name: str = "") -> None:
        super().__init__(
            error_code=BackendErrorMessages.CONCURRENT_MODIFICATION,
            http_status_code=status.HTTP_409_CONFLICT,
            model_name=model_name,
        )


class NoChangesToCommitException(VisitranBackendBaseException):
    def __init__(self) -> None:
        super().__init__(
            error_code=BackendErrorMessages.NO_CHANGES_TO_COMMIT,
            http_status_code=status.HTTP_400_BAD_REQUEST,
        )


class GitConnectionFailedException(VisitranBackendBaseException):
    def __init__(self, error_message: str = "") -> None:
        super().__init__(
            error_code=BackendErrorMessages.GIT_CONNECTION_FAILED,
            http_status_code=status.HTTP_502_BAD_GATEWAY,
            error_message=error_message,
        )


class GitPushFailedException(VisitranBackendBaseException):
    def __init__(self, model_name: str = "", error_message: str = "") -> None:
        super().__init__(
            error_code=BackendErrorMessages.GIT_PUSH_FAILED,
            http_status_code=status.HTTP_502_BAD_GATEWAY,
            model_name=model_name,
            error_message=error_message,
        )


class GitRateLimitException(VisitranBackendBaseException):
    def __init__(self) -> None:
        super().__init__(
            error_code=BackendErrorMessages.GIT_RATE_LIMIT,
            http_status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        )


class GitTokenExpiredException(VisitranBackendBaseException):
    def __init__(self) -> None:
        super().__init__(
            error_code=BackendErrorMessages.GIT_TOKEN_EXPIRED,
            http_status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        )


class UnsupportedGitProviderException(VisitranBackendBaseException):
    def __init__(self, repo_url: str = "") -> None:
        super().__init__(
            error_code=BackendErrorMessages.UNSUPPORTED_GIT_PROVIDER,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            repo_url=repo_url,
        )


class GitConfigurationNotFoundException(VisitranBackendBaseException):
    def __init__(self, project_id: str = "") -> None:
        super().__init__(
            error_code=BackendErrorMessages.GIT_CONFIG_NOT_FOUND,
            http_status_code=status.HTTP_404_NOT_FOUND,
            project_id=project_id,
        )


class GitConfigAlreadyExistsException(VisitranBackendBaseException):
    def __init__(self, project_id: str = "") -> None:
        super().__init__(
            error_code=BackendErrorMessages.GIT_CONFIG_ALREADY_EXISTS,
            http_status_code=status.HTTP_409_CONFLICT,
            project_id=project_id,
        )


class GitBranchException(VisitranBackendBaseException):
    def __init__(self, branch_name: str = "", error_message: str = "") -> None:
        super().__init__(
            error_code=BackendErrorMessages.GIT_BRANCH_FAILED,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            branch_name=branch_name,
            error_message=error_message,
        )


class GitBranchAlreadyExistsException(VisitranBackendBaseException):
    def __init__(self, branch_name: str = "") -> None:
        super().__init__(
            error_code=BackendErrorMessages.GIT_BRANCH_ALREADY_EXISTS,
            http_status_code=status.HTTP_409_CONFLICT,
            branch_name=branch_name,
        )


class GitPRException(VisitranBackendBaseException):
    def __init__(self, error_message: str = "") -> None:
        super().__init__(
            error_code=BackendErrorMessages.GIT_PR_FAILED,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            error_message=error_message,
        )


class GitPRAlreadyExistsException(VisitranBackendBaseException):
    def __init__(self, head_branch: str = "", base_branch: str = "") -> None:
        super().__init__(
            error_code=BackendErrorMessages.GIT_PR_ALREADY_EXISTS,
            http_status_code=status.HTTP_409_CONFLICT,
            head_branch=head_branch,
            base_branch=base_branch,
        )
