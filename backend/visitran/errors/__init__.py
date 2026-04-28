from visitran.errors.base_exceptions import VisitranBaseExceptions
from visitran.errors.core_exceptions import (
    ObjectForClassNotFoundError,
    ModelIncludedIsExcluded,
    RelativePathError,
    VisitranPostgresMissingError,
    ProjectNameAlreadyExistsInProfile,
)
from visitran.errors.execution_exceptions import (
    ModelNotFound,
    ModelExecutionFailed,
    ConnectionFailedError,
    SeedFailureException,
    ModelImportError,
    SqlQueryFailed,
    RunSeedFailedException,
)
from visitran.errors.transformation_exceptions import (
    SynthesisColumnNotExist,
    ColumnNotExist,
    TransformationFailed,
    TransformationError,
)
from visitran.errors.validation_exceptions import (
    InvalidSnapshotFields,
    InvalidSnapshotColumns,
    TableNotFound,
    InvalidCSVHeaders,
    SchemaAlreadyExist,
    DatabasePermissionDeniedError,
    SchemaCreationFailed,
    InvalidConnectionUrlException,
    ConnectionFieldMissingException,
)

__all__ = [
    # Unhandled exceptions
    "VisitranBaseExceptions",
    # Snapshots
    "InvalidSnapshotFields",
    "InvalidSnapshotColumns",
    "InvalidCSVHeaders",
    # Table and model not found
    "TableNotFound",
    # Model execution errors
    "ModelNotFound",
    "ModelExecutionFailed",
    "ObjectForClassNotFoundError",
    "ModelIncludedIsExcluded",
    "RelativePathError",
    "VisitranPostgresMissingError",
    "ConnectionFailedError",
    "ProjectNameAlreadyExistsInProfile",
    # Transformations based error codes
    "ColumnNotExist",
    "SynthesisColumnNotExist",
    "TransformationFailed",
    "TransformationError",
    # RUN models errors
    "SeedFailureException",
    "ModelImportError",
    "SqlQueryFailed",
    # Datasource validation errors
    "SchemaAlreadyExist",
    "DatabasePermissionDeniedError",
    "SchemaCreationFailed",
    "InvalidConnectionUrlException",
    "ConnectionFieldMissingException",
]
