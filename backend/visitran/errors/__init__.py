from visitran.errors.base_exceptions import VisitranBaseExceptions
from visitran.errors.core_exceptions import (
    ModelIncludedIsExcluded,
    ObjectForClassNotFoundError,
    ProjectNameAlreadyExistsInProfile,
    RelativePathError,
    VisitranPostgresMissingError,
)
from visitran.errors.execution_exceptions import (
    ConnectionFailedError,
    ModelExecutionFailed,
    ModelImportError,
    ModelNotFound,
    RunSeedFailedException,
    SeedFailureException,
    SqlQueryFailed,
)
from visitran.errors.transformation_exceptions import ColumnNotExist, SynthesisColumnNotExist, TransformationFailed
from visitran.errors.validation_exceptions import (
    ConnectionFieldMissingException,
    DatabasePermissionDeniedError,
    InvalidConnectionUrlException,
    InvalidCSVHeaders,
    InvalidSnapshotColumns,
    InvalidSnapshotFields,
    SchemaAlreadyExist,
    SchemaCreationFailed,
    TableNotFound,
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
