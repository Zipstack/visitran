from visitran.errors.base_exceptions import VisitranBaseExceptions
from visitran.errors.error_codes import ErrorCodeConstants


class ObjectForClassNotFoundError(VisitranBaseExceptions):
    """Raised when no object of a class is found in given list."""

    def __init__(self, values: str, base: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.OBJECT_FOR_CLASS_NOT_FOUND,
            values=values,
            base=base,
        )


class ModelIncludedIsExcluded(VisitranBaseExceptions):
    """Raised when nodes included from include list is again excluded by
    exclude list."""

    def __init__(self, includes: str, excludes: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.MODEL_INCLUDED_IS_EXCLUDED,
            includes=includes,
            excludes=excludes,
        )


class RelativePathError(VisitranBaseExceptions):
    """Raised when a relative path is passed as project-path argument."""

    def __init__(self, file_name: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.PYTHON_RELATIVE_PATH_ERROR,
        )


class VisitranPostgresMissingError(VisitranBaseExceptions):
    """Raised when visitran[postgres] package is not installed."""

    def __init__(self) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.POSTGRES_PACKAGE_MISSING,
        )


class ProjectNameAlreadyExistsInProfile(VisitranBaseExceptions):
    """Raised when visitran[postgres] package is not installed."""

    def __init__(self, project_name: str, profile_name: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.PROJECT_NAME_ALREADY_EXISTS,
            project_name=project_name,
            profile_name=profile_name,
        )
