from visitran.errors.base_exceptions import VisitranBaseExceptions
from visitran.errors.error_codes import ErrorCodeConstants


class InvalidSnapshotFields(VisitranBaseExceptions):
    """Raised if the configurations file passed for snapshot is not
    adequete."""

    def __init__(self, invalid_fields: list[str]) -> None:
        super().__init__(error_code=ErrorCodeConstants.INVALID_SNAPSHOT_FIELDS, invalid_fields=invalid_fields)


class InvalidSnapshotColumns(VisitranBaseExceptions):
    """Raised if the columns specified in the check cols attribute for snapshot
    is restricted or not exist in table DB."""

    def __init__(self, invalid_cols: list[str]) -> None:
        super().__init__(error_code=ErrorCodeConstants.INVALID_SNAPSHOT_COLUMNS, invalid_cols=invalid_cols)


class TableNotFoundError(VisitranBaseExceptions):
    """Raised if mentioned table in schema dosen't exists."""

    def __init__(self, table_name: str, schema_name: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.TABLE_NOT_FOUND,
            table_name=table_name,
            schema_name=schema_name,
        )


class ModelNotFoundError(VisitranBaseExceptions):
    """Raised when a python module is not found."""

    def __init__(self, module_name: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.MODEL_NOT_FOUND,
            module_name=module_name,
        )


class NodeExecuteError(VisitranBaseExceptions):
    """Raised when a node execution fails."""

    def __init__(self, schema_name: str, table_name: str, node: str, error_message: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.NODE_EXECUTE_ERROR,
            schema_name=schema_name,
            table_name=table_name,
            node=node,
            error_message=error_message,
        )


class ObjectForClassNotFoundError(VisitranBaseExceptions):
    """Raised when no object of a class is found in given list."""

    def __init__(self, values: str, base: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.OBJECT_FOR_CLASS_NOT_FOUND,
            values=values,
            base=base,
        )


class NotSupportedError(VisitranBaseExceptions):
    """Raised when no object of a class is found in given list."""

    def __init__(self, action: str, connector: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.NOT_SUPPORTED,
            action=action,
            connector=connector,
        )


class DoesNotExistError(VisitranBaseExceptions):
    """Raised when no object of a class is found in given list."""

    def __init__(self, object: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.DOES_NOT_EXIST,
            object=object,
        )


class EmptyFileError(VisitranBaseExceptions):
    """Raised when no object of a class is found in given list."""

    def __init__(self, file: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.EMPTY_FILE,
            file=file,
        )


class NodeIncludedIsExcludedError(VisitranBaseExceptions):
    """Raised when nodes included from include list is again excluded by
    exclude list."""

    def __init__(self, includes: str, excludes: str) -> None:
        super().__init__(
            error_code=ErrorCodeConstants.NODES_INCLUDED_IS_EXCLUDED,
            includes=includes,
            excludes=excludes,
        )
