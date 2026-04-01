from visitran.errors.base_exceptions import VisitranBaseExceptions
from visitran.errors.error_codes import ErrorCodeConstants


class ColumnNotExist(VisitranBaseExceptions):
    """Raised if the column is not found."""

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
