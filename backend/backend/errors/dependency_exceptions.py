from typing import Any

from rest_framework import status

from backend.errors.error_codes import BackendErrorMessages
from backend.errors.visitran_backend_base_exceptions import VisitranBackendBaseException


def beautify_transformation_name(transformation_name: str) -> str:
    beautifier = {
        "join": "Join",
        "union": "Merge",
        "pivot": "Pivot",
        "combine_columns": "Combine columns",
        "synthesize": "Add columns",
        "filter": "Filters",
        "groups_and_aggregation": "Groups & Aggregation",
        "distinct": "Drop duplicates",
        "rename_column": "Rename columns",
        "find_and_replace": "Find & Replace",
        "sort": "Sort",
        "clear_all": "Clear All Transforms",
    }
    return beautifier.get(transformation_name, transformation_name)


class ColumnDependency(VisitranBackendBaseException):
    """Raised if the model is configured with invalid source table."""

    def __init__(
        self, model_name: str, transformation_name: str, affected_columns: list[str], affected_transformation: str = ""
    ) -> None:
        super().__init__(
            error_code=BackendErrorMessages.COLUMN_DEPENDENCY,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            affected_columns=affected_columns,
            model_name=model_name,
            transformation_name=beautify_transformation_name(transformation_name),
            affected_transformation=beautify_transformation_name(affected_transformation),
        )

    @property
    def severity(self) -> str:
        return "Warning"


class MultipleColumnDependency(VisitranBackendBaseException):
    """Raised if the model is configured with invalid source table."""

    def __init__(
        self,
        model_name: str,
        transformation_name: str,
        affected_columns: list[str],
        dependency_details: dict[str, Any] = None,
    ) -> None:
        super().__init__(
            error_code=BackendErrorMessages.MULTIPLE_COLUMN_DEPENDENCY,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            model_name=model_name,
            transformation_name=beautify_transformation_name(transformation_name),
            affected_columns_count=affected_columns.__len__(),
            affected_columns=affected_columns,
            dependency_details=self.format_dependency_details(model_name, dependency_details),
        )

    @staticmethod
    def format_dependency_details(cur_model_name: str, dependency_dict: dict[str, Any]) -> str:
        details = []
        for model_name, transformations in dependency_dict.items():
            if model_name == cur_model_name:
                model_name = f"{model_name} (Current model)"
            details.append(f"\n**{model_name}**:")
            for transformation_type, columns in transformations.items():
                column_list = ", ".join([f'"{col}"' for col in columns])
                details.append(f"• **{beautify_transformation_name(transformation_type).title()}**: {column_list}")
        return "\n".join(details)

    @property
    def severity(self) -> str:
        return "Warning"


class ModelTableDependency(VisitranBackendBaseException):
    def __init__(self, child_models: list[str], model_name: str, table_name: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.MODEL_TABLE_DEPENDENCY,
            http_status_code=status.HTTP_409_CONFLICT,
            model_name=model_name,
            child_models=child_models,
            table_name=table_name,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class TransformationDependency(VisitranBackendBaseException):
    """Raised if the model is configured with invalid source table."""

    def __init__(
        self, model_name: str, affected_columns: list[str], transformation_name: str, affected_transformation: str
    ) -> None:
        super().__init__(
            error_code=BackendErrorMessages.TRANSFORMATION_CONFLICT,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            affected_columns=affected_columns,
            model_name=model_name,
            transformation_name=transformation_name,
            affected_transformation=affected_transformation,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class ModelDependency(VisitranBackendBaseException):
    """Raised if the current model is dependent on another model when the
    current model is tried to delete."""

    def __init__(self, child_models: list[str], model_name: str) -> None:
        super().__init__(
            error_code=BackendErrorMessages.MODEL_DEPENDENCY,
            http_status_code=status.HTTP_409_CONFLICT,
            model_name=model_name,
            child_models=child_models,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class ProjectDependencyException(VisitranBackendBaseException):
    """Raised when attempting to delete a project that has associated jobs."""

    def __init__(self, project_name: str, jobs: list[str]) -> None:
        super().__init__(
            error_code=BackendErrorMessages.PROJECT_DEPENDENCY,
            http_status_code=status.HTTP_409_CONFLICT,
            project_name=project_name,
            jobs=jobs,
            job_count=len(jobs),
        )

    @property
    def severity(self) -> str:
        return "Warning"
