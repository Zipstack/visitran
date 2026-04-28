import logging
from typing import Any

from visitran.utils import import_file

from backend.application.utils import get_filter

logger = logging.getLogger(__name__)
from backend.core.models.connection_models import ConnectionDetails
from backend.core.models.environment_models import EnvironmentModels
from backend.core.models.project_details import ProjectDetails
from backend.errors.exceptions import (
    ConnectionAlreadyExists,
    ConnectionDependencyError,
    ConnectionNotExists,
)
from backend.utils.pagination import CustomPaginator


def _get_host_display(con_model):
    """Extract a human-readable host string from connection details.

    Only reads non-sensitive plaintext fields (host, port, account, etc.)
    so no Fernet decryption is needed.
    """
    try:
        details = con_model.connection_details or {}
        ds = con_model.datasource_name
        if ds in ("postgres", "mysql", "trino"):
            host = details.get("host", "")
            port = details.get("port", "")
            return f"{host}:{port}" if host and port else host or None
        if ds == "snowflake":
            return details.get("account") or None
        if ds == "bigquery":
            return details.get("project_id") or None
        if ds == "databricks":
            return details.get("host") or None
        if ds == "duckdb":
            return details.get("file_path") or None
    except Exception:
        logger.warning("Failed to derive host display for connection %s", con_model.connection_id, exc_info=True)
    return None


class ConnectionSession:

    @staticmethod
    def create_connection(connection_details: dict[str, Any]) -> ConnectionDetails:
        filter_condition = get_filter()
        try:
            connection_name = connection_details["name"]
            description = connection_details["description"]
            datasource_name = connection_details["datasource_name"]
            connection_details = connection_details["connection_details"]
            filter_condition["connection_name"] = connection_name
            cd = ConnectionDetails.objects.filter(**filter_condition).first()
            if cd:
                raise ConnectionAlreadyExists(connection_name=connection_name, created_at=cd.created_at)

            connection_model = ConnectionDetails(
                connection_name=connection_name,
                connection_description=description,
                datasource_name=datasource_name,
                connection_details=connection_details,
            )
            connection_model.save()
            return connection_model
        except KeyError as e:
            # TODO - Raise proper exception with all the missing and mandatory keys in one exceptions
            raise Exception(e.__str__())

    @staticmethod
    def get_all_connections(page: int, limit: int, filter_condition: dict[str, Any]) -> Any:
        from django.db.models import Count, Q, Exists, OuterRef

        filter_condition.update(get_filter())
        if "is_deleted" not in filter_condition:
            filter_condition["is_deleted"] = False

        # Annotate counts + sample flag in a single query (no N+1)
        con_qs = (
            ConnectionDetails.objects.filter(**filter_condition)
            .annotate(
                env_count=Count(
                    "environment_model",
                    filter=Q(environment_model__is_deleted=False),
                ),
                project_count=Count("project"),
                is_sample=Exists(
                    ProjectDetails.objects.filter(
                        connection_model_id=OuterRef("connection_id"),
                        is_sample=True,
                    )
                ),
            )
            .order_by("-modified_at")
        )

        custom_paginator = CustomPaginator(queryset=con_qs, limit=limit, page=page)
        con_models = custom_paginator.paginate()

        connection_list = []
        for con_model in con_models.get("page_items"):
            connection_list.append(
                {
                    "id": con_model.connection_id,
                    "name": con_model.connection_name,
                    "description": con_model.connection_description,
                    "datasource_name": con_model.datasource_name,
                    "host": _get_host_display(con_model),
                    "created_by": con_model.created_by,
                    "last_modified_by": con_model.last_modified_by,
                    "db_icon": import_file(f"visitran.adapters.{con_model.datasource_name}").ICON,
                    "is_connection_exist": con_model.is_connection_exist,
                    "is_connection_valid": con_model.is_connection_valid,
                    "connection_flag": con_model.connection_flag,
                    "is_sample_project": con_model.is_sample,
                    "env_count": con_model.env_count,
                    "project_count": con_model.project_count,
                }
            )

        con_models["page_items"] = connection_list
        return con_models

    @staticmethod
    def get_connection_model(connection_id: str) -> ConnectionDetails:
        filter_condition = get_filter()
        filter_condition["connection_id"] = connection_id
        filter_condition["is_deleted"] = False
        con_model = ConnectionDetails.objects.filter(**filter_condition).first()
        if not con_model:
            raise ConnectionNotExists(connection_id=connection_id)
        return con_model

    def get_connection(self, connection_id: str) -> dict[str, Any]:
        """Get connection details by connection id."""
        con_model = self.get_connection_model(connection_id=connection_id)
        if not con_model:
            raise ConnectionNotExists(connection_id=connection_id)

        response_data = {
            "id": con_model.connection_id,
            "name": con_model.connection_name,
            "description": con_model.connection_description,
            "datasource_name": con_model.datasource_name,
            "created_by": con_model.created_by,
            "last_modified_by": con_model.last_modified_by,
            "connection_details": con_model.masked_connection_details,  # Use masked details for API response
            "db_icon": import_file(f"visitran.adapters.{con_model.datasource_name}").ICON,
            "is_connection_exist": con_model.is_connection_exist,
            "is_connection_valid": con_model.is_connection_valid,
            "connection_flag": con_model.connection_flag,
        }
        return response_data

    def update_connection(self, connection_id: str, connection_details: dict[str, Any]) -> ConnectionDetails:
        try:
            con_model: ConnectionDetails = self.get_connection_model(connection_id=connection_id)
            con_model.connection_name = connection_details["name"]
            con_model.connection_description = connection_details.get("description", "")
            con_model.connection_details = connection_details["connection_details"]
            con_model.save()
            return con_model
        except KeyError as e:
            # TODO - Raise proper exception with all the missing and mandatory keys in one exceptions
            raise Exception(e.__str__())

    def validate_dependency(self, env_model: ConnectionDetails) -> None:
        """This method checks for the usage of the current project_connection
        and returns all the reference projects :param env_model:

        ConnectionDetails
        :return: List[dict[project_id: project_details]]
        """
        pass

    def delete_connection(self, connection_id: str) -> None:
        con_model: ConnectionDetails = self.get_connection_model(connection_id=connection_id)
        projects = self.get_projects_by_connection(connection_id=connection_id)
        affected_projects = [project["name"] for project in projects]
        if projects:
            raise ConnectionDependencyError(
                connection_id=connection_id,
                connection_name=con_model.connection_name,
                affected_projects=affected_projects,
            )
        environments = self.get_environments_by_connection(connection_id=connection_id)
        for env in environments:
            env_model: EnvironmentModels = EnvironmentModels.objects.filter(environment_id=env["id"]).first()
            env_model.is_deleted = True
            env_model.save()
        con_model.is_deleted = True
        con_model.save()

    def delete_all_connections(self) -> dict[str, Any]:
        """Delete all connections that have no project dependencies."""
        filter_condition = get_filter()
        filter_condition["is_deleted"] = False
        con_models = ConnectionDetails.objects.filter(**filter_condition)

        connection_ids_to_delete = []
        env_ids_to_delete = []
        skipped = []
        for con_model in con_models:
            projects = self.get_projects_by_connection(
                connection_id=str(con_model.connection_id)
            )
            if projects:
                skipped.append(con_model.connection_name)
                continue
            environments = self.get_environments_by_connection(
                connection_id=str(con_model.connection_id)
            )
            env_ids_to_delete.extend(env["id"] for env in environments)
            connection_ids_to_delete.append(con_model.connection_id)

        # Bulk soft-delete in 2 queries instead of N+M individual saves
        EnvironmentModels.objects.filter(
            environment_id__in=env_ids_to_delete
        ).update(is_deleted=True)
        deleted_count = ConnectionDetails.objects.filter(
            connection_id__in=connection_ids_to_delete
        ).update(is_deleted=True)

        return {"deleted_count": deleted_count, "skipped": skipped}

    def get_projects_by_connection(self, connection_id: str) -> list[dict[str, Any]]:
        connection_model: ConnectionDetails = self.get_connection_model(connection_id=connection_id)
        filter_condition = {"connection_model": connection_model, "is_deleted": False}
        project_models = ProjectDetails.objects.filter(**filter_condition)
        projects_list = []
        for project_model in project_models:
            projects_list.append(
                {
                    "id": project_model.project_id,
                    "name": project_model.project_name,
                    "description": project_model.project_description,
                    "created_by": project_model.created_by,
                    "shared_with": project_model.shared_with,
                }
            )
        return projects_list

    def get_environments_by_connection(self, connection_id: str) -> list[dict[str, Any]]:
        connection_model: ConnectionDetails = self.get_connection_model(connection_id=connection_id)
        filter_condition = {"connection_model": connection_model, "is_deleted": False}
        env_models = EnvironmentModels.objects.filter(**filter_condition)
        environment_list = []
        for env_model in env_models:
            environment_list.append(
                {
                    "id": env_model.environment_id,
                    "name": env_model.environment_name,
                    "description": env_model.environment_description,
                    "is_tested": env_model.is_tested,
                    "created_by": env_model.created_by,
                    "shared_with": env_model.shared_with,
                }
            )
        return environment_list
