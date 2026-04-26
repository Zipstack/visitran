from typing import Any

from visitran.utils import import_file

from backend.application.session.connection_session import ConnectionSession, _get_host_display
from backend.application.utils import get_filter
from backend.core.models.environment_models import EnvironmentModels
from backend.core.models.project_details import ProjectDetails
from backend.errors.exceptions import EnvironmentAlreadyExist, EnvironmentNotExists
from backend.utils.pagination import CustomPaginator


class EnvironmentSession:
    def __init__(self) -> None:
        self._connection_session = ConnectionSession()

    @staticmethod
    def _merge_connection_data(
        frontend_data: dict[str, Any], connection_model
    ) -> dict[str, Any]:
        """Merge frontend connection details with stored decrypted values.

        The frontend may send masked fields (e.g. '********' for passw).
        Use the connection model's decrypted details as the base and overlay
        only non-masked values from the frontend.
        """
        base = connection_model.decrypted_connection_details.copy()
        if not frontend_data:
            return base
        for key, value in frontend_data.items():
            if isinstance(value, str) and value:
                stripped = value.replace(" ", "")
                # Skip fully masked values like '********'
                if stripped and all(c == "*" for c in stripped):
                    continue
                # Skip partially masked values (e.g. masked URLs with *s)
                if "***" in value:
                    continue
            base[key] = value
        return base

    def create_environment(self, environment_details: dict[str, Any]) -> EnvironmentModels:
        try:
            filter_condition = get_filter()
            environment_name = environment_details["name"]
            connection_id = environment_details.get("connection", {}).get("id", "")
            connection_model = self._connection_session.get_connection_model(connection_id=connection_id)
            filter_condition["environment_name"] = environment_name
            ev = EnvironmentModels.objects.filter(**filter_condition).first()
            if ev:
                raise EnvironmentAlreadyExist(env_name=environment_name, created_at=ev.created_at)
            env_connection_data = self._merge_connection_data(
                environment_details.get("connection_details", {}), connection_model
            )
            env_model = EnvironmentModels(
                environment_name=environment_name,
                environment_description=environment_details.get("description", ""),
                deployment_type=environment_details["deployment_type"],
                env_connection_data=env_connection_data,
                env_custom_data=environment_details.get("custom_data", {}),
                connection_model=connection_model,
            )
            env_model.save()
            return env_model
        except KeyError as e:
            # TODO - Raise proper exception with all the missing and mandatory keys in one exceptions
            raise Exception(e.__str__())

    @staticmethod
    def get_environment_model(environment_id: str) -> EnvironmentModels:
        env_model = EnvironmentModels.objects.filter(environment_id=environment_id, is_deleted=False).first()
        if not env_model:
            raise EnvironmentNotExists(environment_id=environment_id)
        return env_model

    @staticmethod
    def get_all_environment_models(filter_condition: dict[str, Any]) -> Any:
        filter_condition.update(get_filter())
        if "is_deleted" not in filter_condition:
            filter_condition["is_deleted"] = False
        env_models = EnvironmentModels.objects.filter(**filter_condition)
        return env_models

    def get_all_environments(self, page: int, limit: int, filter_condition: dict[str, Any]) -> Any:
        env_models = self.get_all_environment_models(filter_condition=filter_condition).order_by("-modified_at")

        custom_paginator = CustomPaginator(queryset=env_models, limit=limit, page=page)
        env_models = custom_paginator.paginate()

        env_data = []
        for env_model in env_models.get("page_items"):
            env_data.append(
                {
                    "id": env_model.environment_id,
                    "name": env_model.environment_name,
                    "description": env_model.environment_description,
                    "deployment_type": env_model.deployment_type,
                    "connection": {
                        "id": env_model.connection_model.connection_id,
                        "name": env_model.connection_model.connection_name,
                        "datasource_name": env_model.connection_model.datasource_name,
                        "db_icon": import_file(f"visitran.adapters.{env_model.connection_model.datasource_name}").ICON,
                        "host": _get_host_display(env_model.connection_model),
                        "connection_flag": env_model.connection_model.connection_flag,
                    },
                    "is_tested": env_model.is_tested,
                }
            )
        env_models["page_items"] = env_data
        return env_models

    def get_environment(self, environment_id: str) -> dict[str, Any]:
        env_model: EnvironmentModels = self.get_environment_model(environment_id=environment_id)
        env_data = {
            "id": env_model.environment_id,
            "name": env_model.environment_name,
            "description": env_model.environment_description,
            "deployment_type": env_model.deployment_type,
            "connection": {
                "id": env_model.connection_model.connection_id,
                "name": env_model.connection_model.connection_name,
                "datasource_name": env_model.connection_model.datasource_name,
            },
            "connection_details": env_model.masked_connection_data,
            "custom_data": env_model.env_custom_data,
            "is_tested": env_model.is_tested,
        }
        return env_data

    def update_environment(self, environment_id: str, environment_details: dict[str, Any]) -> EnvironmentModels:
        try:
            env_model: EnvironmentModels = self.get_environment_model(environment_id=environment_id)
            env_model.environment_name = environment_details["name"]
            env_model.environment_description = environment_details.get("description", "")
            env_model.deployment_type = environment_details["deployment_type"]
            env_connection_data = self._merge_connection_data(
                environment_details.get("connection_details", {}), env_model.connection_model
            )
            env_model.env_connection_data = env_connection_data
            env_model.env_custom_data = environment_details.get("custom_data", {})
            env_model.save()
            return env_model
        except KeyError as e:
            # TODO - Raise proper exception with all the missing and mandatory keys in one exceptions
            raise Exception(e.__str__())

    def projects_by_environment(self, environment_id: str) -> list[dict[str, Any]]:
        env_model: EnvironmentModels = self.get_environment_model(environment_id=environment_id)
        filter_condition = {"environment_model": env_model, "is_deleted": False, "is_archived": False}
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

    def validate_dependency(self, env_model: EnvironmentModels) -> None:
        """This method checks for the usage of the current environment and
        returns all the reference projects :param env_model: EnvironmentModels
        :return: List[dict[project_id: project_details]]"""
        pass

    def delete_environment(self, environment_id: str) -> None:
        # TODO - Need to add environment validation here before deleting
        env_model: EnvironmentModels = self.get_environment_model(environment_id=environment_id)
        env_model.delete()
