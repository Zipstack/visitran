import logging
from pathlib import Path
from typing import Any, Optional, List, Dict

from django.db.models import Count, Q

from visitran.utils import import_file

from backend.application.file_explorer.file_explorer import FileExplorer
from backend.application.session.connection_session import ConnectionSession
from backend.application.session.env_session import EnvironmentSession
from backend.application.session.session import Session
from backend.application.utils import get_filter
from backend.application.visitran_backend_context import VisitranBackendContext
from backend.core.constants.reserved_names import ProjectNameConstants
from backend.core.models.connection_models import ConnectionDetails
from backend.core.models.project_details import ProjectDetails
from backend.errors.exceptions import ProjectAlreadyExists, ProjectNameReservedError


class BaseContext:
    """This base context will read the cache and associates the project and
    profile paths based on the project name."""

    def __init__(self, project_id: str, environment_id: str = "") -> None:
        self._project_name = project_id
        self._environment_id = environment_id
        self._environment_session = None
        self._visitran_context: Optional[VisitranBackendContext] = None
        self._project_db_instance = None
        self._project_path = None
        self._profile_path = None
        self._session = Session(project_id=project_id)
        self._file_explorer: FileExplorer = FileExplorer(project_instance=self.project_instance)

    @classmethod
    def create_connection(
        cls,
        connection_name: str,
        datasource_name: str,
        connection_details: dict[str, Any],
    ) -> ConnectionDetails:
        con_session = ConnectionSession()
        con_details = {
            "name": connection_name,
            "description": "",
            "datasource_name": datasource_name,
            "connection_details": connection_details,
        }
        connection_model = con_session.create_connection(con_details)
        return connection_model

    @property
    def env_session(self) -> EnvironmentSession:
        if not self._environment_session:
            self._environment_session = EnvironmentSession()
        return self._environment_session

    @classmethod
    def create_project(cls, project_details: dict[str, Any]):
        project_name = project_details["project_name"].strip()

        # Check for reserved names
        if ProjectNameConstants.is_reserved_name(project_name):
            raise ProjectNameReservedError(project_name=project_name)

        # Check if project already exists
        pd = ProjectDetails.objects.filter(project_name=project_name).first()
        if pd:
            raise ProjectAlreadyExists(project_name=project_name, created_at=pd.created_at)
        default_path = Path.home() / Path(".visitran") / Path(f"{project_name}")

        connection = project_details.get("connection", {})
        con_session = ConnectionSession()
        connection_model = con_session.get_connection_model(connection_id=connection.get("id"))

        environment_model = None
        if environment := project_details.get("environment", {}):
            env_session = EnvironmentSession()
            environment_model = env_session.get_environment_model(environment_id=environment.get("id"))

        pd = ProjectDetails(
            project_name=project_details["project_name"],
            project_description=project_details.get("description", ""),
            connection_model=connection_model,
            environment_model=environment_model,
            project_path=default_path,
        )
        pd.save()

        # Cloud: auto-create owner permission for the project creator
        try:
            from pluggable_apps.project_sharing.services import create_owner_permission
            from backend.core.models.user_model import User
            created_by = pd.created_by or {}
            owner_user = User.objects.filter(username=created_by.get("username")).first()
            if owner_user:
                create_owner_permission(pd, owner_user)
        except Exception:
            pass  # OSS: no sharing module, or table not migrated yet

        return pd.project_id

    def get_project_details(self) -> dict[str, Any]:
        project_details = {
            "project_id": self.session.project_instance.project_id,
            "project_name": self.session.project_instance.project_name,
            "db_name": self.session.project_instance.database_type,
            "description": self.session.project_instance.project_description,
            "created_at": self.session.project_instance.created_at,
            "modified_at": self.session.project_instance.last_modified_at,
            "db_icon": import_file(f"visitran.adapters.{self.session.project_instance.database_type}").ICON,
            "created_by": self.session.project_instance.created_by,
            "connection": {
                "id": self.session.project_instance.connection_model.connection_id,
                "name": self.session.project_instance.connection_model.connection_name,
            },
            "environment": {},
        }
        if env_model := self.session.project_instance.environment_model:
            project_details["environment"] = {
                "id": env_model.environment_id,
                "name": env_model.environment_name,
            }
        return project_details

    def update_a_project(self, project_details: dict[str, Any]):
        self.session.update_project_details(project_details)

    def delete_project(self):
        self.session.delete_project()

    @classmethod
    def get_project_lists(
        cls, search: str = "", page: int = 1, page_size: int = 20, sort_by: str = "modified"
    ) -> dict:
        """Fetches paginated, searchable project list.

        Returns dict with ``page_items``, ``total``, ``page``, ``page_size``.
        """
        SORT_MAP = {
            "modified": "-last_modified_at",
            "created": "-created_at",
            "name": "project_name",
        }
        order_field = SORT_MAP.get(sort_by, "-last_modified_at")
        project_list = []
        filter_condition = get_filter()
        if search:
            filter_condition["project_name__icontains"] = search
        queryset = (
            ProjectDetails.objects.filter(**filter_condition)
            .select_related("connection_model")
            .order_by(order_field)
        )

        # Cloud: filter to only projects the user can access
        _check_access = None
        _current_user = None
        try:
            from pluggable_apps.project_sharing.services import (
                check_project_access,
                filter_accessible_projects,
            )
            from backend.utils.tenant_context import _get_tenant_context
            _current_user = _get_tenant_context().user
            if _current_user:
                queryset = filter_accessible_projects(queryset, _current_user)
                _check_access = check_project_access
        except Exception:
            pass  # OSS: no sharing module, or table not migrated yet

        # Annotate counts to avoid N+1 queries
        annotations = {}
        if hasattr(ProjectDetails, "config_model"):
            annotations["_total_models"] = Count("config_model", distinct=True)
        if hasattr(ProjectDetails, "chat_project"):
            annotations["_total_ai_chats"] = Count(
                "chat_project",
                filter=Q(chat_project__is_deleted=False),
                distinct=True,
            )
        # Only annotate user_tasks if scheduler app is installed
        from django.apps import apps
        if apps.is_installed("job_scheduler") and hasattr(ProjectDetails, "user_tasks"):
            annotations["_total_scheduled_jobs"] = Count("user_tasks", distinct=True)
            annotations["_total_active_jobs"] = Count(
                "user_tasks__periodic_task",
                filter=Q(user_tasks__periodic_task__enabled=True),
                distinct=True,
            )
            annotations["_total_failed_job"] = Count(
                "user_tasks",
                filter=Q(user_tasks__status="FAILED PERMANENTLY"),
                distinct=True,
            )

        if annotations:
            queryset = queryset.annotate(**annotations)

        total = queryset.count()
        offset = (max(page, 1) - 1) * page_size
        projects = queryset[offset : offset + page_size]
        for project in projects:
            project_list.append(
                {
                    "project_id": project.project_id,
                    "project_name": project.project_name,
                    "db_name": project.database_type,
                    "description": project.project_description,
                    "created_at": project.created_at,
                    "modified_at": project.last_modified_at,
                    "db_icon": import_file(f"visitran.adapters.{project.database_type}").ICON,
                    "created_by": project.created_by,
                    "is_sample": project.is_sample,
                    "project_type": (
                        "Starter" if project.is_sample and project.project_type and "starter" in project.project_type.lower()
                        else "Finalized" if project.is_sample and project.project_type
                        else ""
                    ),
                    "is_completed": project.is_completed,
                    "connection": {
                        "id": project.connection_model.connection_id,
                        "name": project.connection_model.connection_name,
                        "datasource": project.connection_model.datasource_name,
                        "is_connection_exist": project.connection_model.is_connection_exist,
                        "is_connection_valid": project.connection_model.is_connection_valid,
                        "connection_flag": project.connection_model.connection_flag,
                    },
                    "total_scheduled_jobs": getattr(project, "_total_scheduled_jobs", 0),
                    "total_active_jobs": getattr(project, "_total_active_jobs", 0),
                    "total_failed_job": getattr(project, "_total_failed_job", 0),
                    "total_models": getattr(project, "_total_models", 0),
                    "total_ai_chats": getattr(project, "_total_ai_chats", 0),
                }
            )
            # Cloud: include user's role on the project
            if _check_access and _current_user:
                project_list[-1]["user_role"] = _check_access(_current_user, project)

        # Cloud: batch-fetch shared users for avatar display
        try:
            from pluggable_apps.project_sharing.services import get_shared_users_for_projects
            project_uuids = [p.project_uuid for p in projects]
            shared_map = get_shared_users_for_projects(project_uuids)
            for item in project_list:
                item["shared_users"] = shared_map.get(item["project_id"], [])
        except Exception:
            pass  # OSS: no sharing module

        return {
            "page_items": project_list,
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    @classmethod
    def check_project_existence(cls, project_name: str):
        project_name = project_name.strip()
        if project_details := ProjectDetails.objects.filter(project_name=project_name):
            project_detail = project_details[0]
            raise ProjectAlreadyExists(
                project_name=project_detail.project_name,
                created_at=project_detail.created_at,
            )

    @property
    def project_name(self) -> str:
        """Returns the name of the current project."""
        return self._project_name

    @property
    def project_instance(self) -> ProjectDetails:
        """This method returns the DB instance of the current project
        reference."""
        return self.session.project_instance

    @property
    def connection(self) -> ConnectionDetails:
        return self.project_instance.connection_model

    @property
    def project_path(self) -> str:
        """Returns the current project path as string."""
        if not self._project_path:
            self._project_path = self.project_instance.project_path
        return self._project_path

    @property
    def profile_path(self) -> str:
        """Returns the current profile path as string."""
        if not self._profile_path:
            self._profile_path = self.project_instance.profile_path
        return self._profile_path

    @property
    def visitran_context(self) -> VisitranBackendContext:
        """Returns the object of VisitranContext."""
        if not self._visitran_context:
            logging.info(f"Reloading context for project: {self.project_name}")
            return self._reload_context()
        return self._visitran_context

    @property
    def file_explorer(self) -> FileExplorer:
        """Returns the object of file explorer."""
        return self._file_explorer

    @property
    def session(self) -> Session:
        """Returns the object of file explorer."""
        return self._session

    def load_connection_details(self) -> dict[str, Any]:
        """This method loads the env model from run payload, if not exists it
        overrides with connection model."""
        connection_details = self.project_instance.connection_model.decrypted_connection_details
        if self._environment_id:
            env_model = self.env_session.get_environment_model(
                environment_id=self._environment_id
            )
            connection_details = env_model.decrypted_connection_data
        elif env_model := self.project_instance.environment_model:
            connection_details = env_model.decrypted_connection_data
        return connection_details

    def _reload_context(self, env_data: dict[str, Any] = None) -> VisitranBackendContext:
        project_config = {
            "db_type": self.project_instance.database_type,
            "project_path": self.project_instance.project_path,
            "connection_details": self.project_instance.connection_model.decrypted_connection_details,
            "project_schema": self.project_instance.project_schema,
        }
        self._visitran_context: VisitranBackendContext = VisitranBackendContext(
            project_config=project_config,
            is_api_call=True,
            session=self.session,
            env_data=env_data,
        )
        return self._visitran_context

    @property
    def redis_model_key(self):
        redis_key = f"{self.session.tenant_id}_{self.project_instance.project_id}_{self.project_instance.connection_model.connection_id}_models"
        return redis_key

    @property
    def redis_db_metadata_key(self):
        redis_key = f"{self.session.tenant_id}_{self.project_instance.project_id}_{self.project_instance.connection_model.connection_id}_db_metadata"
        return redis_key
