import os
import re
import shutil
import uuid

from django.db import models

from backend.core.models.connection_models import ConnectionDetails
from backend.core.models.environment_models import EnvironmentModels
from backend.utils.tenant_context import get_current_user, get_current_tenant
from backend.utils.utils import get_project_base_path
from utils.models.base_model import BaseModel
from utils.models.organization_mixin import (
    DefaultOrganizationMixin,
    DefaultOrganizationManagerMixin,
)


class ProjectDetailsManager(DefaultOrganizationManagerMixin, models.Manager):
    pass


class ProjectDetails(DefaultOrganizationMixin, BaseModel):
    """This model creates a Core_ProjectDetails table which is used to manage
    the project information."""

    @staticmethod
    def generate_project_py_name(project_name):
        # Replace all spaces with a single underscore
        project_py_name = re.sub(r"\s+", "_", project_name)

        # Remove any special characters other than underscore
        project_py_name = re.sub(r"[^\w_]", "", project_py_name)

        # Remove any numbers
        # project_py_name = re.sub(r"\d", "", project_py_name)

        # Convert to lowercase
        project_py_name = project_py_name.lower()

        return project_py_name

    @property
    def project_id(self) -> str:
        return str(self.project_uuid)

    @property
    def project_py_path(self) -> str:
        # Adding prefix with Org ID. Tenant id is the org id here.
        tenant_id = get_current_tenant()
        project_path = tenant_id + os.path.sep + get_project_base_path(self.project_id)
        return project_path

    @property
    def connection_name(self) -> str:
        return self.connection_model.connection_name

    @property
    def database_type(self) -> str:
        return self.connection_model.datasource_name

    def save(self, *args, **kwargs):
        current_user = get_current_user()
        if not self.created_at:  # If the instance is new
            self.created_by = current_user  # Set created_by only for new instances

        if not self.project_py_name:
            self.project_py_name = self.generate_project_py_name(project_name=self.project_name)

        self.last_modified_by = current_user  # Update last_modified_by for existing instances

        # Finally, call the parent save method
        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        # Delete files stored by the config parser
        if self.project_py_path and os.path.exists(self.project_py_path):
            shutil.rmtree(self.project_py_path)

        if self.profile_path and os.path.exists(self.profile_path):
            os.remove(self.profile_path)

        if self.is_sample:
            self.connection_model.delete()

            # Delete environment_model if it exists
            if hasattr(self, 'environment_model') and self.environment_model:
                self.environment_model.delete()

        super().delete(*args, **kwargs)

    # Attributes for project details
    project_uuid = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    project_name = models.CharField(max_length=100)
    project_description = models.CharField(max_length=500)
    project_py_name = models.CharField(max_length=100, unique=False, null=True, blank=True)
    connection_model = models.ForeignKey(ConnectionDetails, on_delete=models.CASCADE, related_name="project")
    environment_model = models.ForeignKey(
        EnvironmentModels, on_delete=models.SET_NULL, related_name="project", null=True
    )
    project_path = models.CharField(max_length=100)
    profile_path = models.CharField(max_length=100)

    project_schema = models.CharField(max_length=20, blank=True, null=True)
    # User specific access control fields
    created_by = models.JSONField(default=dict)
    created_at = models.DateTimeField(auto_now_add=True)
    last_modified_by = models.JSONField(default=dict)
    last_modified_at = models.DateTimeField(auto_now=True)
    shared_with = models.JSONField(default=list)
    project_model_graph = models.JSONField(default=dict)
    is_archived = models.BooleanField(default=False)
    is_deleted = models.BooleanField(default=False)
    is_sample = models.BooleanField(default=False)
    is_completed = models.BooleanField(default=False)
    onboarding_enabled = models.BooleanField(
        default=False,
        db_comment="Flag to enable/disable onboarding for this project"
    )
    project_type = models.CharField(
        max_length=50,
        null=True,
        blank=True,
        db_comment="Type of project: jaffle_shop_starter, jaffle_shop_finalize, dvd_rental_starter, dvd_rental_finalizer, or null for normal projects"
    )

    # Manager
    objects = ProjectDetailsManager()

    # Project objects for migrations
    project_objects = models.Manager()

    def __str__(self) -> str:
        return self.project_name
