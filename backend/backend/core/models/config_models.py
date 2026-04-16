import os
import uuid
from django.core.files.storage import default_storage
from django.db import models

from backend.core.models.project_details import ProjectDetails
from backend.utils.constants import FileConstants as Fc
from backend.utils.tenant_context import get_current_user
from utils.models.base_model import BaseModel
from utils.models.organization_mixin import DefaultOrganizationMixin, DefaultOrganizationManagerMixin


class ConfigModelsManager(DefaultOrganizationManagerMixin, models.Manager):
    pass


class ConfigModels(DefaultOrganizationMixin, BaseModel):
    """
    This model is used to store the no code models.
    """

    class RunStatus(models.TextChoices):
        NOT_STARTED = "NOT_STARTED", "Not Started"
        RUNNING = "RUNNING", "Running"
        SUCCESS = "SUCCESS", "Success"
        FAILED = "FAILED", "Failed"

    def get_model_upload_path(self, filename: str) -> str:
        """
        This returns the file path based on the org and project dynamically.
        :param filename: name of the file
        :return: a string type file path location
        """

        # Adding prefix with Org ID. Tenant id is the org id here.
        base_path = self.project_instance.project_py_path
        project_path = base_path + os.path.sep + self.project_instance.project_py_name

        # Ensure all directories in the path exist
        model_path = str(os.path.join(project_path, Fc.MODELS) + os.path.sep)
        os.makedirs(os.path.dirname(model_path), exist_ok=True)

        # Create __init__.py in project root directory (required for Python package imports)
        project_init_file = os.path.join(project_path, "__init__.py")
        if not os.path.exists(project_init_file):
            open(project_init_file, "w").close()

        # Create __init__.py in models directory
        init_file = os.path.join(model_path, "__init__.py")
        if not os.path.exists(init_file):
            open(init_file, "w").close()

        file_path = os.path.join(model_path, filename + Fc.PY)
        return file_path

    def save(self, *args, **kwargs):
        # Check if there is an existing file with the same name
        try:
            if self.pk:
                old_file = ConfigModels.objects.get(pk=self.pk).model_py_content
                if old_file and old_file.name != self.model_py_content.name:
                    # Delete the old file if it exists and has a different name
                    if default_storage.exists(old_file.path):
                        default_storage.delete(old_file.path)
                self.last_modified_by = get_current_user()
        except ConfigModels.DoesNotExist:
            # The instance does not exist, so it's a new instance
            self.created_by = get_current_user()
        except NotImplementedError:
            # The instance is stored in google cloud storage
            pass
        finally:
            # Saving the current instance
            super(ConfigModels, self).save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        # Removing the file while deleting the record
        if model_content := self.model_py_content:
            try:
                if model_content.file:
                    default_storage.delete(model_content.file.name)
            except FileNotFoundError:
                # No need to delete when the file is not found
                pass
        super(ConfigModels, self).delete(*args, **kwargs)

    class Meta:
        # Ensures model_name is unique per project
        constraints = [
            models.UniqueConstraint(fields=["project_instance", "model_name"], name="unique_model_name_per_project")
        ]

    # Attributes for config models
    project_instance = models.ForeignKey(ProjectDetails, on_delete=models.CASCADE, related_name="config_model")
    model_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    model_name = models.CharField(max_length=100)
    model_data = models.JSONField(default=dict)
    model_py_content = models.FileField(upload_to=get_model_upload_path, storage=default_storage, max_length=250)

    last_modified_by = models.JSONField(default=dict)
    last_modified_at = models.DateTimeField(auto_now=True)

    # Execution status tracking
    run_status = models.CharField(
        max_length=20,
        choices=RunStatus.choices,
        default=RunStatus.NOT_STARTED,
        help_text="Current execution status of the model",
    )
    failure_reason = models.TextField(
        null=True,
        blank=True,
        help_text="Error message if the model execution failed",
    )
    last_run_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Timestamp of the last execution",
    )
    run_duration = models.FloatField(
        null=True,
        blank=True,
        help_text="Duration of last execution in seconds",
    )

    # Current Manager
    config_objects = models.Manager()

    # Manager
    objects = ConfigModelsManager()

    def __str__(self):
        return self.model_name
