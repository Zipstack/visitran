import os
import uuid
import logging
from django.core.files.storage import default_storage
from django.db import models
from django.utils.timezone import now
from django.core.files.base import ContentFile

from backend.core.models.project_details import ProjectDetails
from backend.utils.constants import FileConstants as Fc
from backend.utils.tenant_context import get_current_user

from backend.errors.exceptions import CSVFileNotExists, UnhandledErrorMessage, CSVRenameFailed
from utils.models.base_model import BaseModel
from utils.models.organization_mixin import DefaultOrganizationMixin, DefaultOrganizationManagerMixin


class CSVModelsManager(DefaultOrganizationManagerMixin, models.Manager):
    pass


class CSVModels(DefaultOrganizationMixin, BaseModel):

    def rename_csv_file(self, filename: str):
        """Rename the file to `filename` while keeping the same extension and
        folder."""
        if not self.csv_field:
            return
        old_path = self.csv_field.name
        old_filename = self.csv_name
        try:
            file_content = default_storage.open(old_path).read()
            base_dir = os.path.dirname(old_path)
            new_path = os.path.join(base_dir, f"{filename}")
            # Save the file with the new name
            default_storage.save(new_path, ContentFile(file_content))
            # Delete the old file
            default_storage.delete(old_path)

            self.csv_field.name = new_path

            self.csv_name = filename
            self.table_name = None
            self.table_schema = None
            self.status = 'uploaded'
            self.save()
        except FileNotFoundError:
            logging.error(f"failed to rename csv file {old_path}")
            raise CSVFileNotExists(self.csv_name)
        except OSError as e:
            logging.error(f"IOError: failed to rename csv file {old_path}. Error : {str(e)}")
            raise CSVRenameFailed(csv_name=self.csv_name,  reason=str(e))
        except Exception as e:
            logging.critical(f"Exception: failed to rename csv file {old_path}, Error: {str(e)}")
            raise CSVRenameFailed(csv_name=self.csv_name,  reason=str(e))

    def get_csv_upload_path(self, filename: str) -> str:
        # Using Django slugify to avoid path traversal
        filename = filename.split(os.path.sep)[-1]
        # Adding prefix with Org ID. Tenant id is the org id here.
        base_path = self.project_instance.project_py_path
        csv_file_path = os.path.join(base_path, Fc.SEEDS, filename)
        return csv_file_path

    def delete(self, *args, **kwargs):
        # Removing the file while deleting the record
        if self.csv_field.file:
            default_storage.delete(self.csv_field.file.name)
        super().delete(*args, **kwargs)

    def save(self, *args, **kwargs):
        # Check if there is an existing file with the same name
        try:
            if self.pk:
                old_file = CSVModels.objects.get(pk=self.pk).csv_field
                if old_file and old_file.name != self.csv_field.name:
                    # Delete the old file if it exists and has a different name
                    if default_storage.exists(old_file.name):
                        default_storage.delete(old_file.name)
        except CSVModels.DoesNotExist:
            # The instance does not exist, so it's a new instance
            self.uploaded_by = get_current_user()
        finally:
            # Saving the current instance
            super().save(*args, **kwargs)

    class Meta:
        # Ensures csv_name is unique per project
        constraints = [
            models.UniqueConstraint(fields=["project_instance", "csv_name"], name="unique_csv_name_per_project")
        ]

    # Attributes for csv files
    project_instance = models.ForeignKey(ProjectDetails, on_delete=models.CASCADE, related_name="csv_model")
    csv_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    csv_name = models.CharField(max_length=100)
    csv_field = models.FileField(upload_to=get_csv_upload_path, storage=default_storage)

    table_name = models.CharField(max_length=255, null=True, blank=True)
    table_schema = models.CharField(max_length=255, null=True, blank=True)
    upload_time = models.DateTimeField(default=now)
    processed_time = models.DateTimeField(null=True, blank=True)
    status = models.CharField(
        max_length=50,
        choices=[
            ("uploaded", "Uploaded"),
            ("in_progress", "In Progress"),
            ("completed", "Completed"),
            ("success", "Success"),
            ("failed", "Failed"),
        ],
        default="uploaded",
    )
    error_message = models.TextField(null=True, blank=True)
    table_exists = models.BooleanField(default=False)  # Tracks table existence

    # User information fields
    uploaded_by = models.JSONField(default=dict)
    updated_at = models.DateTimeField(auto_now_add=True)

    # Manager
    objects = CSVModelsManager()

    def __str__(self):
        return self.csv_name
