from django.db import models

from backend.core.models.config_models import ConfigModels
from backend.core.models.project_details import ProjectDetails
from utils.models.base_model import BaseModel
from utils.models.organization_mixin import DefaultOrganizationMixin, DefaultOrganizationManagerMixin


class BackupModelsManager(DefaultOrganizationManagerMixin, models.Manager):
    pass


class BackupModels(DefaultOrganizationMixin, BaseModel):
    """
    This model is used to store the backup models of the models,
    to store the previous success models from ConfigModels
    """

    project_instance = models.ForeignKey(ProjectDetails, on_delete=models.CASCADE, related_name='backup_model')
    config_model = models.ForeignKey(ConfigModels, on_delete=models.CASCADE, related_name='backup_model')
    model_data = models.JSONField(default=dict)

    # Manager
    objects = BackupModelsManager()
