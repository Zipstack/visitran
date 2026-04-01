from django.db import models

from backend.core.models.config_models import ConfigModels
from backend.core.models.project_details import ProjectDetails
from utils.models.base_model import BaseModel
from utils.models.organization_mixin import DefaultOrganizationManagerMixin, DefaultOrganizationMixin


class DependentModelsManager(DefaultOrganizationManagerMixin, models.Manager):
    pass


class DependentModels(DefaultOrganizationMixin, BaseModel):
    # Attributes for dependent models
    project_instance = models.ForeignKey(ProjectDetails, on_delete=models.CASCADE, related_name="dependent_model")
    model = models.ForeignKey(ConfigModels, on_delete=models.CASCADE)
    transformation_id = models.CharField(max_length=100)
    model_data = models.JSONField(default=dict)

    # Manager
    objects = DependentModelsManager()
