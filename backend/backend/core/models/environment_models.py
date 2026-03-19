import uuid

from django.db import models
from backend.core.models.connection_models import ConnectionDetails
from backend.utils.tenant_context import get_current_user
from utils.models.base_model import BaseModel
from utils.models.organization_mixin import DefaultOrganizationMixin, DefaultOrganizationManagerMixin
from backend.utils.encryption import encrypt_connection_details, decrypt_connection_details, mask_connection_details


class EnvironmentModelsManager(DefaultOrganizationManagerMixin, models.Manager):
    pass


class EnvironmentModels(DefaultOrganizationMixin, BaseModel):
    """
    This model is used  manage the environment details i.e., to manage the sensitive data in project_connection details.
    """

    @property
    def description(self) -> str:
        return self.environment_description

    def save(self, *args, **kwargs):
        current_user = get_current_user()
        if not self.created_at:  # If the instance is new
            self.created_by = current_user  # Set created_by only for new instances
        self.last_modified_by = current_user  # Update last_modified_by for existing instances

        # Encrypt connection data before saving
        if self.env_connection_data:
            self.env_connection_data = encrypt_connection_details(self.env_connection_data)

        # Finally, call the parent save method
        super(EnvironmentModels, self).save(*args, **kwargs)

    @property
    def decrypted_connection_data(self) -> dict:
        """Get decrypted connection data for internal use."""
        try:
            # First try the old Fernet decryption system
            decrypted_data = decrypt_connection_details(self.env_connection_data)
            
            # If Fernet decryption succeeds, return the data
            # (Don't try RSA decryption on already decrypted data)
            return decrypted_data
            
        except Exception as e:
            # If Fernet decryption fails, try RSA decryption
            try:
                from backend.utils.decryption_utils import decrypt_sensitive_fields
                return decrypt_sensitive_fields(self.env_connection_data)
            except Exception as rsa_error:
                # If both fail, return the original data
                import logging
                logging.exception("Failed to decrypt environment data")
                return self.env_connection_data

    @property
    def masked_connection_data(self) -> dict:
        """Get masked connection data for API responses."""
        return mask_connection_details(self.decrypted_connection_data)

    # Attributes for Environment details
    environment_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    environment_name = models.CharField(unique=True, max_length=100)
    environment_description = models.CharField(max_length=500)
    deployment_type = models.CharField(max_length=100)
    env_connection_data = models.JSONField(default=dict)
    env_custom_data = models.JSONField(default=dict)
    connection_model = models.ForeignKey(ConnectionDetails, on_delete=models.CASCADE, related_name='environment_model')
    is_tested = models.BooleanField(default=False)

    # User specific access control fields
    shared_with = models.JSONField(default=list)
    created_by = models.JSONField(default=dict)
    created_at = models.DateTimeField(auto_now_add=True)
    last_modified_by = models.JSONField(default=dict)
    last_modified_at = models.DateTimeField(auto_now=True)
    is_archived = models.BooleanField(default=False)
    is_deleted = models.BooleanField(default=False)

    # Manager
    objects = EnvironmentModelsManager()

    def __str__(self):
        return self.environment_name
