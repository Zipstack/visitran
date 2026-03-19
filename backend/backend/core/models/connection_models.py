import uuid

from django.db import models

from backend.utils.encryption import encrypt_connection_details, decrypt_connection_details, mask_connection_details
from backend.utils.tenant_context import get_current_user
from utils.models.base_model import BaseModel
from utils.models.organization_mixin import DefaultOrganizationMixin, DefaultOrganizationManagerMixin


class ConnectionDetailsManager(DefaultOrganizationManagerMixin, models.Manager):
    pass


class ConnectionDetails(DefaultOrganizationMixin, BaseModel):
    """
    This project_connection details model is used to create a table called Core_ConnectionDetails in DB to manage the
    project_connection fields and datasource type
    """

    @property
    def description(self) -> str:
        return self.connection_description

    # Attributes for DB Connection details
    connection_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    connection_name = models.CharField(max_length=100)
    datasource_name = models.CharField(max_length=100)
    connection_description = models.CharField(max_length=500)
    connection_details = models.JSONField(default=dict)
    is_connection_exist = models.BooleanField(default=True)
    is_connection_valid = models.BooleanField(default=True)

    # User specific access control fields
    shared_with = models.JSONField(default=list)
    created_by = models.JSONField(default=dict)
    last_modified_by = models.JSONField(default=dict)
    last_modified_at = models.DateTimeField(auto_now=True)
    is_archived = models.BooleanField(default=False)
    is_deleted = models.BooleanField(default=False)

    # Manager
    objects = ConnectionDetailsManager()

    def __str__(self):
        return self.connection_name

    def save(self, *args, **kwargs):
        current_user = get_current_user()
        if not self.created_at:  # If the instance is new
            self.created_by = current_user  # Set created_by only for new instances
        self.last_modified_by = current_user  # Update last_modified_by for existing instances

        # Encrypt connection details before saving
        if self.connection_details:
            self.connection_details = encrypt_connection_details(self.connection_details)

        # Finally, call the parent save method
        super(ConnectionDetails, self).save(*args, **kwargs)

    @property
    def decrypted_connection_details(self) -> dict:
        """Get decrypted connection details for internal use."""
        return decrypt_connection_details(self.connection_details)

    @property
    def masked_connection_details(self) -> dict:
        """Get masked connection details for API responses."""
        return mask_connection_details(self.decrypted_connection_details)

    @property
    def connection_flag(self) -> str:
        if self.is_connection_exist and self.is_connection_valid:
            return "GREEN"
        elif self.is_connection_exist:
            return "YELLOW"
        return "RED"
