# Create your models here.
from django.contrib.auth.models import AbstractUser, Group, Permission
from django.db import models

from backend.constants import FieldLengthConstants as FieldLength

NAME_SIZE = 64
KEY_SIZE = 64


class User(AbstractUser):
    """Stores data related to a user belonging to any organization.

    Every org, user is assumed to be unique.
    """

    # Third Party Authentication User ID
    user_id = models.CharField(max_length=NAME_SIZE)
    project_storage_created = models.BooleanField(default=False)
    profile_picture_url = models.TextField(null=True, blank=True)
    created_by = models.ForeignKey(
        "User",
        on_delete=models.SET_NULL,
        related_name="created_users",
        null=True,
        blank=True,
    )
    modified_by = models.ForeignKey(
        "User",
        on_delete=models.SET_NULL,
        related_name="modified_users",
        null=True,
        blank=True,
    )
    modified_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    # Specify a unique related_name for the groups field
    groups = models.ManyToManyField(
        Group,
        related_name="customuser_set",
        related_query_name="customuser",
        blank=True,
    )

    # Specify a unique related_name for the user_permissions field
    user_permissions = models.ManyToManyField(
        Permission,
        related_name="customuser_set",
        related_query_name="customuser",
        blank=True,
    )

    def __str__(self):  # type: ignore
        return f"User({self.id}, email: {self.email}, userId: {self.user_id})"
