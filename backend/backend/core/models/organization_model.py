# Create your models here.
from django.contrib.auth.models import AbstractUser, Group, Permission
from django.db import models

from backend.constants import FieldLengthConstants as FieldLength
from backend.core.models.user_model import User

NAME_SIZE = 64
KEY_SIZE = 64


class Organization(models.Model):
    """Stores data related to an organization.

    The fields created_by and modified_by is updated after a
    :model:`account.User` is created.
    """

    name = models.CharField(max_length=NAME_SIZE)
    display_name = models.CharField(max_length=NAME_SIZE)
    organization_id = models.CharField(
        max_length=FieldLength.ORG_NAME_SIZE, unique=True
    )

    created_by = models.ForeignKey(
        "User",
        on_delete=models.SET_NULL,
        related_name="created_orgs",
        null=True,
        blank=True,
    )
    modified_by = models.ForeignKey(
        "User",
        on_delete=models.SET_NULL,
        related_name="modified_orgs",
        null=True,
        blank=True,
    )
    stripe_customer_id = models.CharField(
        max_length=128,
        blank=True,
        null=True,
        help_text="Stripe customer ID for billing and payments",
    )
    modified_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    auto_create_schema = True
