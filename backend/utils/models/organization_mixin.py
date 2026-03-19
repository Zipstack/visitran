import logging

from django.db import models

from backend.core.models.organization_model import Organization
from backend.utils.tenant_context import get_organization


class DefaultOrganizationMixin(models.Model):
    organization = models.ForeignKey(
        Organization,
        on_delete=models.CASCADE,
        db_comment="Foreign key reference to the Organization model.",
        null=True,
        blank=True,
        default=None,
    )

    class Meta:
        abstract = True

    def save(self, *args, **kwargs):
        if self.organization is None:
            self.organization = get_organization()
        super().save(*args, **kwargs)


class DefaultOrganizationManagerMixin(models.Manager):
    def get_queryset(self):
        organization = get_organization()
        logging.info(f"================ organization == {organization}============== {super().get_queryset()}")
        return super().get_queryset().filter(organization=organization)
