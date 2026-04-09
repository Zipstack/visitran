import hashlib

from django.db import models
from django.utils.timezone import now

from backend.core.models.user_model import User
from utils.models.base_model import BaseModel
from utils.models.organization_mixin import DefaultOrganizationMixin, DefaultOrganizationManagerMixin


class APITokenManager(DefaultOrganizationManagerMixin, models.Manager):
    pass


class APIToken(DefaultOrganizationMixin, BaseModel):
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="api_tokens")
    token = models.CharField(max_length=64, unique=True, editable=False)
    token_hash = models.CharField(max_length=64, blank=True, default="")
    signature = models.CharField(max_length=128, blank=True, default="")
    label = models.CharField(max_length=100, blank=True, default="")
    is_disabled = models.BooleanField(default=False)
    expires_at = models.DateTimeField(null=True, blank=True)
    last_used_at = models.DateTimeField(null=True, blank=True)

    def save(self, *args, **kwargs):
        if self.token:
            self.token_hash = hashlib.sha256(self.token.encode()).hexdigest()
        super().save(*args, **kwargs)

    class Meta:
        pass  # Removed unique_user_org_token constraint — users can have multiple API keys

    # Manager
    objects = APITokenManager()
    raw_objects = models.Manager()

    def is_valid(self):
        if self.is_disabled:
            return False
        return self.expires_at is None or self.expires_at > now()

    @property
    def status(self):
        if self.is_disabled:
            return "disabled"
        if self.expires_at and self.expires_at <= now():
            return "expired"
        return "active"

    @property
    def masked_token(self):
        if len(self.token) > 12:
            return self.token[:12] + "..." + self.token[-4:]
        return "****"

    @property
    def masked_signature(self):
        if len(self.signature) > 8:
            return self.signature[:8] + "..." + self.signature[-4:]
        return "****"

    def __str__(self):
        return f"Token for {self.user.username} ({self.label or 'unlabeled'})"
