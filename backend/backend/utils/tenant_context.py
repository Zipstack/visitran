import logging
from contextvars import ContextVar
from typing import Optional

from django.db.utils import ProgrammingError

from backend.core.models.organization_model import Organization

_tenant_context_var: ContextVar["TenantContext"] = ContextVar("tenant_context", default=None)


class TenantContext:
    def __init__(self):
        self.user = None
        self.tenant = None
        self.env = None

    def set_user(self, user: any):
        resolved_user = user._wrapped if hasattr(user, "_wrapped") else user
        # Convert to dictionary
        if hasattr(resolved_user, "__dict__"):
            user_dict = resolved_user.__dict__
        else:
            user_dict = user
        self.user = user_dict

    def set_tenant(self, tenant, source=None):
        if self.tenant is None:
            self.tenant = tenant
            self.env = source

    def clear(self):
        """Clear the context to avoid leakage."""
        self.user = None
        self.tenant = None
        self.env = None


def _get_tenant_context() -> TenantContext:
    ctx = _tenant_context_var.get()
    if ctx is None:
        ctx = TenantContext()
        _tenant_context_var.set(ctx)
    return ctx


def get_current_user():
    user = _get_tenant_context().user or {}
    created_by = {"name": user.get("email", ""), "username": user.get("username", "")}
    logging.info(f"created_by: {created_by}")
    return created_by


def get_current_tenant() -> str:
    """This fn returns the current tenant ID."""
    # TODO - Need to implement session with proper org in Cloud
    return _get_tenant_context().tenant or "default_org"


def get_current_env() -> str:
    return _get_tenant_context().env


def get_system_user() -> dict[str, str]:
    created_by = {"name": "Visitran", "username": "Visitran"}
    return created_by


def clear_tenant_context():
    _tenant_context_var.set(None)


def get_organization() -> Optional[Organization]:
    organization_id = get_current_tenant()
    try:
        # logging.info(f"fetching organization id from tenant context. organization_id: {organization_id}")
        organization: Organization = Organization.objects.get(organization_id=organization_id)
    except Organization.DoesNotExist:
        # logging.error(f"failed to fetch organization id from tenant context. organization_id: {organization_id}")
        return None
    except ProgrammingError:
        # Handle cases where the database schema might not be fully set up,
        # especially during the execution of management commands
        # other than runserver
        # logging.error(f"ProgrammingError: failed to fetch organization id from tenant context. organization_id: {organization_id}")
        return None
    return organization
