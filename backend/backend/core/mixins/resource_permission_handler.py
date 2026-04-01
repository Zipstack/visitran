from backend.errors import ResourcePermissionDeniedException
from rbac.EnvironmentAwarePermission import EnvironmentAwarePermission


class UserAccessControlMixin:
    """Handles resource access permissions."""

    RESOURCE_NAME = None  # Override in child classes

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uac = EnvironmentAwarePermission()

    def check_permissions(self, request):
        super().check_permissions(request)

        if not self.uac.permission_handler.has_permission(request, self.RESOURCE_NAME):
            raise ResourcePermissionDeniedException()
