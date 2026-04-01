from .base_decorator import BasePermissionDecorator


class OSSPermissionDecorator(BasePermissionDecorator):
    """Allows all users for OSS version."""

    def has_permission(self, request, view_func):
        return True  # No role-based checks for OSS
