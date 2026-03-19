from rbac.oss_decorator import OSSPermissionDecorator


class EnvironmentAwarePermission:
    """Handles both cloud and OSS permission logic"""

    def __init__(self):
        try:
            from pluggable_apps.user_access_control.cloud_decorator import CloudPermissionDecorator

            self.permission_handler = CloudPermissionDecorator()
        except ImportError:
            self.permission_handler = OSSPermissionDecorator()
