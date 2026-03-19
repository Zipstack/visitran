import logging
import os
from importlib import import_module
from typing import Any

from django.apps import apps

Logger = logging.getLogger(__name__)

PLUGINS_APP = "plugins"
AUTH_PLUGIN_DIR = "authentication"
AUTH_MODULE_PREFIX = "scalekit"
AUTH_MODULE = "module"
AUTH_METADATA = "metadata"
METADATA_SERVICE_CLASS = "service_class"
METADATA_IS_ACTIVE = "is_active"


def _load_plugins() -> dict[str, dict[str, Any]]:
    """Iterate through authentication plugins and register their metadata."""
    auth_modules = {}
    try:
        auth_app = apps.get_app_config(PLUGINS_APP)
    except LookupError:
        Logger.info("No plugins app found. Running in OSS mode.")
        return auth_modules

    auth_package_path = auth_app.module.__package__
    auth_dir = os.path.join(auth_app.path, AUTH_PLUGIN_DIR)
    auth_package_path = f"{auth_package_path}.{AUTH_PLUGIN_DIR}"

    if not os.path.isdir(auth_dir):
        Logger.info("No authentication plugin directory found.")
        return auth_modules

    for item in os.listdir(auth_dir):
        if not item.startswith(AUTH_MODULE_PREFIX):
            continue
        if os.path.isdir(os.path.join(auth_dir, item)):
            auth_module_name = item
        elif item.endswith(".so"):
            auth_module_name = item.split(".")[0]
        else:
            continue
        try:
            full_module_path = f"{auth_package_path}.{auth_module_name}"
            module = import_module(full_module_path)
            metadata = getattr(module, AUTH_METADATA, {})
            if metadata.get(METADATA_IS_ACTIVE, False):
                auth_modules[auth_module_name] = {
                    AUTH_MODULE: module,
                    AUTH_METADATA: module.metadata,
                }
                Logger.info(
                    "Loaded auth plugin: %s, is_active: %s",
                    module.metadata["name"],
                    module.metadata["is_active"],
                )
        except ModuleNotFoundError as exception:
            Logger.error(
                "Error while importing authentication module: %s", exception
            )

    if len(auth_modules) > 1:
        raise ValueError(
            "Multiple authentication modules found. "
            "Only one authentication method is allowed."
        )
    elif len(auth_modules) == 0:
        Logger.info(
            "No authentication modules found. "
            "Application will start with default OSS authentication."
        )
    return auth_modules


class AuthenticationPluginRegistry:
    auth_modules: dict[str, dict[str, Any]] = _load_plugins()
    _service_instance: Any | None = None

    @classmethod
    def is_plugin_available(cls) -> bool:
        return len(cls.auth_modules) > 0

    @classmethod
    def get_plugin(cls) -> Any:
        if cls._service_instance is None:
            chosen_auth_module = next(iter(cls.auth_modules.values()))
            chosen_metadata = chosen_auth_module[AUTH_METADATA]
            service_class_name = chosen_metadata[METADATA_SERVICE_CLASS]
            cls._service_instance = service_class_name()
        return cls._service_instance
