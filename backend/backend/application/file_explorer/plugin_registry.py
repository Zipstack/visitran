import logging
import os
from importlib import import_module
from typing import Any

from django.apps import apps

from backend.application.file_explorer.constants import PluginConfig

Logger = logging.getLogger(__name__)


def _load_plugins() -> dict[str, dict[str, Any]]:
    """Iterating through the storage plugins and register their
    metadata."""
    storage_app = apps.get_app_config(PluginConfig.PLUGINS_APP)
    storage_package_path = storage_app.module.__package__
    storage_dir = os.path.join(storage_app.path, PluginConfig.STORAGE_PLUGIN_DIR)
    storage_package_path = f"{storage_package_path}.{PluginConfig.STORAGE_PLUGIN_DIR}"
    storage_modules = {}

    for item in os.listdir(storage_dir):
        # Loads a plugin only if name starts with `storage`.
        if not item.startswith(PluginConfig.STORAGE_MODULE_PREFIX):
            continue
        # Loads a plugin if it is in a directory.
        if os.path.isdir(os.path.join(storage_dir, item)):
            storage_module_name = item
        # Loads a plugin if it is a shared library.
        # Module name is extracted from shared library name.
        # `storage.platform_architecture.so` will be file name and
        # `storage` will be the module name.
        elif item.endswith(".so"):
            storage_module_name = item.split(".")[0]
        else:
            continue
        try:
            full_module_path = f"{storage_package_path}.{storage_module_name}"
            module = import_module(full_module_path)
            metadata = getattr(module, PluginConfig.STORAGE_METADATA, {})
            if metadata.get(PluginConfig.METADATA_IS_ACTIVE, False):
                storage_modules[storage_module_name] = {
                    PluginConfig.STORAGE_MODULE: module,
                    PluginConfig.STORAGE_METADATA: module.metadata,
                }
                Logger.info(
                    "Loaded storage plugin: %s, is_active: %s",
                    module.metadata["name"],
                    module.metadata["is_active"],
                )
            else:
                Logger.warning(
                    "Metadata is not active for %s storage module.",
                    storage_module_name,
                )
        except ModuleNotFoundError as exception:
            Logger.error(
                "Error while importing storage module : %s",
                exception,
            )

    if len(storage_modules) > 1:
        raise ValueError(
            "Multiple storage modules found."
            "Only one storage method is allowed."
        )
    elif len(storage_modules) == 0:
        Logger.warning(
            "No storage modules found."
            "Application will start without storage module"
        )
    return storage_modules


class PluginRegistry:
    storage_modules: dict[str, dict[str, Any]] = _load_plugins()

    @classmethod
    def is_plugin_available(cls) -> bool:
        """Check if any storage plugin is available.

        Returns:
            bool: True if a plugin is available, False otherwise.
        """
        return len(cls.storage_modules) > 0

    @classmethod
    def get_plugin(cls) -> Any:
        """Get the selected storage plugin.

        Returns:
            StorageService: Selected storage plugin instance.
        """
        chosen_storage_module = next(iter(cls.storage_modules.values()))
        chosen_metadata = chosen_storage_module[PluginConfig.STORAGE_METADATA]
        service_class_name = chosen_metadata[
            PluginConfig.METADATA_SERVICE_CLASS
        ]
        return service_class_name()
