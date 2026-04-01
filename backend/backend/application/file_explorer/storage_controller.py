from backend.application.file_explorer.file_system_handler import FileSystemHandler
from backend.application.file_explorer.plugin_registry import PluginRegistry


class FileStorageController:

    def __init__(self) -> None:
        if PluginRegistry.is_plugin_available():
            self.storage_handler: FileSystemHandler = PluginRegistry.get_plugin()
        else:
            self.storage_handler = FileSystemHandler()

    def save_file(self, file_content: str, file_name: str) -> None:
        self.storage_handler.write_file(file_name, file_content)

    def read_file(self, file_path) -> None:
        self.storage_handler.read_file(file_path)

    def list_files(self, file_path) -> None:
        self.storage_handler.list_files(file_path)

    def file_exists(self, file_path) -> None:
        self.storage_handler.file_exists(file_path)

    def delete_file(self, file_path) -> None:
        self.storage_handler.delete_file(file_path)
