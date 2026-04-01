from backend.application.file_explorer.file_system_handler import FileSystemHandler


class LocalFileSystemHandler(FileSystemHandler):
    def __init__(self):
        super().__init__("file")
