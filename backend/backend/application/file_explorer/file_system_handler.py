import logging
from typing import List

import fsspec

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FileSystemHandler:
    def __init__(self, filesystem: str, file_path_prefix: str = ""):
        self.file_path_prefix = file_path_prefix
        try:
            self.fs = fsspec.filesystem(filesystem)
            logger.info(f"Initialized filesystem with type: {filesystem}")
        except Exception as e:
            logger.error(f"Failed to initialize filesystem: {e}")
            raise

    def read_file(self, path: str) -> str:
        try:
            path = f"{self.file_path_prefix}{path}"
            logger.info(f"Reading file: {path}")
            with self.fs.open(path, "r") as f:
                content = f.read()
            return content
        except FileNotFoundError:
            logger.error(f"File not found: {path}")
            raise
        except Exception as e:
            logger.error(f"Failed to read file: {e}")
            raise

    def write_file(self, path: str, data: str) -> None:
        try:
            path = f"{self.file_path_prefix}{path}"
            logger.info(f"Writing to file: {path}")
            with self.fs.open(path, "w") as f:
                f.write(data)
            logger.info(f"Successfully wrote to {path}")
        except Exception as e:
            logger.error(f"Failed to write to file: {e}")
            raise

    def list_files(self, path: str) -> list[str]:
        try:
            path = f"{self.file_path_prefix}{path}"
            logger.info(f"Listing files in directory: {path}")
            files = self.fs.ls(path)
            logger.info(f"Found files: {files}")
            return files
        except FileNotFoundError:
            logger.error(f"Directory not found: {path}")
            raise
        except Exception as e:
            logger.error(f"Failed to list files: {e}")
            raise

    def file_exists(self, path: str) -> bool:
        try:
            path = f"{self.file_path_prefix}{path}"
            exists = self.fs.exists(path)
            logger.info(f"File exists ({path}): {exists}")
            return exists
        except Exception as e:
            logger.error(f"Failed to check if file exists: {e}")
            raise

    def delete_file(self, path: str) -> None:
        try:
            path = f"{self.file_path_prefix}{path}"
            if self.fs.exists(path):
                logger.info(f"Deleting file: {path}")
                self.fs.rm(path)
                logger.info(f"Successfully deleted {path}")
            else:
                logger.warning(f"File not found, cannot delete: {path}")
        except Exception as e:
            logger.error(f"Failed to delete file: {e}")
            raise
