import os
from pathlib import Path

import pytest

from backend.application.file_explorer.file_explorer import FileExplorer


@pytest.mark.minimal_core
class TestFileExplorer:

    def test_project_path(self):
        test_path = Path(os.getcwd()).parent.parent.parent / "integration" / "data"
        file_explorer = FileExplorer(project_path=str(test_path))
        assert str(test_path) == file_explorer.project_path

    def test_model_path(self):
        test_path = Path(os.getcwd()).parent.parent.parent / "integration" / "data"
        file_explorer = FileExplorer(project_path=str(test_path))
        assert str(test_path / "models" / "full_code") + "/" == file_explorer.model_path_prefix
