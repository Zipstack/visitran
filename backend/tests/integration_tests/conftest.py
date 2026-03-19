from __future__ import annotations

import os
import shutil
import subprocess
import time
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING

import django
import pytest
import requests
import yaml
from django.core.management import call_command

from backend.utils.constants import RouterConstants
from tests.integration_tests.helper import PORT, TEST_PROJECT_NAME, RequestFactory

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.tmpdir import TempPathFactory


request_factory = RequestFactory()


@pytest.fixture(scope="session")
def create_project_profile_paths(
    tmp_path_factory: TempPathFactory,
) -> Generator[dict[str, str], None, None]:
    """Fixture will create temporary profile, project directories during setup
    phase and deletes these folders in teardown phase."""
    project_name = TEST_PROJECT_NAME
    profile_tmp_path: Path = tmp_path_factory.mktemp("profilepath_" + project_name)
    project_tmp_path: Path = tmp_path_factory.mktemp("projectpath")
    yield {
        "project_path": str(project_tmp_path),
        "profile_path": str(profile_tmp_path),
        "project_name": project_name,
    }
    shutil.rmtree(profile_tmp_path)
    shutil.rmtree(project_tmp_path)


@pytest.fixture(scope="session")
def init_duckdb_project(create_project_profile_paths: dict[str, str]) -> dict[str, str]:
    """Fixture will call visitran init using `subprocess` module and
    initializes a duckdb project named `duckdbuitest` in path created in above
    step."""
    profile_path = create_project_profile_paths["profile_path"]
    project_path = create_project_profile_paths["project_path"]
    project_name = create_project_profile_paths["project_name"]
    process = subprocess.Popen(
        [
            "visitran",
            "init",
            project_name,
            "--project-path",
            project_path,
            "--profile-path",
            profile_path,
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    process.stdin.write(b"2\n")
    process.stdin.write(b"\n")
    process.stdin.flush()
    output, _ = process.communicate()
    assert "Which database" in output.decode("utf-8")
    with open(profile_path + "/visitran_profile.yaml", encoding="utf-8") as f:
        profile = f.read()
        assert "duckdb" in profile, "check if profile file is created"

    with open(project_path + f"/{project_name}/visitran_project.yaml", encoding="utf-8") as f:
        project = f.read()
        assert project_name in project, "check if project yaml file is created"
    return {
        "project_path": project_path,
        "profile_path": profile_path,
        "project_name": project_name,
    }


@pytest.fixture(scope="session")
def generate_employee_csv_file(init_duckdb_project: dict[str, str]) -> None:
    """Fixture will add some dummy csv files on which we will be later running
    our transfomation tests, we can add more csv files if requred."""
    project_path = init_duckdb_project["project_path"]
    project_name = init_duckdb_project["project_name"]

    csv_file1 = Path(project_path + f"/{project_name}/" + "seeds/employee.csv")
    csv_file1.write_text(
        """id,name,age,profession,salary
1,John,25,Engineer,50000
2,Jane,30,Manager,80000
3,Bob,40,Developer,65000
4,Alice,35,,100000
5,Charlie,45,Architect,90000
6,David,50,Engineer,70000
"""
    )
    csv_file2 = Path(project_path + f"/{project_name}/" + "seeds/employee_home.csv")
    csv_file2.write_text(
        """id,town
1,New York
2,Los Angeles
3,Chicago
4,Houston
5,Phoenix
"""
    )


def run_server_worker() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "server.settings.dev")
    django.setup(set_prefix=True)
    call_command("runserver", PORT, "--noreload")

    # subprocess.run(["visitran", "ui",
    # "--project-path",f"{project_path}/{project_name}", "--profile-path",profile_path])


def create_or_update_cache_file(profile_path: str, project_path: str, project_name: str) -> None:
    current_path = Path.cwd()
    leaf_dir = current_path.name
    if "visitran" == leaf_dir:
        cache_file_path_yaml = str(current_path) + "/cloud/application/cache.yaml"
    elif "cloud" == leaf_dir:
        cache_file_path_yaml = str(current_path) + "/application/cache.yaml"
    else:
        cache_file_path_yaml = str(current_path) + "/visitran/cloud/application/cache.yaml"
    local_cache = {
        RouterConstants.PROJECTS: {
            project_name: {
                "project_path": project_path + "/" + project_name,
                "profile_path": profile_path,
            }
        }
    }
    assert "/visitran/cloud/application/cache.yaml" in cache_file_path_yaml

    with open(cache_file_path_yaml, "w+", encoding="utf-8") as cache_file:
        yaml.dump(local_cache, cache_file, default_flow_style=False, sort_keys=False)


@pytest.fixture(scope="session")
def start_backend_server(init_duckdb_project: dict[str, str]) -> None:
    """Fixture function will create a `cache.yaml` which is later used by cloud
    to identify the working project and its paths, then it will start cloud
    server.

    During tear down it kills the server process running in seperate
    process.
    """
    profile_path = init_duckdb_project["profile_path"]
    project_path = init_duckdb_project["project_path"]
    project_name = init_duckdb_project["project_name"]

    create_or_update_cache_file(profile_path, project_path, project_name)


@pytest.fixture(scope="session")
def wait_for_health(start_backend_server: dict[str, str]) -> None:
    """Fixture hits the health live endpoint again and again till it returns
    status code 200."""
    # make request to http://localhost:8000/api/v1/health and wait for 200
    # if 200 then return
    # if not 200 then sleep 1 and try again
    # if 10 tries then fail
    count = 0
    while count < 10:
        try:
            response = request_factory("health", True)
            if response.status_code == 200:
                return
        except requests.ConnectionError:
            pass
        count = count + 1
        time.sleep(1)
    raise RuntimeError("Could not connect to cloud server")


@pytest.fixture(scope="session")
def wait_till_ready(
    init_duckdb_project: dict[str, str],
    generate_employee_csv_file: None,
    start_backend_server: None,
    wait_for_health: None,
) -> None:
    """Fixture that waits until the cloud server is ready to serve requests."""
    pass
