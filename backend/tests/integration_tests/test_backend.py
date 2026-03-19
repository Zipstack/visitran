from __future__ import annotations

import json
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from django.core.files.uploadedfile import SimpleUploadedFile

from tests.integration_tests.helper import CONTENT_JSON, RequestFactory

if TYPE_CHECKING:  # pragma: no cover
    from pytest_snapshot.plugin import Snapshot

request_factory = RequestFactory()

CSV_TO_UPLOAD = "uploaded_csv.csv"


@pytest.mark.ui
@pytest.mark.xdist_group(name="cloud")
class TestVisitranBackend:
    """All non explorer api tests are written here."""

    @pytest.mark.parametrize("supported", ["datasources", "aggregations", "formulas"])
    def test_get_apis_describing_support(self, supported: str, snapshot: Snapshot, wait_till_ready: None) -> None:
        """Calls above api endpoints and compares the response with snapshot
        files."""
        response = request_factory(supported)
        # newline is added below to accomodate changes added
        # by pre-commit hook end-of-file-fixer
        json_data = json.dumps(response.json(), indent=2) + "\n"
        snapshot.assert_match(json_data, f"get_supported_{supported}.json")

    @pytest.mark.parametrize("datasource", ["duckdb", "postgres", "trino", "bigquery"])
    def test_get_datasource_fields(self, datasource: str, snapshot: Snapshot, wait_till_ready: None) -> None:
        """Tests all the datasource fields endpoints and compares the response
        with snapshot files."""
        response = request_factory(f"datasource/{datasource}/fields")
        # newline is added below to accomodate changes added
        # by pre-commit hook end-of-file-fixer
        json_data = json.dumps(response.json(), indent=2) + "\n"
        snapshot.assert_match(json_data, f"get_datasource_{datasource}_fields.json")

    def test_get_duckdb_datasource_fields(self, snapshot: Snapshot, wait_till_ready: None) -> None:
        """Tests all the datasource fields endpoints and compares the response
        with snapshot files."""
        response = request_factory("datasource/duckdb/fields")
        json_parsed = response.json()
        json_data = json.dumps(json_parsed, indent=2) + "\n"
        snapshot.assert_match(json_data, "get_datasource_duckdb_fields_alone.json")
        assert len(json_parsed["datasource_fields"]) > 0

    @pytest.mark.parametrize(
        "project_data",
        ["schemas", "explorer", "project_connection", "reload"],
    )
    def test_get_project_data(
        self,
        project_data: str,
        wait_till_ready: None,
        init_duckdb_project: dict[str, str],
        snapshot: Snapshot,
    ) -> None:
        """Tests all the project endpoints and compares the response with
        snapshot files."""
        project_name = init_duckdb_project["project_name"]
        project_path = init_duckdb_project["project_path"]
        response = request_factory(f"project/{project_name}/{project_data}")
        # newline is added below to accomodate changes added
        # by pre-commit hook end-of-file-fixer
        json_parsed = response.json()

        if project_data == "project_connection":
            assert (
                json_parsed["connection_details"][0]["value"] == project_path + "/" + project_name + "/models/local.db"
            )
            # since path varies for each run, removing that from snapshot check
            json_parsed["connection_details"][0]["value"] = ""

        json_data = json.dumps(json_parsed, indent=2) + "\n"
        snapshot.assert_match(json_data, f"get_project_{project_name}_{project_data}.json")

    def test_get_project_connection(
        self,
        wait_till_ready: None,
        init_duckdb_project: dict[str, str],
        snapshot: Snapshot,
    ) -> None:
        """Tests all the project endpoints and compares the response with
        snapshot files."""
        project_name = init_duckdb_project["project_name"]
        project_path = init_duckdb_project["project_path"]
        response = request_factory(f"project/{project_name}/project_connection")
        # newline is added below to accomodate changes added
        # by pre-commit hook end-of-file-fixer
        json_parsed = response.json()
        assert json_parsed["connection_details"][0]["value"] == project_path + "/" + project_name + "/models/local.db"
        # since path varies for each run, removing that from snapshot check
        json_parsed["connection_details"][0]["value"] = ""
        assert json_parsed["project_name"] == project_name
        json_data = json.dumps(json_parsed, indent=2) + "\n"

        snapshot.assert_match(json_data, f"get_project_{project_name}_connection_alone.json")
        assert len(json_parsed["connection_details"]) > 0, "project_connection details should not be empty"

    def test_get_project_reload(
        self,
        wait_till_ready: None,
        init_duckdb_project: dict[str, str],
        snapshot: Snapshot,
    ) -> None:
        """Tests all the project endpoints and compares the response with
        snapshot files."""
        project_name = init_duckdb_project["project_name"]
        response = request_factory(f"project/{project_name}/reload")
        # newline is added below to accomodate changes added
        # by pre-commit hook end-of-file-fixer
        json_parsed = response.json()
        json_data = json.dumps(response.json(), indent=2) + "\n"

        snapshot.assert_match(json_data, f"get_project_{project_name}_reload_alone.json")
        assert json_parsed["project_name"] == project_name

    def test_update_datasource_connection(self, wait_till_ready: None, init_duckdb_project: dict[str, str]) -> None:
        """Tests datasource updation endpoint."""
        project_path = init_duckdb_project["project_path"]
        project_name = init_duckdb_project["project_name"]

        # Once "project/{project_name}/project_connection" gets
        # fixed use that to construct below data
        db_details = {
            "connection_details": {"file_path": f"{project_path}/{project_name}/models/local.db"},
        }

        response = request_factory.put(
            f"project/{project_name}/project_connection/duckdb/update",
            payload=json.dumps(db_details),
            headers=CONTENT_JSON,
        )
        assert project_name in response.json()["connection_details"]["file_path"]

    # Run seed
    def test_execute_seed(
        self,
        wait_till_ready: None,
        init_duckdb_project: dict[str, str],
        snapshot: Snapshot,
    ) -> None:
        """Tests seed execution endpoint."""
        project_name = init_duckdb_project["project_name"]
        payload = json.dumps({"project_name": project_name})
        response = request_factory.post(f"project/{project_name}/execute/seed", payload)
        json_data = json.dumps(response.json(), indent=2) + "\n"
        snapshot.assert_match(json_data, f"{project_name}_execute_seed.json")
        assert response.json()["status"] == "success"

    def test_get_project_datasource_test(
        self,
        wait_till_ready: None,
        init_duckdb_project: dict[str, str],
        snapshot: Snapshot,
    ) -> None:
        """Tests datasource test endpoint."""
        project_name = init_duckdb_project["project_name"]
        response = request_factory(f"project/{project_name}/project_connection/duckdb/test")
        json_parsed = response.json()
        json_data = json.dumps(json_parsed, indent=2) + "\n"
        snapshot.assert_match(json_data, f"{project_name}_test_duckdb_datasource.json")

    def test_get_project_tables(
        self,
        wait_till_ready: None,
        init_duckdb_project: dict[str, str],
        snapshot: Snapshot,
    ) -> None:
        """Tests project tables endpoint."""
        project_name = init_duckdb_project["project_name"]
        payload = json.dumps({"project_name": project_name})
        request_factory.post(f"project/{project_name}/execute/seed", payload)
        response = request_factory(f"project/{project_name}/schemas/tables")
        json_parsed = response.json()
        json_data = json.dumps(json_parsed, indent=2) + "\n"
        snapshot.assert_match(json_data, f"get_{project_name}_project_tables.json")

    def test_get_project_tables_from_schema(
        self,
        wait_till_ready: None,
        init_duckdb_project: dict[str, str],
        snapshot: Snapshot,
    ) -> None:
        """Tests project tables endpoint."""
        project_name = init_duckdb_project["project_name"]
        payload = json.dumps({"project_name": project_name})
        request_factory.post(f"project/{project_name}/execute/seed", payload)
        response = request_factory(f"project/{project_name}/schema/default/tables")
        json_parsed = response.json()
        json_data = json.dumps(json_parsed, indent=2) + "\n"
        snapshot.assert_match(json_data, f"get_{project_name}_project_tables_from_schema.json")

    def test_get_table_columns_from_schema(
        self,
        wait_till_ready: None,
        init_duckdb_project: dict[str, str],
        snapshot: Snapshot,
    ) -> None:
        """Tests get project table column endpoint."""
        project_name = init_duckdb_project["project_name"]
        payload = json.dumps({"project_name": project_name})
        request_factory.post(f"project/{project_name}/execute/seed", payload)
        response = request_factory(f"project/{project_name}/schema/default/table/employee/columns")
        json_parsed = response.json()
        json_data = json.dumps(json_parsed, indent=2) + "\n"
        snapshot.assert_match(json_data, f"get_{project_name}_project_table_columns_from_schema.json")

    def test_get_table_contents_from_schema(
        self,
        wait_till_ready: None,
        init_duckdb_project: dict[str, str],
        snapshot: Snapshot,
    ) -> None:
        """Tests get project table contents endpoint."""
        project_name = init_duckdb_project["project_name"]
        payload = json.dumps({"project_name": project_name})
        request_factory.post(f"project/{project_name}/execute/seed", payload)
        response = request_factory(f"project/{project_name}/schema/default/table/employee/content")
        json_parsed = response.json()
        json_data = json.dumps(json_parsed, indent=2) + "\n"
        snapshot.assert_match(json_data, f"get_{project_name}_project_table_contents_from_schema.json")


@pytest.fixture(scope="function")
def add_csv_to_test_upload(
    tmp_path: Path,
    wait_till_ready: None,
) -> Generator[Path, None, None]:
    """CSV file used for upload api test."""
    upload_csv = tmp_path / "uploaded_csv.csv"
    upload_csv.write_text(
        """id,name,age
        1,John,23.2
        2,Smith,24.5
        3,Joe,25.8
        4,Paul,26.9
"""
    )
    yield upload_csv
    upload_csv.unlink()


@pytest.mark.ui
@pytest.mark.xdist_group(name="cloud")
class TestVisitranUIExplorer:
    """All explorer file operation api tests are written here."""

    def test_create_rename_delete_folder(
        self,
        wait_till_ready: None,
        init_duckdb_project: dict[str, str],
    ) -> None:
        project_name = init_duckdb_project["project_name"]
        project_path = init_duckdb_project["project_path"]
        # Create folder
        body = {"folder_name": "folder_check", "parent_path": "seeds"}
        payload = json.dumps(body)
        response = request_factory.post(
            f"project/{project_name}/explorer/folder/create",
            payload=payload,
        )
        json_parsed = response.json()
        assert json_parsed["status"] == "success", "folder creation failed"
        folder_check = Path(project_path) / project_name / "seeds" / "folder_check"
        assert folder_check.is_dir()

        # rename folder
        body = {"file_name": "seeds/folder_check", "rename": "seeds/folder_renamed"}
        payload = json.dumps(body)
        response = request_factory.post(
            f"project/{project_name}/explorer/file/rename",
            payload=payload,
        )
        json_parsed = response.json()
        assert json_parsed["status"] == "success", "folder rename failed"
        renamed_folder = Path(project_path) / project_name / "seeds" / "folder_renamed"
        assert renamed_folder.is_dir()
        assert not folder_check.exists()

        # delete folder
        body = {"file_name": "seeds/folder_renamed"}
        payload = json.dumps(body)
        response = request_factory.delete(
            f"project/{project_name}/explorer/file/delete",
            payload=payload,
            headers=CONTENT_JSON,
        )
        json_parsed = response.json()
        assert json_parsed["status"] == "success", "folder delete failed"
        assert not renamed_folder.exists()
        assert not folder_check.exists()

    def test_create_rename_delete_model(
        self,
        wait_till_ready: None,
        init_duckdb_project: dict[str, str],
    ) -> None:
        project_name = init_duckdb_project["project_name"]
        project_path = init_duckdb_project["project_path"]
        # Create model
        body = {"model_name": "model_file", "parent_path": "models/no_code"}
        payload = json.dumps(body)
        response = request_factory.post(
            f"project/{project_name}/explorer/model/create",
            payload=payload,
        )
        json_parsed = response.json()
        assert json_parsed["status"] == "success", "model creation failed"
        file_check = Path(project_path) / project_name / "models" / "no_code" / "model_file"
        assert file_check.is_file()

        # rename model
        body = {
            "file_name": "models/no_code/model_file",
            "rename": "models/no_code/renamed_model_file",
        }
        payload = json.dumps(body)
        response = request_factory.post(
            f"project/{project_name}/explorer/file/rename",
            payload=payload,
        )
        json_parsed = response.json()
        assert json_parsed["status"] == "success", "model rename failed"
        renamed_file = Path(project_path) / project_name / "models" / "no_code" / "renamed_model_file"
        assert renamed_file.is_file()
        assert not file_check.exists()

        # delete model
        body = {"file_name": "models/no_code/renamed_model_file"}
        payload = json.dumps(body)
        response = request_factory.delete(
            f"project/{project_name}/explorer/file/delete",
            payload=payload,
            headers=CONTENT_JSON,
        )
        json_parsed = response.json()
        assert json_parsed["status"] == "success", "model delete failed"
        assert not renamed_file.exists()
        assert not file_check.exists()

    def test_seed_file_upload(
        self,
        init_duckdb_project: dict[str, str],
        add_csv_to_test_upload: Generator[Path, None, None],
    ) -> None:
        project_name = init_duckdb_project["project_name"]
        project_path = init_duckdb_project["project_path"]
        FILE_UPLOAD_FAILED = "file upload failed"

        with open(str(add_csv_to_test_upload), "rb") as fileobj:

            csv_file = SimpleUploadedFile(CSV_TO_UPLOAD, fileobj.read(), content_type="text/csv")

            data = {"file": csv_file, "file_name": f"seeds/{CSV_TO_UPLOAD}"}

            response = request_factory.post(
                f"project/{project_name}/explorer/upload",
                payload=data,
                content_type="multipart",
            )
        json_parsed = response.json()
        assert json_parsed["status"] == "success", FILE_UPLOAD_FAILED
        file_check = Path(project_path) / project_name / "seeds" / CSV_TO_UPLOAD
        assert file_check.is_file()

        with open(str(add_csv_to_test_upload), "rb") as fileobj:

            csv_file = SimpleUploadedFile(CSV_TO_UPLOAD, fileobj.read(), content_type="text/csv")

            data = {"file": csv_file, "file_name": f"seeds/{CSV_TO_UPLOAD}"}

            response = request_factory.post(
                f"project/{project_name}/explorer/upload", payload=data, content_type="multipart", return_response=True
            )
            json_parsed = response.json()
            assert json_parsed["status"] == "failed", FILE_UPLOAD_FAILED
            assert (
                json_parsed["error_message"] == "The CSV file you are trying to add already exists. "
                "Please try again with a different path or choose a unique "
                "file name"
            ), FILE_UPLOAD_FAILED

    def test_float64_ui_representation(
        self,
        init_duckdb_project: dict[str, str],
        wait_till_ready: None,
    ) -> None:
        project_name = init_duckdb_project["project_name"]
        project_path = init_duckdb_project["project_path"]

        upload_csv = Path(project_path) / "duckdbuitest" / "seeds" / "testfloat.csv"

        upload_csv.write_text(
            """id,name,age
1,John,23.2
2,Smith,24.5
3,Joe,25.8
4,Paul,26.9
"""
        )

        # first execute seed again to add the new table
        payload = json.dumps({"project_name": project_name})
        response = request_factory.post(f"project/{project_name}/execute/seed", payload)
        assert response.json()["status"] == "success"

        # get new table contents
        payload = json.dumps({"project_name": project_name})
        request_factory.post(f"project/{project_name}/execute/seed", payload)
        response = request_factory(f"project/{project_name}/schema/default/table/testfloat/content")
        json_parsed = response.json()
        assert (
            json_parsed["column_description"]["age"]["data_type"] != "String"
        ), "float64 is represented as string in UI"
