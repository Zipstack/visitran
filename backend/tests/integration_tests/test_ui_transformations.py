from __future__ import annotations

import gc
import json
import os
import shutil
import time
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from tests.integration_tests.helper import CONTENT_JSON, TEST_PROJECT_NAME, RequestFactory
from visitran.singleton import Singleton

if TYPE_CHECKING:  # pragma: no cover
    from pytest_snapshot.plugin import Snapshot


MultiJoinCSVs = ["film.csv", "inventory.csv", "rental.csv", "store.csv"]
YAML_PATH_PREFIX = "transform_yamls"
model_delete_failed = "model delete failed"
lineage_failure = "get lineage failed"
nocode_model_create_failed = "no_code_model creation failed"
run_model_failed = "run model failed"
model_creation_failed = "model creation failed"
failed_with_error_message = "failed with different error message"
MODELS_NO_CODE = "models/no_code"

request_factory = RequestFactory()
yaml_transformations = [
    {
        "type": "simple_transformation",
        "name": TEST_PROJECT_NAME,
        "file_name": "simple_transformation",
    },
    {
        "type": "add_column",
        "name": TEST_PROJECT_NAME,
        "file_name": "add_column",
    },
    {
        "type": "simple_sort_n_hide",
        "name": TEST_PROJECT_NAME,
        "file_name": "simple_sort_n_hide",
    },
    {
        "type": "simple_aggregation_n_sort",
        "name": TEST_PROJECT_NAME,
        "file_name": "simple_aggregation_n_sort",
    },
    {
        "type": "filter_n_aggregation",
        "name": TEST_PROJECT_NAME,
        "file_name": "filter_n_aggregation",
    },
    {
        "type": "aggregation_column_in_filter",
        "name": TEST_PROJECT_NAME,
        "file_name": "aggregation_column_in_filter",
    },
    {
        "type": "formula_in_aggregation",
        "name": TEST_PROJECT_NAME,
        "file_name": "formula_in_aggregation",
    },
    {
        "type": "inner_join",
        "name": TEST_PROJECT_NAME,
        "file_name": "inner_join",
    },
    {
        "type": "full_join",
        "name": TEST_PROJECT_NAME,
        "file_name": "full_join",
    },
    {
        "type": "left_join",
        "name": TEST_PROJECT_NAME,
        "file_name": "left_join",
    },
    {
        "type": "right_join",
        "name": TEST_PROJECT_NAME,
        "file_name": "right_join",
    },
    {
        "type": "drop_duplicates",
        "name": TEST_PROJECT_NAME,
        "file_name": "drop_duplicates",
    },
    {
        "type": "merge_check",
        "name": TEST_PROJECT_NAME,
        "file_name": "merge_check",
    },
    {
        "type": "sort_on_synthesized_column",
        "name": TEST_PROJECT_NAME,
        "file_name": "sort_on_synthesized_column",
    },
    {
        "type": "three_joins",
        "name": TEST_PROJECT_NAME,
        "file_name": "three_joins",
    },
    {
        "type": "filter_n_groupby",
        "name": TEST_PROJECT_NAME,
        "file_name": "filter_n_groupby",
    },
    {
        "type": "invalid_csv",
        "name": TEST_PROJECT_NAME,
        "file_name": "invalid_csv",
    },
]


@pytest.fixture(scope="class")
def seed_on_startup(
    init_duckdb_project: dict[str, str],
    wait_till_ready: None,
) -> None:
    """Copies sakila csvs to seed folder and deletes the no code models created
    during the test run."""
    project_name = init_duckdb_project["project_name"]

    payload = json.dumps({"project_name": project_name})
    request_factory.post(
        f"project/{project_name}/execute/seed",
        payload=payload,
    )


@pytest.fixture(scope="function")
def cleanup_no_code_models_on_teardown(
    init_duckdb_project: dict[str, str],
    wait_till_ready: None,
) -> Generator[None, None, None]:
    yield
    project_name = init_duckdb_project["project_name"]
    project_path = init_duckdb_project["project_path"]
    to_delete_files = Path(project_path) / project_name / "models" / "no_code"
    for path in to_delete_files.rglob("*"):
        if path.is_file() and path.name != "__init__.py" and path.name != "no_code_model":
            file_to_delete = path.name
            body = {"file_name": f"models/no_code/{file_to_delete}"}
            payload = json.dumps(body)
            response = request_factory.delete(
                f"project/{project_name}/explorer/file/delete",
                payload=payload,
                headers=CONTENT_JSON,
            )
            json_parsed = response.json()
            assert json_parsed["status"] == "success", model_delete_failed
            assert not path.exists()


# need to investigate why this test fails if ran after TestLineage
@pytest.mark.order(Before="TestLineage")
@pytest.mark.ui
@pytest.mark.xdist_group(name="cloud")
class TestTransformations:
    # @pytest.mark.selected
    @pytest.mark.parametrize(
        "tf_payload",
        yaml_transformations,
        ids=[
            "simple_transformation",
            "add_column",
            "simple_sort_n_hide",
            "simple_aggregation_n_sort",
            "filter_n_aggregation",
            "aggregation_column_in_filter",
            "formula_in_aggregation",
            "inner_join",
            "full_join",
            "left_join",
            "right_join",
            "drop_duplicates",
            "merge_check",
            "sort_on_synthesized_column",
            "three_joins",
            "filter_n_groupby",
            "invalid_csv",
        ],
    )
    def test_transformations(
        self,
        seed_on_startup: None,
        cleanup_no_code_models_on_teardown: Generator[None, None, None],
        init_duckdb_project: dict[str, str],
        snapshot: Snapshot,
        tf_payload: dict[str, str],
        shared_datadir: Path,
    ) -> None:
        """Tests all the transformation endpoints and compares the responses
        with snapshot files."""
        Singleton.reset_cache()
        gc.collect()
        project_name = init_duckdb_project["project_name"]
        project_path = init_duckdb_project["project_path"]
        yaml_path = shared_datadir / YAML_PATH_PREFIX / "test_transformations" / f"{tf_payload['file_name']}.yaml"
        tf_payload["file"] = open(f"{yaml_path}").read()
        tf_type = tf_payload["type"]
        del tf_payload["type"]

        # Test to check for invalid csv file
        if tf_type.startswith("invalid_csv"):
            src = shared_datadir / "invalid_csv" / "staff.csv"
            dst = Path(project_path) / project_name / "seeds" / "staff.csv"
            shutil.copy2(src, dst)

            payload = json.dumps({"project_name": project_name})

            response = request_factory.post(
                f"project/{project_name}/execute/seed", payload=payload, return_response=True
            )
            json_parsed = response.json()
            assert "failed" == json_parsed["status"], "Error message is different"
            os.remove(dst)
            return

        # copy sakila csv for 3 join test
        if tf_type.startswith("three_joins"):
            for csv in MultiJoinCSVs:
                src = shared_datadir / "sakila" / csv
                dst = Path(project_path) / project_name / "seeds" / csv
                shutil.copy2(src, dst)

            payload = json.dumps({"project_name": project_name})
            request_factory.post(
                f"project/{project_name}/execute/seed",
                payload=payload,
            )

        path = Path(__file__).parent / "snapshots" / Path(__file__).stem
        snapshot_dir = path / f"test_transform_table_{tf_type}"
        snapshot.snapshot_dir = str(snapshot_dir)

        try:
            # Create model
            body = {"model_name": tf_type, "parent_path": MODELS_NO_CODE}
            payload = json.dumps(body)
            response = request_factory.post(
                f"project/{project_name}/explorer/model/create",
                payload=payload,
            )
            json_parsed = response.json()
            assert json_parsed["status"] == "success", model_creation_failed
            file_check = Path(project_path) / project_name / "models" / "no_code" / tf_type
            assert file_check.is_file()

            # write yaml to no code model
            tf_payload["name"] = project_name
            payload = json.dumps(tf_payload)
            response = request_factory.post(
                f"project/{project_name}/no_code_model/{tf_type}",
                payload=payload,
            )
            json_parsed = response.json()
            assert json_parsed["status"] == "success", nocode_model_create_failed

            payload = json.dumps({"project_name": project_name})
            response = request_factory.post(
                f"project/{project_name}/execute/run", payload=payload, return_response=True
            )
            json_parsed = response.json()

            if tf_type == "formula_in_aggregation":
                assert json_parsed["status"] == "failed", run_model_failed
                assert (
                    "The column name - avg_salary reffered in the formula "
                    "is not exist in the mapped table, "
                    "If you are trying to map a aggregation column here, "
                    "you can make it possible by using child model support" in json_parsed["error_message"]
                ), failed_with_error_message
                return
            else:
                assert json_parsed["status"] == "success", run_model_failed

            response = request_factory(f"project/{project_name}/schema/default/tables")
            json_parsed = response.json()
            assert f"default.{tf_type}" in json_parsed["table_names"]

            response = request_factory(f"project/{project_name}/schema/default/table/{tf_type}/content")
            json_parsed = response.json()
            json_data = json.dumps(json_parsed, indent=2) + "\n"

            snapshot.assert_match(json_data, f"transform_table_{tf_type}.json")

        finally:
            # delete sakila csv for three_join test
            if tf_type.startswith("three_joins"):
                for csv in MultiJoinCSVs:
                    dst = Path(project_path) / project_name / "seeds" / csv
                    dst.unlink()

    def test_conflicting_column_added(
        self,
        init_duckdb_project: dict[str, str],
        shared_datadir: Path,
        seed_on_startup: None,
        cleanup_no_code_models_on_teardown: Generator[None, None, None],
    ) -> None:
        project_name = init_duckdb_project["project_name"]
        project_path = init_duckdb_project["project_path"]

        tf_type = "conflicting_column"

        # Create model
        body = {"model_name": tf_type, "parent_path": MODELS_NO_CODE}
        payload = json.dumps(body)
        response = request_factory.post(
            f"project/{project_name}/explorer/model/create",
            payload=payload,
        )
        json_parsed = response.json()
        assert json_parsed["status"] == "success", model_creation_failed
        file_check = Path(project_path) / project_name / "models" / "no_code" / tf_type
        assert file_check.is_file()

        # write yaml to no code model
        prefix_path = shared_datadir / YAML_PATH_PREFIX / "test_conflicting_column_added"
        yaml_path = prefix_path / "conflicting_column_1.yaml"
        tf_payload = {
            "name": TEST_PROJECT_NAME,
            "file_name": tf_type,
            "file": open(yaml_path).read(),
        }
        payload = json.dumps(tf_payload)
        response = request_factory.post(
            f"project/{project_name}/no_code_model/{tf_type}",
            payload=payload,
        )
        json_parsed = response.json()
        assert json_parsed["status"] == "success", nocode_model_create_failed

        payload = json.dumps({"project_name": project_name})
        response = request_factory.post(
            f"project/{project_name}/execute/run",
            payload=payload,
        )
        json_parsed = response.json()
        assert json_parsed["status"] == "success", run_model_failed

        yaml_path = prefix_path / "conflicting_column_2.yaml"
        tf_payload = {
            "name": TEST_PROJECT_NAME,
            "file_name": tf_type,
            "file": open(yaml_path).read(),
        }
        # time.sleep(3000)
        payload = json.dumps(tf_payload)
        response = request_factory.post(
            f"project/{project_name}/no_code_model/{tf_type}",
            payload=payload,
            return_response=True,
        )
        json_parsed = response.json()
        assert json_parsed["status"] == "failed", "should fail as age column already exists"
        assert "Column already exists" in json_parsed["error_message"], failed_with_error_message

    def test_join_failure_on_second_model(
        self,
        wait_till_ready: None,
        cleanup_no_code_models_on_teardown: Generator[None, None, None],
        init_duckdb_project: dict[str, str],
        shared_datadir: Path,
    ) -> None:
        """Tests all the transformation endpoints and compares the response
        with snapshot files."""
        Singleton.reset_cache()
        gc.collect()
        project_name = init_duckdb_project["project_name"]
        project_path = init_duckdb_project["project_path"]
        for csv in MultiJoinCSVs:
            src = shared_datadir / "sakila" / csv
            dst = Path(project_path) / project_name / "seeds" / csv
            shutil.copy2(src, dst)

        payload = json.dumps({"project_name": project_name})
        request_factory.post(
            f"project/{project_name}/execute/seed",
            payload=payload,
        )

        try:
            # Create model
            tf_type = "model_with_join_1"
            body = {"model_name": tf_type, "parent_path": MODELS_NO_CODE}
            payload = json.dumps(body)
            response = request_factory.post(
                f"project/{project_name}/explorer/model/create",
                payload=payload,
            )
            json_parsed = response.json()
            assert json_parsed["status"] == "success", model_creation_failed
            file_check = Path(project_path) / project_name / "models" / "no_code" / tf_type
            assert file_check.is_file()

            # write yaml to no code model
            yaml_path = shared_datadir / YAML_PATH_PREFIX / "test_join_failure_on_second_model" / f"{tf_type}.yaml"
            tf_payload = {
                "name": TEST_PROJECT_NAME,
                "file_name": tf_type,
                "file": open(yaml_path).read(),
            }
            payload = json.dumps(tf_payload)
            response = request_factory.post(
                f"project/{project_name}/no_code_model/{tf_type}",
                payload=payload,
            )
            json_parsed = response.json()
            assert json_parsed["status"] == "success", nocode_model_create_failed

            payload = json.dumps({"project_name": project_name})
            response = request_factory.post(
                f"project/{project_name}/execute/run",
                payload=payload,
            )
            json_parsed = response.json()
            assert json_parsed["status"] == "success", run_model_failed

            tf_type = "model_with_join_2"

            body = {"model_name": tf_type, "parent_path": MODELS_NO_CODE}
            payload = json.dumps(body)
            response = request_factory.post(
                f"project/{project_name}/explorer/model/create",
                payload=payload,
            )
            json_parsed = response.json()
            assert json_parsed["status"] == "success", model_creation_failed
            file_check = Path(project_path) / project_name / "models" / "no_code" / tf_type
            assert file_check.is_file()

            yaml_path = shared_datadir / YAML_PATH_PREFIX / "test_join_failure_on_second_model" / f"{tf_type}.yaml"
            tf_payload = {
                "name": TEST_PROJECT_NAME,
                "file_name": tf_type,
                "file": open(yaml_path).read(),
            }

            payload = json.dumps(tf_payload)
            response = request_factory.post(
                f"project/{project_name}/no_code_model/{tf_type}",
                payload=payload,
            )
            json_parsed = response.json()
            assert json_parsed["status"] == "success", nocode_model_create_failed

            payload = json.dumps({"project_name": project_name})
            response = request_factory.post(
                f"project/{project_name}/execute/run",
                payload=payload,
            )
            json_parsed = response.json()
            assert json_parsed["status"] == "success", run_model_failed

        finally:
            for csv in MultiJoinCSVs:
                dst = Path(project_path) / project_name / "seeds" / csv
                dst.unlink()

    @pytest.mark.parametrize(
        "tf_payload",
        [
            {
                "type": "add_column_n_sort",
                "name": TEST_PROJECT_NAME,
                "file_name": "add_column_n_sort",
            },
            {
                "type": "aggregate_n_sort",
                "name": TEST_PROJECT_NAME,
                "file_name": "aggregate_n_sort",
            },
            {
                "type": "join_n_sort",
                "name": TEST_PROJECT_NAME,
                "file_name": "join_n_sort",
            },
            {
                "type": "add_column_in_added_column",
                "name": TEST_PROJECT_NAME,
                "file_name": "add_column_in_added_column",
            },
            {
                "type": "distinct_of_added_column",
                "name": TEST_PROJECT_NAME,
                "file_name": "distinct_of_added_column",
            },
            {
                "type": "formula_creating_existing_column",
                "name": TEST_PROJECT_NAME,
                "file_name": "formula_creating_existing_column",
            },
            {
                "type": "failure_and_rollback",
                "name": TEST_PROJECT_NAME,
                "file_name": "failure_and_rollback",
            },
        ],
        ids=[
            "add_column_n_sort",
            "aggregate_n_sort",
            "join_n_sort",
            "add_column_in_added_column",
            "distinct_of_added_column",
            "formula_creating_existing_column",
            "failure_and_rollback",
        ],
    )
    def test_dependency_violation(
        self,
        seed_on_startup: None,
        cleanup_no_code_models_on_teardown: Generator[None, None, None],
        tf_payload: dict[str, str],
        init_duckdb_project: dict[str, str],
        shared_datadir: Path,
        snapshot: Snapshot,
    ) -> None:
        """Tests all the transformation endpoints and compares the response
        with snapshot files."""
        Singleton.reset_cache()
        gc.collect()
        project_name = init_duckdb_project["project_name"]
        project_path = init_duckdb_project["project_path"]
        yaml_path = shared_datadir / YAML_PATH_PREFIX / "test_dependency_violation" / f"{tf_payload['file_name']}.yaml"
        tf_payload["file"] = open(f"{yaml_path}").read()
        tf_type = tf_payload["type"]
        del tf_payload["type"]

        # Create model
        body = {"model_name": tf_type, "parent_path": MODELS_NO_CODE}
        payload = json.dumps(body)
        response = request_factory.post(
            f"project/{project_name}/explorer/model/create",
            payload=payload,
        )
        json_parsed = response.json()
        assert json_parsed["status"] == "success", model_creation_failed
        file_check = Path(project_path) / project_name / "models" / "no_code" / tf_type
        assert file_check.is_file()

        # write yaml to no code model
        tf_payload["name"] = project_name
        payload = json.dumps(tf_payload)
        response = request_factory.post(
            f"project/{project_name}/no_code_model/{tf_type}",
            payload=payload,
        )
        json_parsed = response.json()
        assert json_parsed["status"] == "success", nocode_model_create_failed

        # Runs the no-code-model
        payload = json.dumps({"project_name": project_name})
        response = request_factory.post(
            f"project/{project_name}/execute/run",
            payload=payload,
        )
        json_parsed = response.json()
        assert json_parsed["status"] == "success", run_model_failed

        response = request_factory(f"project/{project_name}/schema/default/tables")
        json_parsed = response.json()
        assert f"default.{tf_type}" in json_parsed["table_names"]

        # write yaml to no code model
        yaml_path = (
            shared_datadir / YAML_PATH_PREFIX / "test_dependency_violation" / f"{tf_payload['file_name']}_violated.yaml"
        )
        tf_payload["file"] = open(f"{yaml_path}").read()
        payload = json.dumps(tf_payload)
        response = request_factory.post(
            f"project/{project_name}/no_code_model/{tf_type}",
            payload=payload,
            return_response=True,
        )
        json_parsed = response.json()
        if tf_type in (
            "add_column_n_sort",
            "add_column_in_added_column",
            "distinct_of_added_column",
            "merge_on_added_column",
        ):
            assert json_parsed["status"] == "failed", nocode_model_create_failed
            assert (
                "Column has dependency on existing transformation" in json_parsed["error_message"]
            ), failed_with_error_message
            return
        elif tf_type in ("formula_creating_existing_column",):
            assert json_parsed["status"] == "failed", nocode_model_create_failed
            assert "Column already exists" in json_parsed["error_message"], failed_with_error_message
            return

        assert json_parsed["status"] == "success", nocode_model_create_failed

        payload = json.dumps({"project_name": project_name})
        response = request_factory.post(
            f"project/{project_name}/execute/run",
            payload=payload,
            return_response=True,
        )
        json_parsed = response.json()
        assert json_parsed["status"] == "failed", run_model_failed
        if tf_type == "aggregate_n_sort":
            assert "'avg_salary' is not found in table" in json_parsed["error_message"], failed_with_error_message
        elif tf_type == "join_n_sort":
            assert "Column 'town' is not found in table" in json_parsed["error_message"], failed_with_error_message
        elif tf_type == "failure_and_rollback":
            assert json_parsed["status"] == "failed", nocode_model_create_failed

            response = request_factory(f"project/{project_name}/no_code_model/{tf_type}/rollback")
            json_parsed = response.json()
            json_data = json.dumps(json_parsed, indent=2) + "\n"

            snapshot.assert_match(json_data, f"transform_table_{tf_type}.json")

    @pytest.mark.parametrize(
        "tf_payload",
        [
            {
                "name": TEST_PROJECT_NAME,
                "file_name": "columns_simple_transform",
                "type": "columns_simple_transform",
            },
            {
                "name": TEST_PROJECT_NAME,
                "file_name": "columns_join_transform",
                "type": "columns_join_transform",
            },
            {
                "name": TEST_PROJECT_NAME,
                "file_name": "columns_transform_filter",
                "type": "columns_transform_filter",
            },
            {
                "name": TEST_PROJECT_NAME,
                "file_name": "columns_transform_synthesis",
                "type": "columns_transform_synthesis",
            },
            {
                "name": TEST_PROJECT_NAME,
                "file_name": "columns_transform_synthesis_and_groupby",
                "type": "columns_transform_synthesis_and_groupby",
            },
            {
                "name": TEST_PROJECT_NAME,
                "file_name": "columns_transform_groupby",
                "type": "columns_transform_groupby",
            },
            {
                "name": TEST_PROJECT_NAME,
                "file_name": "columns_transform_groupby_and_aggregator",
                "type": "columns_transform_groupby_and_aggregator",
            },
        ],
        ids=[
            "columns_simple_transform",
            "columns_join_transform",
            "columns_transform_filter",
            "columns_transform_synthesis",
            "columns_transform_synthesis_and_groupby",
            "columns_transform_groupby",
            "columns_transform_groupby_and_aggregator",
        ],
    )
    def test_model_columns(
        self,
        seed_on_startup: None,
        cleanup_no_code_models_on_teardown: Generator[None, None, None],
        init_duckdb_project: dict[str, str],
        snapshot: Snapshot,
        tf_payload: dict[str, str],
        shared_datadir: Path,
    ) -> None:
        """Tests mapping columns and it's corresponding datatypes."""
        Singleton.reset_cache()
        gc.collect()
        project_name = init_duckdb_project["project_name"]
        project_path = init_duckdb_project["project_path"]

        # Load YAML schema of no-code-model
        tf_type = tf_payload["type"]
        yaml_path = shared_datadir / YAML_PATH_PREFIX / "test_model_columns" / f"{tf_payload['file_name']}.yaml"
        tf_payload["file"] = open(f"{yaml_path}").read()

        snapshot_dir = Path(__file__).parent / "snapshots" / Path(__file__).stem / "test_model_columns"
        snapshot.snapshot_dir = str(snapshot_dir)

        # Creation of no_code_model
        body = {"model_name": tf_type, "parent_path": MODELS_NO_CODE}
        payload = json.dumps(body)
        response = request_factory.post(
            f"project/{project_name}/explorer/model/create",
            payload=payload,
        )
        json_parsed = response.json()
        assert json_parsed["status"] == "success", model_creation_failed
        file_check = Path(project_path) / project_name / "models" / "no_code" / tf_type
        assert file_check.is_file()

        # write yaml to no code model
        tf_payload["name"] = project_name
        payload = json.dumps(tf_payload)
        response = request_factory.post(
            f"project/{project_name}/no_code_model/{tf_type}",
            payload=payload,
        )
        json_parsed = response.json()
        assert json_parsed["status"] == "success", nocode_model_create_failed

        # Run the current no_code_model
        payload = json.dumps({"project_name": project_name})
        response = request_factory.post(f"project/{project_name}/execute/run", payload=payload, return_response=True)
        json_parsed = response.json()
        assert json_parsed["status"] == "success", nocode_model_create_failed

        response = request_factory(f"project/{project_name}/no_code_model/{tf_type}/content")
        json_parsed = response.json()
        del json_parsed["content"]
        json_data = json.dumps(json_parsed, indent=2) + "\n"

        snapshot.assert_match(json_data, f"{tf_type}.json")

    def test_ui_start(self, wait_till_ready: None, init_duckdb_project: dict[str, str], shared_datadir: Path) -> None:
        "dummy test to keep the server started and not exit, for debugging purposes"
        project_name = init_duckdb_project["project_name"]
        project_path = init_duckdb_project["project_path"]
        try:
            # delete sakila csv for three_join test
            for csv in MultiJoinCSVs:
                src = shared_datadir / "sakila" / csv
                dst = Path(project_path) / project_name / "seeds" / csv
                shutil.copy2(src, dst)

            if os.environ.get("START_UI") == "1":
                time.sleep(3600)
            # now access localhost:8000 from browser and check if you can see the UI.
        finally:
            for csv in MultiJoinCSVs:
                dst = Path(project_path) / project_name / "seeds" / csv
                dst.unlink()


@pytest.mark.ui
@pytest.mark.xdist_group(name="cloud")
class TestLineage:
    model_a = {
        "name": TEST_PROJECT_NAME,
        "file_name": "model_a",
    }

    model_b = {
        "name": TEST_PROJECT_NAME,
        "file_name": "model_b",
    }

    model_ab = {
        "name": TEST_PROJECT_NAME,
        "file_name": "model_ab",
    }

    def test_lineage_generation(
        self,
        init_duckdb_project: dict[str, str],
        shared_datadir: Path,
        seed_on_startup: None,
        # cleanup_no_code_models_on_teardown: Generator[None, None, None],
    ) -> None:
        """Test lineage API.

        Checks if model_a and model_b is connected to model_ab
        """
        try:

            project_path = init_duckdb_project["project_path"]
            project_name = init_duckdb_project["project_name"]

            # Create and save model_a
            tf_type = "model_a"
            body = {"model_name": tf_type, "parent_path": MODELS_NO_CODE}
            payload = json.dumps(body)
            response = request_factory.post(
                f"project/{project_name}/explorer/model/create",
                payload=payload,
            )
            json_parsed = response.json()
            assert json_parsed["status"] == "success", model_creation_failed
            file_check = Path(project_path) / project_name / "models" / "no_code" / tf_type
            assert file_check.is_file()

            TestLineage.model_a["file"] = open(
                shared_datadir / YAML_PATH_PREFIX / "test_lineage_generation" / f"{tf_type}.yaml"
            ).read()
            payload = json.dumps(TestLineage.model_a)
            response = request_factory.post(
                f"project/{project_name}/no_code_model/{tf_type}",
                payload=payload,
            )
            json_parsed = response.json()
            assert json_parsed["status"] == "success", f"{tf_type} creation failed"

            # Check model_a in lineage

            response = request_factory(
                f"project/{project_name}/lineage",
            )
            json_parsed = response.json()
            assert json_parsed["status"] == "success", lineage_failure
            assert tf_type in {item["data"]["label"] for item in json_parsed["data"]["nodes"]}
            assert len(json_parsed["data"]["edges"]) == 0

            # Create and save model_b
            tf_type = "model_b"
            body = {"model_name": tf_type, "parent_path": MODELS_NO_CODE}
            payload = json.dumps(body)
            response = request_factory.post(
                f"project/{project_name}/explorer/model/create",
                payload=payload,
            )
            json_parsed = response.json()
            assert json_parsed["status"] == "success", model_creation_failed
            file_check = Path(project_path) / project_name / "models" / "no_code" / tf_type
            assert file_check.is_file()

            TestLineage.model_b["file"] = open(
                shared_datadir / YAML_PATH_PREFIX / f"test_lineage_generation/{tf_type}.yaml"
            ).read()
            payload = json.dumps(TestLineage.model_b)
            response = request_factory.post(
                f"project/{project_name}/no_code_model/{tf_type}",
                payload=payload,
            )
            json_parsed = response.json()
            assert json_parsed["status"] == "success", "model_b creation failed"

            # Check model_a and model_b in lineage

            response = request_factory(
                f"project/{project_name}/lineage",
            )
            json_parsed = response.json()
            assert json_parsed["status"] == "success", lineage_failure
            assert "model_b" in {item["data"]["label"] for item in json_parsed["data"]["nodes"]}
            assert "model_a" in {item["data"]["label"] for item in json_parsed["data"]["nodes"]}
            assert len(json_parsed["data"]["edges"]) == 0

            # Run model_a and model_b
            payload = json.dumps({"project_name": project_name})
            response = request_factory.post(
                f"project/{project_name}/execute/run",
                payload=payload,
            )
            json_parsed = response.json()
            assert json_parsed["status"] == "success", run_model_failed
            # Create and save model_ab
            tf_type = "model_ab"
            body = {"model_name": tf_type, "parent_path": MODELS_NO_CODE}
            payload = json.dumps(body)
            response = request_factory.post(
                f"project/{project_name}/explorer/model/create",
                payload=payload,
            )
            json_parsed = response.json()
            assert json_parsed["status"] == "success", model_creation_failed
            file_check = Path(project_path) / project_name / "models" / "no_code" / tf_type
            assert file_check.is_file()

            TestLineage.model_ab["file"] = open(
                shared_datadir / YAML_PATH_PREFIX / f"test_lineage_generation/{tf_type}.yaml"
            ).read()
            payload = json.dumps(TestLineage.model_ab)
            response = request_factory.post(
                f"project/{project_name}/no_code_model/{tf_type}",
                payload=payload,
            )
            json_parsed = response.json()
            assert json_parsed["status"] == "success", f"{tf_type} creation failed"

            # Check model_a, model_b, model_ab in
            # lineage and check if they are connected

            response = request_factory(
                f"project/{project_name}/lineage",
            )
            json_parsed = response.json()
            assert json_parsed["status"] == "success", lineage_failure
            models_dict = {item["data"]["label"]: item["id"] for item in json_parsed["data"]["nodes"]}
            assert "model_b" in models_dict
            assert "model_a" in models_dict
            assert "model_ab" in models_dict
            edges = json_parsed["data"]["edges"]
            assert len(edges) == 2

            assert (
                sum([models_dict["model_ab"] == item["target"] for item in edges]) == 2
            ), "model_ab should be having 2 edges as target from model_a and model_b"
            assert (
                sum([models_dict["model_a"] == item["source"] for item in edges]) == 1
            ), "model_b should be having only one edge as source"
            assert (
                sum([models_dict["model_b"] == item["source"] for item in edges]) == 1
            ), "model_b should be having only one edge as source"
        finally:
            project_name = init_duckdb_project["project_name"]
            project_path = init_duckdb_project["project_path"]
            model_path = Path(project_path) / project_name / "models" / "no_code"
            to_delete_files = [model_path / "model_ab", model_path / "model_a", model_path / "model_b"]
            for path in to_delete_files:
                body = {"file_name": str(path)}
                payload = json.dumps(body)
                response = request_factory.delete(
                    f"project/{project_name}/explorer/file/delete",
                    payload=payload,
                    headers=CONTENT_JSON,
                )
                json_parsed = response.json()
                assert json_parsed["status"] == "success", model_delete_failed
                assert not path.exists()
