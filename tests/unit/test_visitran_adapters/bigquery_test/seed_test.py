from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from adapters.bigquery.adapter import BigQueryAdapter
from adapters.bigquery.seed import BigQuerySeed
from google.cloud.exceptions import NotFound

from tests.unit.test_visitran_adapters.bigquery_test.adapter_test import TestBigQueryAdapter
from visitran import DoesNotExistError, EmptyFileError, NotSupportedError

if TYPE_CHECKING:  # pragma: no cover
    from google.cloud.bigquery.client import Client


DUMMY_SEED_FILE = "seed.csv"


@pytest.mark.bigquery
@pytest.mark.xdist_group(name="bigquery")
class TestBigQuerySeed(TestBigQueryAdapter):
    def test_bigquery_seed_setup(self, tmp_path: Path) -> None:
        """Test that the BigQuerySeed object is correctly initialized with the
        given parameters."""
        bigquery_adapter = BigQueryAdapter(conn_details=self.CONN_DETAILS)
        non_existant_seed_file = str(tmp_path / DUMMY_SEED_FILE)
        seedobj = bigquery_adapter.load_seed("devtests", non_existant_seed_file)
        assert isinstance(seedobj, BigQuerySeed)
        assert seedobj.dataset_id == "devtests"
        assert seedobj.abs_path == non_existant_seed_file
        assert seedobj._file_name == "seed"

    def test_bigquery_seed_invalid_file(self, tmp_path: Path) -> None:
        """Test that an error is raised when attempting to execute a
        BigQuerySeed object with an invalid file."""
        bigquery_adapter = BigQueryAdapter(conn_details=self.CONN_DETAILS)

        invalidcsvfile = tmp_path / "seed.parquet"

        seedobj = bigquery_adapter.load_seed("devtests", str(invalidcsvfile))
        with pytest.raises(DoesNotExistError):
            seedobj.execute()

        invalidcsvfile.touch()
        seedobj = bigquery_adapter.load_seed("devtests", str(invalidcsvfile))
        with pytest.raises(NotSupportedError):
            seedobj.execute()

    def test_bigquery_seed_empty_csv(self, tmp_path: Path) -> None:
        """Test that an error is raised when attempting to execute a
        BigQuerySeed object with an empty CSV file."""
        bigquery_adapter = BigQueryAdapter(conn_details=self.CONN_DETAILS)

        csvfile = tmp_path.joinpath(DUMMY_SEED_FILE)
        csvfile.touch()
        seedobj = bigquery_adapter.load_seed("devtests", str(csvfile))

        with pytest.raises(EmptyFileError):
            seedobj.execute()

    def test_bigquery_seed_header_only_csv(self, tmp_path: Path) -> None:
        """Test that a BigQuery table is created when executing a BigQuerySeed
        object with a CSV file that only contains headers."""
        bigquery_adapter = BigQueryAdapter(conn_details=self.CONN_DETAILS)

        csvfile = tmp_path.joinpath(DUMMY_SEED_FILE)
        csvfile.touch()
        seedobj = bigquery_adapter.load_seed("devtests", str(csvfile))

        with open(csvfile, "w") as file:
            file.write("id,name,age\n")

        seedobj.execute()

        conn = bigquery_adapter.db_connection.connection
        client: Client = conn.client
        table_id = "visitran-bq.devtests.seed"

        try:
            val = client.get_table(table_id)
            assert val.num_rows == 0
        except NotFound:
            pytest.fail(f"{table_id} not found")

    def test_bigquery_seed_csv(self, tmp_path: Path) -> None:
        """Test that a BigQuery table is created with the correct data when
        executing a BigQuerySeed object with a CSV file that contains data."""
        bigquery_adapter = BigQueryAdapter(conn_details=self.CONN_DETAILS)

        csvfile = tmp_path.joinpath(DUMMY_SEED_FILE)
        csvfile.touch()
        seedobj = bigquery_adapter.load_seed("devtests", str(csvfile))

        with open(csvfile, "w") as file:
            file.write("id,name,age\n")
            file.write("1,John,25\n")
            file.write("2,Jack,26\n")
            file.write("3,Jill,27\n")

        seedobj.execute()

        conn = bigquery_adapter.db_connection.connection

        client: Client = conn.client
        table_id = "visitran-bq.devtests.seed"

        try:
            val = client.get_table(table_id)
            assert val.num_rows == 3
        except NotFound:
            pytest.fail(f"{table_id} not found")
