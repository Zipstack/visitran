from typing import Union

import pytest
from adapters.bigquery.adapter import BigQueryAdapter
from adapters.bigquery.connection import BigQueryConnection
from adapters.bigquery.model import BigQueryModel

from visitran.templates.model import VisitranModel


@pytest.mark.bigquery
@pytest.mark.xdist_group(name="bigquery")
class TestBigQueryAdapter:
    CONN_DETAILS: dict[str, Union[str, int]] = {
        "project_id": "visitran-bq",
        "dataset_id": "raw",
        "auth_external_data": False,
        "partition_column": "PARTITIONTIME",
    }

    def test_bigquery_adapter(self) -> None:
        """Test the BigQueryAdapter class by checking if it can establish a
        project_connection to BigQuery and load a VisitranModel object into a
        BigQueryModel object."""
        visitran_obj = VisitranModel()  # type: ignore[abstract]

        bigquery_adapter = BigQueryAdapter(conn_details=self.CONN_DETAILS)
        assert isinstance(bigquery_adapter.db_connection, BigQueryConnection)
        assert isinstance(bigquery_adapter.load_model(model=visitran_obj), BigQueryModel)
