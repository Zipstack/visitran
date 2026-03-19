from visitran.adapters.databricks.connection import DatabricksConnection
from visitran.adapters.scd import BaseSCD
from visitran.templates.snapshot import VisitranSnapshot


class DatabricksSCD(BaseSCD):
    """Databricks Slowly Changing Dimensions implementation."""

    def __init__(self, db_connection: DatabricksConnection, visitran_scd: VisitranSnapshot) -> None:
        super().__init__(db_connection=db_connection, visitran_scd=visitran_scd)
