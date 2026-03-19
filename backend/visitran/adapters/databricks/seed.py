from visitran.adapters.databricks.connection import DatabricksConnection
from visitran.adapters.seed import BaseSeed


class DatabricksSeed(BaseSeed):
    """Databricks seed implementation - inherits all functionality from BaseSeed."""

    def __init__(self, db_connection: DatabricksConnection, schema: str, abs_path: str) -> None:
        super().__init__(db_connection=db_connection, schema=schema, abs_path=abs_path)
