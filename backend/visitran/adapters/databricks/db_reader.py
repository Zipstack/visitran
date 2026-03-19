from visitran.adapters.databricks.connection import DatabricksConnection
from visitran.adapters.db_reader import BaseDBReader


class DatabricksDBReader(BaseDBReader):
    """Databricks database reader - inherits all functionality from BaseDBReader."""

    def __init__(self, db_connection: DatabricksConnection) -> None:
        super().__init__(db_connection=db_connection)
