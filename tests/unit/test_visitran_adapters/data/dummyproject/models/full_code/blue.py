import ibis
from ibis.expr.types.relations import Table

from visitran.materialization import Materialization
from visitran.templates.model import VisitranModel


class Blue(VisitranModel):
    def __init__(self) -> None:
        super().__init__()
        self.materialization = Materialization.TABLE
        self.source_table_name = "emr"
        self.source_schema_name = "energy"
        self.destination_table_name = "blue"
        self.destination_schema_name = "colour"
        self.database = "universe"

    def select(self) -> Table:
        return ibis.table(dict(three="string"), name="blue")
