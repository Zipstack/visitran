import ibis
from ibis.expr.types.relations import Table

from visitran.materialization import Materialization
from visitran.templates.model import VisitranModel


class Green(VisitranModel):
    def __init__(self) -> None:
        super().__init__()
        self.materialization = Materialization.VIEW
        self.source_table_name = "emr"
        self.source_schema_name = "energy"
        self.destination_table_name = "green"
        self.destination_schema_name = "colour"
        self.database = "universe"

    def select(self) -> Table:
        return ibis.table(dict(two="string"), name="green")
