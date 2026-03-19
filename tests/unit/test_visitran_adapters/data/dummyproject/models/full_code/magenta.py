import ibis
from dummyproject.models.full_code.blue import Blue
from dummyproject.models.full_code.red import Red
from ibis.expr.types.relations import Table

from visitran.materialization import Materialization


class Magenta(Red, Blue):  # type: ignore[misc]
    def __init__(self) -> None:
        super().__init__()
        self.materialization = Materialization.TABLE
        self.destination_table_name = "magenta"
        self.destination_schema_name = "colour"
        self.database = "universe"

    def select(self) -> Table:
        return ibis.table(dict(red="string", blue="string"), name="magenta")
