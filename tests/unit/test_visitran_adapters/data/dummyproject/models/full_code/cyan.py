import ibis
from dummyproject.models.full_code.blue import Blue
from dummyproject.models.full_code.green import Green
from ibis.expr.types.relations import Table

from visitran.materialization import Materialization


class Cyan(Green, Blue):  # type: ignore[misc]
    def __init__(self) -> None:
        super().__init__()
        self.materialization = Materialization.EPHEMERAL
        self.destination_table_name = "cyan"
        self.destination_schema_name = "colour"
        self.database = "universe"

    def select(self) -> Table:
        return ibis.table(dict(green="string", blue="string"), name="cyan")
