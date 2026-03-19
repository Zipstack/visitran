import ibis
from dummyproject.models.full_code.cyan import Cyan
from dummyproject.models.full_code.red import Red
from ibis.expr.types.relations import Table

from visitran.materialization import Materialization


class WhiteCR(Cyan, Red):  # type: ignore[misc]
    def __init__(self) -> None:
        super().__init__()
        self.materialization = Materialization.TABLE
        self.destination_table_name = "white_cr"
        self.destination_schema_name = "colour"
        self.database = "universe"

    def select(self) -> Table:
        return ibis.table(dict(cyan="string", red="string"), name="white_cr")
