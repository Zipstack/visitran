import ibis
from dummyproject.models.full_code.blue import Blue
from dummyproject.models.full_code.green import Green
from dummyproject.models.full_code.red import Red
from ibis.expr.types.relations import Table

from visitran.materialization import Materialization


class WhiteRGB(Red, Green, Blue):  # type: ignore[misc]
    def __init__(self) -> None:
        super().__init__()
        self.materialization = Materialization.TABLE
        self.destination_table_name = "white_rgb"
        self.destination_schema_name = "colour"
        self.database = "universe"

    def select(self) -> Table:
        return ibis.table(dict(red="string", green="string", blue="string"), name="white_rgb")
