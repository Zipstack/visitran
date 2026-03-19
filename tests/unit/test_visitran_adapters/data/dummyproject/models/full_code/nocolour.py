import ibis
from dummyproject.models.full_code.blue import Blue
from dummyproject.models.full_code.red import Red
from ibis.expr.types.relations import Table

from visitran.materialization import Materialization


class NoColour(Red, Blue):  # type: ignore[misc]
    def __init__(self) -> None:
        super().__init__()
        self.materialization = Materialization.EPHEMERAL
        self.database = "universe"

    def select(self) -> Table:
        return ibis.table(dict(radio_wave="string"), name="no_colour")
