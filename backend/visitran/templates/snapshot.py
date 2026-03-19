from visitran.singleton import Singleton


class VisitranSnapshot(metaclass=Singleton):
    """Base class which inherits by CLI application for snapshot."""

    def __init__(self) -> None:
        # Database name in which snapshot operation performs
        self.database: str = ""

        # Source db info
        self.source_table_name: str = ""
        self.source_schema_name: str = ""

        # Snapshot db info
        self.snapshot_table_name: str = ""
        self.snapshot_schema_name: str = ""

        # Snapshot information's
        self.unique_key: str = ""
        self.strategy: str = ""
        self.updated_at: str = ""

        self.check_cols: list[str] = []
        self.invalidate_hard_deletes: bool = False
