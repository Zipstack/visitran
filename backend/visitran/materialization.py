from __future__ import annotations

from enum import Enum, auto


class Materialization(Enum):
    """Ways in which a table can be materialized."""

    EPHEMERAL = auto()
    TABLE = auto()
    VIEW = auto()
    INCREMENTAL = auto()
