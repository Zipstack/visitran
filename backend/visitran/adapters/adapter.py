from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional, Union

from visitran.adapters.connection import BaseConnection
from visitran.adapters.db_reader import BaseDBReader
from visitran.adapters.model import BaseModel
from visitran.adapters.scd import BaseSCD
from visitran.adapters.seed import BaseSeed
from visitran.events.functions import fire_event
from visitran.events.types import MaterializationType, SqlExecutionCompleted
from visitran.templates.model import VisitranModel
from visitran.templates.snapshot import VisitranSnapshot


class BaseAdapter(ABC):
    def __init__(self, conn_details: dict[str, Union[str, int]]) -> None:
        self._connection: BaseConnection
        self._model: BaseModel
        self._seed: BaseSeed
        self._scd: BaseSCD
        self._db_reader: BaseDBReader

        self._initialize(conn_details)

    def _initialize(self, conn_details: dict[str, Union[str, int]]) -> None:
        self.load_connection_obj(conn_details=conn_details)

    @abstractmethod
    def load_connection_obj(self, conn_details: dict[str, Union[str, int]]) -> BaseConnection:
        raise NotImplementedError

    @abstractmethod
    def load_model(self, model: VisitranModel) -> Optional[BaseModel]:
        raise NotImplementedError

    @abstractmethod
    def load_seed(self, schema: str, abs_path: str) -> Optional[BaseSeed]:
        raise NotImplementedError

    @abstractmethod
    def load_scd(self, visitran_snapshot: VisitranSnapshot) -> Optional[BaseSCD]:
        raise NotImplementedError

    @abstractmethod
    def load_db_reader(self) -> Optional[BaseSCD]:
        raise NotImplementedError

    @property
    def db_connection(self) -> BaseConnection:
        return self._connection

    @property
    def db_model(self) -> BaseModel:
        return self._model

    @property
    def db_seed(self) -> BaseSeed:
        return self._seed

    @property
    def db_scd(self) -> BaseSCD:
        return self._scd

    @property
    def db_reader(self) -> BaseDBReader:
        return self._db_reader

    def run_model(self, visitran_model: VisitranModel):
        self.load_model(model=visitran_model)
        fire_event(MaterializationType(materialization=str(visitran_model.materialization)))
        return self.db_model.execute()

    def run_seeds(self, schema: str, abs_path: str) -> None:
        seed_obj = self.load_seed(schema, abs_path)
        self.db_seed.execute()
        fire_event(SqlExecutionCompleted())
        return seed_obj

    def run_scd(self, visitran_snapshot: VisitranSnapshot) -> None:
        self.load_scd(visitran_snapshot=visitran_snapshot)
        self.db_scd.execute()

    def get_db_details(self) -> dict[str, Any]:
        self.load_db_reader()
        return self.db_reader.execute()

    def update_current_table_in_db_metadata(self, db_metadata: str, table_name: str, schema_name: str) -> str:
        if not hasattr(self, "db_reader"):
            self.load_db_reader()
        return self.db_reader.update_table(
            db_metadata=db_metadata, table_name=table_name, schema_name=schema_name
        )

    def delete_current_table_in_db_metadata(self, db_metadata: str, table_name: str, schema_name: str) -> str:
        if not hasattr(self, "db_reader"):
            self.load_db_reader()
        return self.db_reader.delete_table(
            db_metadata=db_metadata, table_name=table_name, schema_name=schema_name
        )
