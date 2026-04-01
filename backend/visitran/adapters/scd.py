"""This is base class for Snapshot commands, This has to be inherited in all
the adapters which supports snapshot."""

from abc import ABC, abstractmethod
from typing import Any

from visitran.adapters.connection import BaseConnection
from visitran.constants import SnapshotConstants
from visitran.errors import InvalidSnapshotFields, InvalidSnapshotColumns
from visitran.templates.snapshot import VisitranSnapshot
from visitran.utils import generate_tmp_name


class BaseSCD(ABC):
    """Abstract base class holds basic methods for SCD queries."""

    def __init__(self, db_connection: BaseConnection, visitran_scd: VisitranSnapshot):
        # Configured attributes
        self._db_connection = db_connection
        self._visitran_scd: VisitranSnapshot = visitran_scd
        self._scd_conf = visitran_scd.__dict__

        # Operational attributes
        self._temp_table_name: str = ""
        self._row_changed_expr: str = ""

    def __getitem__(self, item: str) -> Any:
        """Returns items from scd conf dict."""
        return self._scd_conf.get(item)

    def __setitem__(self, key: str, value: Any) -> None:
        """Sets the item to SCD conf dict."""
        self._scd_conf[key] = value

    @property
    def visitran_scd(self) -> VisitranSnapshot:
        return self._visitran_scd

    @property
    def database_name(self) -> str:
        return self["database"] or ""

    @property
    def source_schema_name(self) -> str:
        return self["source_schema_name"] or ""

    @property
    def source_table_name(self) -> str:
        return self["source_table_name"] or ""

    @property
    def snapshot_schema_name(self) -> str:
        return self["snapshot_schema_name"] or ""

    @property
    def snapshot_table_name(self) -> str:
        return self["snapshot_table_name"] or ""

    @property
    def unique_id_attr(self) -> str:
        return self["unique_key"] or ""

    @property
    def scd_strategy(self) -> str:
        return self["strategy"] or ""

    @property
    def updated_at_attr(self) -> str:
        return self["updated_at"] or ""

    @property
    def check_cols(self) -> list[str]:
        return self["check_cols"] or []

    @property
    def invalidate_hard_deletes(self) -> bool:
        return self["invalidate_hard_deletes"] or False

    @property
    def source_sql(self) -> str:
        """SQL notation for SCD source table."""
        return f'"{self.database_name}"."{self.source_schema_name}"."{self.source_table_name}"'

    @property
    def target_sql(self) -> str:
        """SQL notation for SCD target table."""
        return f'"{self.database_name}"."{self.snapshot_schema_name}"."{self.snapshot_table_name}"'

    @property
    def temporary_table(self) -> str:
        return f'"{self._temp_table_name}"'

    @property
    def row_changed_expr(self) -> str:
        if not self._row_changed_expr:
            self._row_changed_expr = self.__get_row_changed_expr_by_strategy()
        return self._row_changed_expr

    def __get_row_changed_expr_by_strategy(self) -> str:
        if self.scd_strategy == "timestamp":
            return self.__get_timestamp_row_change_expr()
        else:
            return self.__get_check_row_change_expr(check_cols=self.check_cols)

    @staticmethod
    def __get_timestamp_row_change_expr() -> str:
        """Returns change expression for timestamp strategy."""
        return "(snapshotted_data.scd_valid_from < source_data.updated_at)"

    @staticmethod
    def __get_check_row_change_expr(check_cols: list[str]) -> str:
        """Returns change expression for check strategy."""
        _expr = ""
        col_count: int = len(check_cols) - 1
        for loop_indx, col in enumerate(check_cols):
            _expr += f"""snapshotted_data."{col}" != source_data."{col}"
            or  (
            ((snapshotted_data."{col}" is null) and not (source_data."{col}" is null))
            or
            ((not snapshotted_data."{col}" is null) and (source_data."{col}" is null))
            )
            """
            if loop_indx < col_count:
                _expr += " or "
        return _expr

    def create_temporary_table_name(self) -> str:
        self._temp_table_name = self.snapshot_table_name + generate_tmp_name(prefix="_postgres_scd_tmp_")
        return self._temp_table_name

    @staticmethod
    def snapshot_hash_arguments(coloumns: list[str]) -> str:
        """This is to determine the unique hash argument for the snapshot
        columns."""
        no_of_cols = len(coloumns) - 1
        hash_arg = "md5("
        for loop_indx, col in enumerate(coloumns):
            hash_arg += f"coalesce(cast({col} as varchar ), '')"
            if loop_indx < no_of_cols:
                hash_arg += " || '|' || "
        hash_arg += ")"
        return hash_arg

    @staticmethod
    @abstractmethod
    def get_snapshot_timestamp() -> str:
        """Gets the current timestamp syntax based on the adapters."""
        raise NotImplementedError

    def create_snapshot_table(self) -> str:
        """This method returns the SQL creation schema for snapshot table based
        on the strategy applied."""
        snp_strategy: str = f"scd_{self.scd_strategy}_table"
        return getattr(self, snp_strategy)()  # type: ignore[no-any-return]

    @abstractmethod
    def scd_timestamp_table(self) -> str:
        """This method has to be inherited in adapters for timestamp based SCD
        strategy."""
        raise NotImplementedError

    @abstractmethod
    def scd_check_table(self) -> str:
        """This method has to be inherited in adapters for check based SCD
        strategy."""
        raise NotImplementedError

    @abstractmethod
    def create_snapshot_staging_table(self) -> str:
        """Returns the schema for temporary table."""
        raise NotImplementedError

    @abstractmethod
    def merge_snapshot_staging_table(self, staging_cols: list[str]) -> str:
        """This method has to be inherited in adapters, This returns the schema
        for merging the temporary staging table."""
        raise NotImplementedError

    def _is_valid_check_cols(self, check_cols: list[str]) -> None:
        """This method check the coloumns specified in check_cols are valid and
        exists in the source DB."""
        invalid_cols = list(set(check_cols) & set(SnapshotConstants.RESTRICTED_COLS))

        table_cols = self._db_connection.get_table_columns(
            schema_name=self.source_schema_name, table_name=self.source_table_name
        )

        for cols in check_cols:
            if cols not in table_cols and cols not in invalid_cols:
                invalid_cols.append(cols)

        if invalid_cols:
            raise InvalidSnapshotColumns(invalid_cols=invalid_cols)

    def validate_snapshot_configuration(self) -> None:
        """This validates the given configuration."""
        invalid_fields: list[str] = []
        mandatory_fields: dict[str, Any] = {
            "source_table_name": str,
            "source_schema_name": str,
            "snapshot_table_name": str,
            "snapshot_schema_name": str,
            "database": str,
            "invalidate_hard_deletes": bool,
            "unique_key": str,
            "strategy": str,
        }
        if self.visitran_scd.strategy in (SnapshotConstants.TIMESTAMP,):
            mandatory_fields[SnapshotConstants.UPDATED_AT] = str
        elif self.visitran_scd.strategy in (SnapshotConstants.CHECK,):
            mandatory_fields[SnapshotConstants.CHECK_COLS] = list
            self._is_valid_check_cols(self.check_cols)
        else:
            mandatory_fields.pop(SnapshotConstants.STRATEGY)
            invalid_fields.append(SnapshotConstants.STRATEGY)

        for key, value in mandatory_fields.items():
            _attr = self.__getattribute__(key)
            if not _attr or not isinstance(_attr, value):
                invalid_fields.append(key)
                self._scd_conf[key] = _attr

        if invalid_fields:
            raise InvalidSnapshotFields(invalid_fields=invalid_fields)

    @abstractmethod
    def execute(self) -> bool:
        raise NotImplementedError
