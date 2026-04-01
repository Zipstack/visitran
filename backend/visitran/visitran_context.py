from typing import Any, Dict, Union

from visitran.adapters.adapter import BaseAdapter
from visitran.utils import get_adapter_cls


class VisitranContext:
    def __init__(
        self,
        project_config: dict[str, Any],
        is_api_call: bool = False,
        env_data: dict[str, Any] = None,
    ) -> None:
        self.env_data = env_data or {}
        self.project_conf: dict[str, Any] = project_config or {}
        self.project_path = self.project_conf.get("project_path")
        self._project_name: str = self.project_conf.get("name", "")
        self._db_details: dict[str, Any] = self.project_conf.get("connection_details")
        self._db_type: str = self.project_conf.get("db_type")
        self._conn_details: dict[str, Any] = self.__update_connection_details(env_data)

        default_schema_key = "dataset_id" if self._db_type == "bigquery" else "schema"
        self._schema_name: str = (
            self.env_data.get("schema")
            or self.project_conf.get("project_schema")
            or self._conn_details.get(default_schema_key)
        )
        self._db_adapter: BaseAdapter = self.__load_db_adapter()
        self.is_api_call = is_api_call

    @property
    def database_type(self):
        return self._db_type

    @property
    def schema_name(self):
        return self._schema_name

    @property
    def db_connection_details(self) -> dict[str, Union[str, int]]:
        return self._conn_details

    def __load_db_adapter(self) -> BaseAdapter:
        adapter_cls: type[BaseAdapter] = get_adapter_cls(adapter_name=self._db_type)
        return adapter_cls(conn_details=self.db_connection_details)

    def __update_connection_details(self, env_data: dict[str, Any]):
        if self._db_type == "duckdb":
            return self._db_details.copy()

        # Create a copy to avoid modifying the original project config
        conn_details = self._db_details.copy()

        if env_data:
            conn_details.update(env_data)
        elif self.project_conf.get("project_schema"):
            if self._db_type == "bigquery":
                conn_details.update({"dataset_id": self.project_conf.get("project_schema")})
            else:
                conn_details.update({"schema": self.project_conf.get("project_schema")})
        return conn_details

    @property
    def db_adapter(self) -> BaseAdapter:
        return self._db_adapter

    def list_all_schemas(self) -> list[Any]:
        return self.db_adapter.db_connection.list_all_schemas()

    def drop_table_if_exist(self, schema_name: str, table_name: str):
        return self.db_adapter.db_connection.drop_table_if_exist(schema_name=schema_name, table_name=table_name)

    def create_schema(self):
        self.db_adapter.db_connection.create_schema(self.schema_name)

    def list_all_tables(self, schema_name: str = "") -> list[Any]:
        schema_name = schema_name or self.schema_name
        return self.db_adapter.db_connection.list_all_tables(schema_name=schema_name)

    def import_database(self, db_path: str, import_path: str) -> None:
        return self.db_adapter.db_connection.import_database(db_path=db_path, import_path=import_path)

    def get_table_columns_with_type(self, schema_name: str, table_name: str) -> list[dict[str, Any]]:
        return self.db_adapter.db_connection.get_table_columns_with_type(schema_name=schema_name, table_name=table_name)

    def get_table_columns(self, schema_name: str, table_name: str) -> list[str]:
        return self.db_adapter.db_connection.get_table_columns(schema_name=schema_name, table_name=table_name)

    def close_db_connection(self) -> None:
        """Closes the database connection."""
        self.db_adapter.db_connection.close_connection()
