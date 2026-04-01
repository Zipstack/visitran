from typing import Any

import yaml

from visitran.adapters.connection import BaseConnection


class BaseDBReader:
    TWO_WEEKS = 60 * 60 * 24 * 14

    def __init__(self, db_connection: BaseConnection) -> None:
        self.connection: BaseConnection = db_connection
        self.inspector = None
        self.map: dict[str, Any] = {}

    def get_table_info(self, schema_name: str, table_name: str) -> tuple[str, dict[str, Any]]:
        table_schema = self.connection.get_table_obj(schema_name=schema_name, table_name=table_name).schema()
        columns = []
        for name, dtype in zip(table_schema.names, table_schema.types):
            column = {
                "name": name,
                "dtype": str(dtype),
                "nullable": dtype.nullable,
                "autoincrement": False,
                "default": None,
                "comment": "",
            }

            columns.append(column)

        if self.inspector is not None:
            sqlalchemy_cols = self.inspector.get_columns(table_name, schema_name)
            foreign_keys = self.inspector.get_foreign_keys(table_name, schema_name)
            primary_keys = self.inspector.get_pk_constraint(table_name, schema_name)
            try:
                unique_constraints = self.inspector.get_unique_constraints(table_name, schema_name)
            except Exception:
                unique_constraints = []

            try:
                indexes = self.inspector.get_indexes(table_name, schema_name)
                for sqlalchemy_col in sqlalchemy_cols:
                    for column in columns:
                        if column["name"] == sqlalchemy_col["name"]:
                            column["autoincrement"] = sqlalchemy_col["autoincrement"]
                            column["default"] = sqlalchemy_col["default"]
                            column["comment"] = sqlalchemy_col["comment"]
                            break
            except Exception:
                indexes = []
        else:
            foreign_keys = []
            primary_keys = []
            unique_constraints = []
            indexes = []

        table_info = {
            "name": table_name,
            "schema_name": schema_name,
            "primary_keys": primary_keys,
            "foreign_keys": foreign_keys,
            "unique_constraints": unique_constraints,
            "indexes": indexes,
            "columns": columns,
        }
        return table_name, table_info

    def execute(self, existing_db_metadata: str = "") -> dict[str, Any]:
        import logging

        schemas = self.connection.list_all_schemas()
        self.map["schemas"] = list(schemas)
        if "tables" not in self.map:
            self.map["tables"] = {}
        for schema in schemas:
            tables = self.connection.list_all_tables(schema_name=schema)
            for table in tables:
                try:
                    table_name, table_info = self.get_table_info(schema_name=schema, table_name=table)
                    self.map["tables"][table_name] = table_info
                except Exception as e:
                    # Log the problematic table and continue with other tables
                    logging.warning(f"Skipping table '{schema}.{table}' due to schema introspection error: {str(e)}")
                    continue
        return self.map

    def get_sample_data(self, table_info: dict):
        table = table_info["name"]
        schema = table_info["schema_name"]
        # TODO : Use direct sampleing feature of database if available
        sample_data = self.connection.get_table_obj(schema_name=schema, table_name=table).limit(100).execute()
        if len(sample_data) > 5:
            sample_data = sample_data.sample(5)
        for column in table_info["columns"]:
            column_name = column["name"]
            column["sample_data"] = []
            for data in sample_data[column_name]:
                val = str(data)
                if len(val) > 100:
                    column["sample_data"].append(val[:100] + "...(truncated)")
                else:
                    column["sample_data"].append(val)

    def gather_sample_data(self) -> None:
        for table_info in self.map["tables"]:
            self.get_sample_data(table_info)

    def update_table(self, db_metadata: str, table_name: str, schema_name: str) -> str:
        self.map = yaml.safe_load(db_metadata)
        table_name, table_info = self.get_table_info(schema_name=schema_name, table_name=table_name)
        # self.get_sample_data(table_info)
        self.map["tables"][table_name] = table_info
        return self.map

    def delete_table(self, db_metadata: str, table_name: str, schema_name: str) -> str:
        self.map = yaml.safe_load(db_metadata)
        try:
            self.map["tables"].pop(table_name)
        except KeyError:
            pass
        return self.map

    def get_db_map_yaml(self) -> str:
        return yaml.dump(self.map, default_flow_style=False, sort_keys=False)
