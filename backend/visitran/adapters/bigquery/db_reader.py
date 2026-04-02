import logging
from typing import Any

import sqlalchemy
from visitran.adapters.bigquery.connection import BigQueryConnection
from visitran.adapters.db_reader import BaseDBReader


class BigQueryDBReader(BaseDBReader):
    def __init__(self, db_connection: BigQueryConnection) -> None:
        super().__init__(db_connection)
        self.sqlalchemy_engine = sqlalchemy.create_engine(
            self.connection.connection_string,
            credentials_info=self.connection.credentials_dict,
        )
        self.inspector = sqlalchemy.inspect(self.sqlalchemy_engine)

    def get_table_info(self, schema_name: str, table_name: str) -> tuple[str, dict[str, Any]]:
        """Get table info, falling back to SQLAlchemy for tables Ibis can't
        handle.

        BigQuery's INTERVAL type doesn't specify precision/unit, causing
        Ibis to fail with "Interval precision is None". This override
        catches such errors and uses SQLAlchemy inspector as a fallback.
        """
        try:
            # Try normal Ibis-based schema parsing
            return super().get_table_info(schema_name, table_name)
        except Exception as e:
            error_msg = str(e).lower()
            # Fallback to SQLAlchemy for INTERVAL column errors or other Ibis parsing issues
            if "interval" in error_msg or "precision" in error_msg:
                logging.warning(
                    f"Table '{schema_name}.{table_name}' has columns Ibis can't parse "
                    f"(likely INTERVAL). Falling back to SQLAlchemy: {e}"
                )
                return self._get_table_info_via_sqlalchemy(schema_name, table_name)
            raise

    def _get_table_info_via_sqlalchemy(self, schema_name: str, table_name: str) -> tuple[str, dict[str, Any]]:
        """Fallback method using SQLAlchemy inspector for tables Ibis can't
        handle.

        This handles BigQuery tables with INTERVAL columns that cause
        Ibis to fail.
        """
        columns = []
        sqlalchemy_cols = self.inspector.get_columns(table_name, schema_name)

        for col in sqlalchemy_cols:
            columns.append({
                "name": col["name"],
                "dtype": str(col["type"]),  # SQLAlchemy type as string (e.g., "INTERVAL")
                "nullable": col.get("nullable", True),
                "autoincrement": col.get("autoincrement", False),
                "default": col.get("default"),
                "comment": col.get("comment", "")
            })

        # Get constraints using inspector
        foreign_keys = self.inspector.get_foreign_keys(table_name, schema_name)
        primary_keys = self.inspector.get_pk_constraint(table_name, schema_name)

        try:
            unique_constraints = self.inspector.get_unique_constraints(table_name, schema_name)
        except Exception:
            unique_constraints = []

        try:
            indexes = self.inspector.get_indexes(table_name, schema_name)
        except Exception:
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
