import concurrent.futures
import logging
import time
from typing import Any, Dict

import sqlalchemy

from visitran.adapters.db_reader import BaseDBReader
from visitran.adapters.snowflake.connection import SnowflakeConnection


class SnowflakeDBReader(BaseDBReader):
    def __init__(self, db_connection: SnowflakeConnection) -> None:
        super().__init__(db_connection)
        self.sqlalchemy_engine = sqlalchemy.create_engine(
            self.connection.connection_string,
        )
        self.inspector = sqlalchemy.inspect(self.sqlalchemy_engine)
        # Cache for faster repeated calls
        self._cache = {}
        self._cache_timestamp = 0
        self._cache_ttl = 300  # 5 minutes cache

    def execute(self, existing_db_metadata: str = "") -> dict[str, Any]:
        """Override the base execute method to provide much faster Snowflake-
        specific implementation.

        Uses SQLAlchemy inspector and parallel processing for better
        performance.
        """
        # Check cache first
        current_time = time.time()
        if (current_time - self._cache_timestamp) < self._cache_ttl and self._cache:
            logging.info("Using cached database info for faster response")
            return self._cache

        logging.info("Building fresh database metadata tree...")

        try:
            # Use SQLAlchemy inspector for faster schema/table discovery
            schemas = self.inspector.get_schema_names()
            result = {"schemas": schemas, "tables": {}}

            # Process schemas in parallel for better performance
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                # Submit schema scanning tasks
                future_to_schema = {executor.submit(self._scan_schema_tables, schema): schema for schema in schemas}

                # Collect results as they complete
                for future in concurrent.futures.as_completed(future_to_schema):
                    schema, tables_info = future.result()
                    if tables_info:
                        result["tables"].update(tables_info)

            # Cache the result
            self._cache = result
            self._cache_timestamp = current_time

            logging.info(
                f"Database metadata tree built successfully: {len(schemas)} schemas, {len(result['tables'])} tables"
            )
            return result

        except Exception as e:
            logging.error(f"Error building database metadata tree: {e}")
            # Fallback to base implementation if inspector fails
            logging.info("Falling back to base implementation...")
            return super().execute(existing_db_metadata)

    def _scan_schema_tables(self, schema: str) -> tuple[str, dict[str, Any]]:
        """Scan tables in a specific schema using optimized methods.

        Returns tuple of (schema_name, tables_info_dict)
        """
        try:
            tables_info = {}

            # Get tables for this schema using inspector (faster)
            tables = self.inspector.get_table_names(schema=schema)

            for table in tables:
                try:
                    # Get table info using optimized method
                    table_info = self._get_optimized_table_info(schema, table)
                    if table_info:
                        tables_info[table] = table_info

                except Exception as table_error:
                    logging.warning(f"Error getting info for table {schema}.{table}: {table_error}")
                    # Continue with other tables
                    continue

            return schema, tables_info

        except Exception as schema_error:
            logging.error(f"Error scanning schema {schema}: {schema_error}")
            return schema, {}

    def _get_optimized_table_info(self, schema: str, table: str) -> dict[str, Any]:
        """Get optimized table information using SQLAlchemy inspector.

        Much faster than the base implementation.
        """
        try:
            # Get columns using inspector (faster than raw SQL)
            columns_info = self.inspector.get_columns(table, schema=schema)

            # Get primary key info
            primary_keys = self.inspector.get_pk_constraint(table, schema=schema)
            pk_columns = primary_keys.get("constrained_columns", [])

            # Build column information
            columns = []
            for col in columns_info:
                column_info = {
                    "name": col["name"],
                    "type": str(col["type"]),
                    "nullable": col.get("nullable", True),
                    "default": col.get("default", None),
                    "primary_key": col["name"] in pk_columns,
                }
                columns.append(column_info)

            # Get table size info if available (optional)
            table_size = None
            try:
                # This is a lightweight query to get row count
                with self.sqlalchemy_engine.connect() as conn:
                    result = conn.execute(f"SELECT COUNT(*) as row_count FROM {schema}.{table}").fetchone()
                    table_size = result[0] if result else None
            except:
                # Ignore size query errors, not critical
                pass

            return {
                "name": table,
                "schema_name": schema,
                "columns": columns,
                "primary_keys": pk_columns,
                "row_count": table_size,
                "last_updated": time.time(),
            }

        except Exception as e:
            logging.error(f"Error getting optimized table info for {schema}.{table}: {e}")
            # Fallback to base method if inspector fails
            try:
                return self.get_table_info(schema_name=schema, table_name=table)
            except:
                return None

    def get_fast_table_info(self, schema: str, table: str) -> dict[str, Any]:
        """Get table info quickly without building the full tree.

        Useful for getting info about specific tables only.
        """
        return self._get_optimized_table_info(schema, table)

    def get_schema_summary(self) -> dict[str, Any]:
        """Get a quick summary of schemas and table counts without detailed
        column info.

        Much faster than full execute().
        """
        try:
            schemas = self.inspector.get_schema_names()
            summary = {"schemas": schemas, "table_counts": {}}

            for schema in schemas:
                try:
                    tables = self.inspector.get_table_names(schema=schema)
                    summary["table_counts"][schema] = len(tables)
                except:
                    summary["table_counts"][schema] = 0

            return summary

        except Exception as e:
            logging.error(f"Error getting schema summary: {e}")
            return {"schemas": [], "table_counts": {}}
