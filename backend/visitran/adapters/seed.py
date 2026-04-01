from __future__ import annotations

import re
from abc import ABC
from os import path
from typing import TYPE_CHECKING

import ibis
import pandas as pd
from visitran.adapters.connection import BaseConnection
from visitran.errors import InvalidCSVHeaders, SeedFailureException

if TYPE_CHECKING:  # pragma: no cover
    from ibis.backends.base import BaseBackend
    from ibis.expr.types.relations import Table


class BaseSeed(ABC):
    def __init__(self, db_connection: BaseConnection, schema: str, abs_path: str) -> None:
        self._db_connection = db_connection
        self.schema: str = schema
        self.abs_path: str = abs_path
        self._file_name: str = self._get_abs_file_name()
        self._csv_table_name: str = ""
        self._agate_table = None
        self.destination_table_name: str = self.convert_filename_to_table_name(self.csv_file_name)

    def _get_abs_file_name(self) -> str:
        return self.abs_path.split(path.sep)[-1].split(".")[0]

    @property
    def csv_file_name(self) -> str:
        return self._file_name

    @staticmethod
    def convert_filename_to_table_name(filename: str) -> str:
        """This regex is to handle the csv filename if contains spaces and
        hyphens."""
        # Replace spaces and hyphens with underscores
        filename = filename.replace("-", "_")
        filename = filename.replace(" ", "_")
        # Remove characters that are not letters, digits, hyphens or underscores
        filename = re.sub(r"[^a-zA-Z0-9_-]", "", filename)
        return filename

    @property
    def db_connection(self) -> BaseConnection:
        return self._db_connection

    def get_csv_table(self) -> Table:
        try:
            # Read the CSV file into a pandas DataFrame
            df = pd.read_csv(self.abs_path)
            for col in df.columns:
                # Clean column name and check validity
                cleaned_col = col.strip().replace(" ", "_")
                cleaned_col = cleaned_col.replace('"', "")
                cleaned_col = cleaned_col.replace("'", "")

                # Check if this looks like a date column (contains forward slashes)
                if "/" in cleaned_col:
                    # For date-like columns, replace slashes with underscores to avoid duplicates
                    # e.g., "1/22/20" becomes "1_22_20", "12/2/20" becomes "12_2_20"
                    cleaned_col = cleaned_col.replace("/", "_")

                # Remove any remaining non-alphanumeric characters except underscores
                cleaned_col = re.sub(r"[^a-zA-Z0-9_]", "", cleaned_col).strip()

                if not cleaned_col or cleaned_col.strip() == "":
                    raise InvalidCSVHeaders(csv_file_name=self.csv_file_name, column_name=col)

                if df[col].dtype == "object":
                    try:
                        temp_dt = pd.to_datetime(df[col], format="mixed", errors="raise")
                        df[col] = temp_dt
                    except ValueError:
                        try:
                            temp_date = pd.to_datetime(df[col], format="mixed", errors="raise").dt.date
                            df[col] = temp_date
                        except ValueError:
                            pass
                df = df.rename(columns={col: cleaned_col})

            # Create an Ibis in-memory table from the pandas DataFrame
            table = ibis.memtable(df)
            return table
        except FileNotFoundError:
            raise SeedFailureException(seed_file_name=self.csv_file_name, error_message="File not found")
        except Exception as error:
            raise SeedFailureException(seed_file_name=self.csv_file_name, error_message=str(error))

    def execute(self) -> None:
        """This checks the CSV file is exist in DB, and creates schema in the
        target database from project configuration and inserts the CSV
        records."""

        # The drop SQL query will drop the table if it is only exists !
        # Constructing SQL statement for CSV schema in target adapters
        ibis_table: Table = self.get_csv_table()
        self.db_connection.create_schema(schema_name=self.schema)  # create schema if not exist
        self.db_connection.drop_table_if_exist(table_name=self.destination_table_name, schema_name=self.schema)
        self.db_connection.create_table(
            table_name=self.destination_table_name, table_statement=ibis_table, schema_name=self.schema
        )
