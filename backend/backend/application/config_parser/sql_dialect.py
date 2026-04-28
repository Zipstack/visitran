"""
SQL Dialect Abstraction for Database-Specific SQL Generation.

This module provides an abstraction layer for database-specific SQL syntax,
following SOLID principles to enable extensible, maintainable SQL generation.

Design Principles:
- Single Responsibility: Each dialect handles only its database's SQL syntax
- Open/Closed: New databases can be added without modifying existing code
- Liskov Substitution: All dialects are interchangeable through the abstract interface
- Interface Segregation: Focused interface for SQL generation concerns only
- Dependency Inversion: SQLQueryBuilder depends on abstract SQLDialect

Supported Databases:
- PostgreSQL: Standard SQL with explicit column lists
- Snowflake: Similar to PostgreSQL with some extensions
- BigQuery: Supports SELECT * REPLACE syntax
- DuckDB: Supports SELECT * REPLACE syntax
- Databricks: Spark SQL dialect
- Trino: Standard SQL similar to PostgreSQL
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List, Optional, Tuple


class DatabaseType(Enum):
    """Supported database types."""
    POSTGRES = "postgres"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    DUCKDB = "duckdb"
    DATABRICKS = "databricks"
    TRINO = "trino"
    UNKNOWN = "unknown"

    @classmethod
    def from_string(cls, db_type: str) -> "DatabaseType":
        """Convert string to DatabaseType enum."""
        if not db_type:
            return cls.UNKNOWN
        db_type_lower = db_type.lower()
        for member in cls:
            if member.value == db_type_lower:
                return member
        return cls.UNKNOWN


class SQLDialect(ABC):
    """
    Abstract base class for database-specific SQL dialects.

    Each concrete implementation provides database-specific SQL syntax
    for operations that vary between database systems.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the dialect name (e.g., 'postgres', 'bigquery')."""
        pass

    @property
    def supports_select_replace(self) -> bool:
        """
        Whether this dialect supports SELECT * REPLACE (expr AS col) syntax.

        This is a DuckDB/BigQuery-specific feature. Most databases don't support it.
        Default: False
        """
        return False

    @property
    def supports_select_except(self) -> bool:
        """
        Whether this dialect supports SELECT * EXCEPT (col1, col2) syntax.

        This is a BigQuery/DuckDB-specific feature.
        Default: False
        """
        return False

    def quote_identifier(self, identifier: str) -> str:
        """
        Quote an identifier (table name, column name) for this dialect.

        Default uses double quotes which is ANSI SQL standard.
        Override for databases with different quoting (e.g., backticks for MySQL).
        """
        return f'"{identifier}"'

    def quote_schema_table(self, schema: str, table: str) -> str:
        """Quote and combine schema and table name."""
        if schema:
            return f'{self.quote_identifier(schema)}.{self.quote_identifier(table)}'
        return self.quote_identifier(table)

    def build_column_alias(self, expression: str, alias: str) -> str:
        """Build a column alias expression."""
        return f'{expression} AS {self.quote_identifier(alias)}'

    def build_replace_function(self, column: str, find: str, replace: str) -> str:
        """
        Build a REPLACE function call for string replacement.

        Default uses standard SQL REPLACE function.
        Override for databases with different syntax.
        """
        # Escape single quotes in find/replace values
        find_escaped = find.replace("'", "''") if find else ""
        replace_escaped = replace.replace("'", "''") if replace else ""
        return f"REPLACE({self.quote_identifier(column)}, '{find_escaped}', '{replace_escaped}')"

    def build_select_with_replacements(
        self,
        source_columns: List[str],
        replaced_columns: Dict[str, str],
        additional_columns: List[str],
        join_columns: List[str],
        cte_columns: List[str],
    ) -> str:
        """
        Build SELECT clause with column replacements.

        This is the key method that varies between databases:
        - Databases with SELECT * REPLACE: Use that syntax
        - Other databases: Build explicit column list

        Args:
            source_columns: Original columns from source table
            replaced_columns: Dict mapping column name to replacement expression
            additional_columns: Additional computed columns (synthesize, etc.)
            join_columns: Columns from joined tables (already aliased)
            cte_columns: Columns added by previous CTEs

        Returns:
            SELECT clause string (without SELECT keyword)
        """
        # Default implementation: explicit column list (PostgreSQL-compatible)
        return self._build_explicit_column_list(
            source_columns, replaced_columns, additional_columns, join_columns, cte_columns
        )

    def _build_explicit_column_list(
        self,
        source_columns: List[str],
        replaced_columns: Dict[str, str],
        additional_columns: List[str],
        join_columns: List[str],
        cte_columns: List[str],
    ) -> str:
        """
        Build explicit column list with replacements applied.

        This is PostgreSQL-compatible and works on all databases.
        """
        column_exprs = []

        # Add source columns (with replacements applied)
        for col in source_columns:
            if col in replaced_columns:
                column_exprs.append(f'{replaced_columns[col]} AS {self.quote_identifier(col)}')
            else:
                column_exprs.append(self.quote_identifier(col))

        # Add join columns (already aliased in CTE)
        for col in join_columns:
            if col in replaced_columns:
                column_exprs.append(f'{replaced_columns[col]} AS {self.quote_identifier(col)}')
            else:
                column_exprs.append(self.quote_identifier(col))

        # Add CTE-accumulated columns (synthesize, combine, window)
        for col in cte_columns:
            if col not in source_columns and col not in join_columns:
                if col in replaced_columns:
                    column_exprs.append(f'{replaced_columns[col]} AS {self.quote_identifier(col)}')
                else:
                    column_exprs.append(self.quote_identifier(col))

        # Add any additional columns from current state
        if additional_columns:
            column_exprs.extend(additional_columns)

        return ", ".join(column_exprs)

    def build_select_with_replacements_fallback(
        self,
        replaced_columns: Dict[str, str],
        additional_columns: List[str],
    ) -> str:
        """
        Build SELECT clause when source column metadata is not available.

        For databases that support SELECT * REPLACE, use that.
        For others, use * with replacement columns having _replaced suffix.
        """
        if self.supports_select_replace and replaced_columns:
            # Use SELECT * REPLACE syntax
            replace_parts = []
            for col, expr in replaced_columns.items():
                replace_parts.append(f'{expr} AS {self.quote_identifier(col)}')
            replace_clause = ", ".join(replace_parts)

            column_exprs = [f"* REPLACE ({replace_clause})"]
            if additional_columns:
                column_exprs.extend(additional_columns)
            return ", ".join(column_exprs)
        else:
            # Fallback: * plus replacement columns with _replaced suffix
            column_exprs = ["*"]
            for col, expr in replaced_columns.items():
                column_exprs.append(f'{expr} AS {self.quote_identifier(col + "_replaced")}')
            if additional_columns:
                column_exprs.extend(additional_columns)
            return ", ".join(column_exprs)

    def build_cte(self, cte_name: str, sql: str) -> str:
        """Build a CTE (Common Table Expression) definition."""
        return f'{self.quote_identifier(cte_name)} AS ({sql})'

    def build_with_clause(self, ctes: List[Tuple[str, str]]) -> str:
        """Build WITH clause from list of (name, sql) tuples."""
        if not ctes:
            return ""
        cte_defs = [self.build_cte(name, sql) for name, sql in ctes]
        return f"WITH {', '.join(cte_defs)}"


class PostgreSQLDialect(SQLDialect):
    """PostgreSQL-specific SQL dialect."""

    @property
    def name(self) -> str:
        return "postgres"


class SnowflakeDialect(SQLDialect):
    """Snowflake-specific SQL dialect."""

    @property
    def name(self) -> str:
        return "snowflake"

    # Snowflake uses double quotes for identifiers (same as default)
    # Snowflake doesn't support SELECT * REPLACE


class BigQueryDialect(SQLDialect):
    """BigQuery-specific SQL dialect."""

    @property
    def name(self) -> str:
        return "bigquery"

    @property
    def supports_select_replace(self) -> bool:
        return True

    @property
    def supports_select_except(self) -> bool:
        return True

    def quote_identifier(self, identifier: str) -> str:
        """BigQuery uses backticks for identifiers."""
        return f"`{identifier}`"


class DuckDBDialect(SQLDialect):
    """DuckDB-specific SQL dialect."""

    @property
    def name(self) -> str:
        return "duckdb"

    @property
    def supports_select_replace(self) -> bool:
        return True

    @property
    def supports_select_except(self) -> bool:
        return True


class DatabricksDialect(SQLDialect):
    """Databricks (Spark SQL) specific dialect."""

    @property
    def name(self) -> str:
        return "databricks"

    def quote_identifier(self, identifier: str) -> str:
        """Databricks uses backticks for identifiers."""
        return f"`{identifier}`"


class TrinoDialect(SQLDialect):
    """Trino-specific SQL dialect."""

    @property
    def name(self) -> str:
        return "trino"

    # Trino uses double quotes (ANSI standard)


class SQLDialectFactory:
    """
    Factory for creating SQL dialect instances.

    Usage:
        dialect = SQLDialectFactory.get_dialect("postgres")
        dialect = SQLDialectFactory.get_dialect(DatabaseType.BIGQUERY)
    """

    _dialects: Dict[DatabaseType, SQLDialect] = {}
    _initialized: bool = False

    @classmethod
    def _ensure_initialized(cls) -> None:
        """Initialize dialect registry."""
        if cls._initialized:
            return

        cls._dialects = {
            DatabaseType.POSTGRES: PostgreSQLDialect(),
            DatabaseType.SNOWFLAKE: SnowflakeDialect(),
            DatabaseType.BIGQUERY: BigQueryDialect(),
            DatabaseType.DUCKDB: DuckDBDialect(),
            DatabaseType.DATABRICKS: DatabricksDialect(),
            DatabaseType.TRINO: TrinoDialect(),
        }
        cls._initialized = True

    @classmethod
    def get_dialect(cls, db_type: Optional[str] = None) -> SQLDialect:
        """
        Get the appropriate SQL dialect for a database type.

        Args:
            db_type: Database type string (e.g., 'postgres', 'bigquery')
                    If None, returns PostgreSQL dialect as default.

        Returns:
            SQLDialect instance for the specified database
        """
        cls._ensure_initialized()

        if db_type is None:
            # Default to PostgreSQL (most common, safest)
            return cls._dialects[DatabaseType.POSTGRES]

        db_enum = DatabaseType.from_string(db_type)
        if db_enum == DatabaseType.UNKNOWN:
            # Unknown database - use PostgreSQL as safe default
            return cls._dialects[DatabaseType.POSTGRES]

        return cls._dialects.get(db_enum, cls._dialects[DatabaseType.POSTGRES])

    @classmethod
    def register_dialect(cls, db_type: DatabaseType, dialect: SQLDialect) -> None:
        """
        Register a custom dialect for a database type.

        This allows extending the factory with new databases without
        modifying the core code (Open/Closed principle).
        """
        cls._ensure_initialized()
        cls._dialects[db_type] = dialect
