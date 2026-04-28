from typing import Any, Optional

from backend.application.config_parser.base_parser import BaseParser
from backend.application.config_parser.transformation_parsers.filter_parser import FilterParser
from backend.application.interpreter.constants import Operators


class UnionColumnExpression(BaseParser):
    """Parser for column expression in a union branch."""

    @property
    def output_column(self) -> str:
        return self.get("output_column")

    @property
    def expression_type(self) -> str:
        """Returns COLUMN, LITERAL, or FORMULA."""
        return self.get("expression_type")

    @property
    def column_name(self) -> Optional[str]:
        """Returns column name for COLUMN type."""
        return self.get("column_name")

    @property
    def literal_value(self) -> Optional[str]:
        """Returns literal value for LITERAL type."""
        return self.get("literal_value")

    @property
    def literal_type(self) -> Optional[str]:
        """Returns literal data type for LITERAL type."""
        return self.get("literal_type", "String")

    @property
    def formula(self) -> Optional[str]:
        """Returns formula for FORMULA type."""
        return self.get("formula")

    @property
    def cast_type(self) -> Optional[str]:
        """Returns target cast type (e.g., VARCHAR, INTEGER, FLOAT)."""
        return self.get("cast_type")


class UnionBranchParser(BaseParser):
    """Parser for a single union branch."""

    def __init__(self, config_data: dict[str, Any]):
        super().__init__(config_data)
        self._filter_parser = None
        self._column_parsers: list[UnionColumnExpression] = []

    @property
    def branch_id(self) -> int:
        return self.get("branch_id")

    @property
    def table(self) -> str:
        return self.get("table")

    @property
    def schema(self) -> str:
        return self.get("schema")

    def get_column_expressions(self) -> list[UnionColumnExpression]:
        """Returns list of column expression parsers."""
        if not self._column_parsers:
            for col_expr in self.get("columns", []):
                self._column_parsers.append(UnionColumnExpression(col_expr))
        return self._column_parsers

    @property
    def filters(self) -> Optional[FilterParser]:
        """Returns FilterParser object for filters on this union branch.

        Converts union filter format to standard FilterParser criteria
        format.
        """
        raw_filters = self.get("filters")
        if not self._filter_parser and raw_filters:
            criteria = self._convert_filters_to_criteria(raw_filters)
            self._filter_parser = FilterParser({"criteria": criteria})
        return self._filter_parser

    def _convert_filters_to_criteria(self, filters: list) -> list:
        """Convert union filter format to standard FilterParser criteria
        format.

        Union format: [{
            "column": "col_name",
            "operator": "EQ|NEQ|GT|...",
            "rhs_type": "VALUE|COLUMN",
            "rhs_value": "val",
            "rhs_column": "col_name",
            "logical_operator": "AND|OR"
        }]

        FilterParser format: [{
            "condition": {
                "lhs": {"type": "COLUMN", "column": {"column_name": "col_name"}},
                "operator": "EQ",
                "rhs": {"type": "VALUE", "value": "val"} or {"type": "COLUMN", "column": {"column_name": "col_name"}}
            },
            "logical_operator": "AND|OR"
        }]
        """
        criteria = []
        for filter_spec in filters:
            operator = filter_spec.get("operator")
            condition = {
                "condition": {
                    "lhs": {
                        "type": "COLUMN",
                        "column": {
                            "column_name": filter_spec.get("column"),
                            "data_type": filter_spec.get("column_type", "String")
                        }
                    },
                    "operator": operator,
                    "rhs": {}
                },
                "logical_operator": filter_spec.get("logical_operator", "AND")
            }

            # For TRUE, FALSE, NULL, NOTNULL operators, don't set rhs
            if operator not in Operators.NO_RHS_OPERATORS:
                # Handle rhs based on type
                rhs_type = filter_spec.get("rhs_type", "VALUE")
                if rhs_type == "COLUMN":
                    condition["condition"]["rhs"] = {
                        "type": "COLUMN",
                        "column": {
                            "column_name": filter_spec.get("rhs_column")
                        }
                    }
                else:  # VALUE
                    condition["condition"]["rhs"] = {
                        "type": "VALUE",
                        "value": filter_spec.get("rhs_value") or filter_spec.get("value")
                    }

            criteria.append(condition)

        return criteria


class UnionParser(BaseParser):
    def __init__(self, config_data: dict[str, Any]):
        super().__init__(config_data)
        self._filter_parser = None

    @property
    def source_table(self) -> str:
        return self.get("source_table")

    @property
    def source_column(self) -> str:
        return self.get("source_column")

    @property
    def merge_table(self) -> str:
        return self.get("merge_table")

    @property
    def merge_schema(self) -> str:
        """Return merge table schema, default empty string."""
        return self.get("merge_schema", "") or ""

    @property
    def merge_column(self) -> str:
        return self.get("merge_column")

    @property
    def column_type(self) -> str:
        return self.get("column_type")

    @property
    def column_dbtype(self) -> str:
        return self.get("column_dbtype")

    @property
    def filters(self) -> Optional[FilterParser]:
        """Returns FilterParser object for filters on this union branch.

        Converts union filter format to standard FilterParser criteria
        format.
        """
        if not self._filter_parser and self.get("filters"):
            criteria = self._convert_filters_to_criteria(self.get("filters"))
            self._filter_parser = FilterParser({"criteria": criteria})
        return self._filter_parser

    def _convert_filters_to_criteria(self, filters: list) -> list:
        """Convert union filter format to standard FilterParser criteria
        format.

        Union format: [{
            "column": "col_name",
            "operator": "EQ|NEQ|GT|...",
            "rhs_type": "VALUE|COLUMN",
            "rhs_value": "val",
            "rhs_column": "col_name",
            "logical_operator": "AND|OR"
        }]

        FilterParser format: [{
            "condition": {
                "lhs": {"type": "COLUMN", "column": {"column_name": "col_name"}},
                "operator": "EQ",
                "rhs": {"type": "VALUE", "value": "val"} or {"type": "COLUMN", "column": {"column_name": "col_name"}}
            },
            "logical_operator": "AND|OR"
        }]
        """
        criteria = []
        for filter_spec in filters:
            operator = filter_spec.get("operator")
            condition = {
                "condition": {
                    "lhs": {
                        "type": "COLUMN",
                        "column": {
                            "column_name": filter_spec.get("column"),  # Fixed: use column_name not name
                            "data_type": filter_spec.get("column_type", "String")
                        }
                    },
                    "operator": operator,
                    "rhs": {}
                },
                "logical_operator": filter_spec.get("logical_operator", "AND")
            }

            # For TRUE, FALSE, NULL, NOTNULL operators, don't set rhs
            if operator not in Operators.NO_RHS_OPERATORS:
                # Handle rhs based on type
                rhs_type = filter_spec.get("rhs_type", "VALUE")
                if rhs_type == "COLUMN":
                    condition["condition"]["rhs"] = {
                        "type": "COLUMN",
                        "column": {
                            "column_name": filter_spec.get("rhs_column")  # Fixed: use column_name not name
                        }
                    }
                else:  # VALUE
                    condition["condition"]["rhs"] = {
                        "type": "VALUE",
                        "value": filter_spec.get("rhs_value") or filter_spec.get("value")
                    }

            criteria.append(condition)

        return criteria


class UnionParsers(BaseParser):
    def __init__(self, config_data: dict[str, Any]):
        super().__init__(config_data)
        self._union_parser: list[UnionParser] = []
        self._branch_parsers: list[UnionBranchParser] = []
        self._union_columns: list[str] = []
        self._source_filter_parser = None

    def is_branch_based(self) -> bool:
        """Check if this is the new branch-based union format."""
        # New format has 'branches' key (output_columns is optional)
        return "branches" in self._config_data

    def get_union_parsers(self) -> list[UnionParser]:
        """Returns table-based union parsers (legacy format)."""
        if not self._union_parser:
            for union in self.get("tables", []):
                self._union_parser.append(UnionParser(union))
        return self._union_parser

    def get_branch_parsers(self) -> list[UnionBranchParser]:
        """Returns branch-based union parsers (new format)."""
        if not self._branch_parsers:
            for branch in self.get("branches", []):
                self._branch_parsers.append(UnionBranchParser(branch))
        return self._branch_parsers

    @property
    def output_columns(self) -> list[dict]:
        """Returns output column definitions for branch-based unions."""
        return self.get("output_columns", [])

    @property
    def column_names(self) -> list[str]:
        """Returns column names (legacy format compatibility)."""
        if not self._union_columns:
            if self.is_branch_based():
                # For branch-based, return output column names
                self._union_columns = [col.get("column_name") for col in self.output_columns]
            else:
                # For table-based, return source column names
                for union_parser in self.get_union_parsers():
                    self._union_columns.append(union_parser.source_column)
        return self._union_columns

    @property
    def unions_duplicate(self) -> bool:
        return self.get("ignore_duplicate", False)

    @property
    def source_filters(self) -> Optional[FilterParser]:
        """Returns FilterParser object for source-level filters.

        Converts union filter format to standard FilterParser criteria
        format.
        """
        raw_filters = self.get("source_filters")
        if not self._source_filter_parser and raw_filters:
            criteria = self._convert_filters_to_criteria(raw_filters)
            self._source_filter_parser = FilterParser({"criteria": criteria})
        return self._source_filter_parser

    def _convert_filters_to_criteria(self, filters: list) -> list:
        """Convert union filter format to standard FilterParser criteria
        format.

        Union format: [{
            "column": "col_name",
            "operator": "EQ|NEQ|GT|...",
            "rhs_type": "VALUE|COLUMN",
            "rhs_value": "val",
            "rhs_column": "col_name",
            "logical_operator": "AND|OR"
        }]

        FilterParser format: [{
            "condition": {
                "lhs": {"type": "COLUMN", "column": {"column_name": "col_name"}},
                "operator": "EQ",
                "rhs": {"type": "VALUE", "value": "val"} or {"type": "COLUMN", "column": {"column_name": "col_name"}}
            },
            "logical_operator": "AND|OR"
        }]
        """
        criteria = []
        for filter_spec in filters:
            operator = filter_spec.get("operator")
            condition = {
                "condition": {
                    "lhs": {
                        "type": "COLUMN",
                        "column": {
                            "column_name": filter_spec.get("column"),
                            "data_type": filter_spec.get("column_type", "String")
                        }
                    },
                    "operator": operator,
                    "rhs": {}
                },
                "logical_operator": filter_spec.get("logical_operator", "AND")
            }

            # For TRUE, FALSE, NULL, NOTNULL operators, don't set rhs
            if operator not in Operators.NO_RHS_OPERATORS:
                # Handle rhs based on type
                rhs_type = filter_spec.get("rhs_type", "VALUE")
                if rhs_type == "COLUMN":
                    condition["condition"]["rhs"] = {
                        "type": "COLUMN",
                        "column": {
                            "column_name": filter_spec.get("rhs_column")
                        }
                    }
                else:  # VALUE
                    condition["condition"]["rhs"] = {
                        "type": "VALUE",
                        "value": filter_spec.get("rhs_value") or filter_spec.get("value")
                    }

            criteria.append(condition)

        return criteria
