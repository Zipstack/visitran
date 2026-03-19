import ibis

from formulasql.utils.formulasql_utils import FormulaSQLUtils

try:
    from abc import ABC as Base
except:
    from abc import ABC as Base


class Window(Base):
    """Window functions for analytics operations like LAG, LEAD, RANK, etc."""

    def __init__(self):
        pass

    @staticmethod
    def lag(table, node, data_types, inter_exps):
        """Returns the value from a previous row within a partition."""
        if len(node['inputs']) < 1 or len(node['inputs']) > 3:
            raise Exception("LAG function requires 1 to 3 parameters: LAG(column, [offset], [default])")

        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])

        offset = 1
        if len(node['inputs']) >= 2:
            offset_expr = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
            if hasattr(offset_expr, 'op') and hasattr(offset_expr.op(), 'value'):
                offset = int(offset_expr.op().value)
            else:
                offset = int(node['inputs'][1])

        default = None
        if len(node['inputs']) >= 3:
            default = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][2])

        e = e.lag(offset, default=default)
        data_types[node['outputs'][0]] = data_types.get(node['inputs'][0], 'numeric')
        return e

    @staticmethod
    def lead(table, node, data_types, inter_exps):
        """Returns the value from a subsequent row within a partition."""
        if len(node['inputs']) < 1 or len(node['inputs']) > 3:
            raise Exception("LEAD function requires 1 to 3 parameters: LEAD(column, [offset], [default])")

        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])

        offset = 1
        if len(node['inputs']) >= 2:
            offset_expr = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
            if hasattr(offset_expr, 'op') and hasattr(offset_expr.op(), 'value'):
                offset = int(offset_expr.op().value)
            else:
                offset = int(node['inputs'][1])

        default = None
        if len(node['inputs']) >= 3:
            default = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][2])

        e = e.lead(offset, default=default)
        data_types[node['outputs'][0]] = data_types.get(node['inputs'][0], 'numeric')
        return e

    @staticmethod
    def cumsum(table, node, data_types, inter_exps):
        """Returns the cumulative sum."""
        if len(node['inputs']) != 1:
            raise Exception("CUMSUM function requires 1 parameter")

        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.cumsum()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def cummean(table, node, data_types, inter_exps):
        """Returns the cumulative mean."""
        if len(node['inputs']) != 1:
            raise Exception("CUMMEAN function requires 1 parameter")

        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.cummean()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def cummin(table, node, data_types, inter_exps):
        """Returns the cumulative minimum."""
        if len(node['inputs']) != 1:
            raise Exception("CUMMIN function requires 1 parameter")

        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.cummin()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def cummax(table, node, data_types, inter_exps):
        """Returns the cumulative maximum."""
        if len(node['inputs']) != 1:
            raise Exception("CUMMAX function requires 1 parameter")

        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.cummax()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def first(table, node, data_types, inter_exps):
        """Returns the first value in a window. Empty strings are treated as NULL."""
        if len(node['inputs']) != 1:
            raise Exception("FIRST function requires 1 parameter")

        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        # Convert empty strings to NULL before applying first()
        # so that empty values are returned as NULL, not ""
        if e.type().is_string():
            e = e.nullif(ibis.literal(''))
        e = e.first()
        data_types[node['outputs'][0]] = data_types.get(node['inputs'][0], 'numeric')
        return e

    @staticmethod
    def last(table, node, data_types, inter_exps):
        """Returns the last value in a window. Empty strings are treated as NULL."""
        if len(node['inputs']) != 1:
            raise Exception("LAST function requires 1 parameter")

        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        # Convert empty strings to NULL before applying last()
        # so that empty values are returned as NULL, not ""
        if e.type().is_string():
            e = e.nullif(ibis.literal(''))
        e = e.last()
        data_types[node['outputs'][0]] = data_types.get(node['inputs'][0], 'numeric')
        return e

    @staticmethod
    def row_number(table, node, data_types, inter_exps):
        """Returns row number starting from 1 within a window partition.

        Note: ibis.row_number() is 0-based, so we add 1 to match SQL standard.

        Usage:
            ROW_NUMBER() - numbers rows in default order
            ROW_NUMBER(order_column) - numbers rows ordered by the specified column
        """
        if len(node['inputs']) > 1:
            raise Exception("ROW_NUMBER function takes 0 or 1 parameter: ROW_NUMBER() or ROW_NUMBER(order_column)")

        # ibis.row_number() returns 0-based, add 1 for SQL-standard 1-based numbering
        e = ibis.row_number() + 1

        # If order column is provided, apply ordering
        if len(node['inputs']) == 1:
            order_col = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
            e = e.over(ibis.window(order_by=order_col))

        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def rank(table, node, data_types, inter_exps):
        """Returns rank with gaps for ties within a window partition.

        Note: ibis rank() is 0-based, so we add 1 to match SQL standard.
        """
        if len(node['inputs']) != 1:
            raise Exception("RANK function requires 1 parameter: RANK(column)")

        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        # ibis rank() returns 0-based, add 1 for SQL-standard 1-based ranking
        e = e.rank() + 1
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def dense_rank(table, node, data_types, inter_exps):
        """Returns rank without gaps for ties within a window partition.

        Note: ibis dense_rank() is 0-based, so we add 1 to match SQL standard.
        """
        if len(node['inputs']) != 1:
            raise Exception("DENSE_RANK function requires 1 parameter: DENSE_RANK(column)")

        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        # ibis dense_rank() returns 0-based, add 1 for SQL-standard 1-based ranking
        e = e.dense_rank() + 1
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def percent_rank(table, node, data_types, inter_exps):
        """Returns relative rank as a percentage (0 to 1) within a window
        partition.

        Formula: (rank - 1) / (total_rows - 1)
        Returns 0 for the first row, 1 for the last row in the partition.
        """
        if len(node['inputs']) != 1:
            raise Exception("PERCENT_RANK function requires 1 parameter: PERCENT_RANK(column)")

        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.percent_rank()
        data_types[node['outputs'][0]] = 'numeric'
        return e
