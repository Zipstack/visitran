import math

import ibis

from formulasql.utils.constants import IbisDataType
from formulasql.utils.formulasql_utils import FormulaSQLUtils
from formulasql.functions.datetime import _bq_timestamp_cast

try:
    from formulasql.base_functions.base_math import BaseMath as Base
except:
    from abc import ABC as Base



class Math(Base):

    def __init__(self):
        pass

    @staticmethod
    def __num(s):
        try:
            return int(s)
        except ValueError:
            return float(s)

    @staticmethod
    def abs(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("ABS function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.abs()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def acos(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("ACOS function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.acos()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def asin(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("ASIN function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.asin()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def atan(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("ATAN function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.atan()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def atan2(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 2:
            raise Exception("ATAN2 function requires 2 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
        e = e1.atan2(e2)
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def bitand(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 2:
            raise Exception("BITAND function requires 2 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
        e = e1.__and__(e2)
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def bitor(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 2:
            raise Exception("BITOR function requires 2 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
        e = e1.__or__(e2)
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def bitxor(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 2:
            raise Exception("BITXOR function requires 2 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
        e = e1.__xor__(e2)
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def bitlshift(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 2:
            raise Exception("BITLSHIFT function requires 2 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
        # Use multiplication by power of 2 instead of << operator
        # which is not supported on all backends (e.g. PostgreSQL bigint)
        e = (e1 * (ibis.literal(2) ** e2)).cast('int64')
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def bitrshift(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 2:
            raise Exception("BITRSHIFT function requires 2 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
        # Use floor division by power of 2 instead of >> operator
        # which is not supported on all backends (e.g. PostgreSQL bigint)
        e = (e1 / (ibis.literal(2) ** e2)).floor().cast('int64')
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def ceiling(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("CEILING function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.ceil()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def cos(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("COS function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.cos()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def cot(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("COT function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.cot()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def degrees(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("DEGREES function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.degrees()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def delta(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 2:
            raise Exception("DELTA function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
        e = (e == e2).ifelse(1, 0)
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def even(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("EVEN function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.ceil().cast('int')
        e = (e >= 0).ifelse(e + e.__mod__(2), e - e.__mod__(2))
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def odd(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("ODD function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.ceil().cast('int')
        e = (e >= 0).ifelse(e + (1 - e.__mod__(2)), e - (1 - e.__mod__(2)))
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def exp(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("EXP function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.exp()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def floor(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("FLOOR function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.floor()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def int_(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("INT function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.cast('int').floor()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def ln(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("LN function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.ln()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def log(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 1:
            e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
            e2 = ibis.literal(10)
        elif node['inputs'].__len__() == 2:
            e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
            e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
        else:
            raise Exception("LOG function requires 1 or 2 parameters")

        e = e1.log(e2)
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def log10(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("LOG10 function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.log10()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def max_(table, node, data_types, inter_exps):
        inputs = node['inputs']
        if len(inputs) == 0:
            raise Exception("MAX function requires at least 1 parameter")

        if len(inputs) == 1:
            expr = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, inputs[0])
            maximum = expr.max()
            data_types[node['outputs'][0]] = maximum
            return maximum

        maximum = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        for n in node['inputs'][1:]:
            second_maximum = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, n)
            maximum = ibis.greatest(maximum, second_maximum)

        inter_exps[node['outputs'][0]] = maximum
        return maximum

    @staticmethod
    def min_(table, node, data_types, inter_exps):
        inputs = node['inputs']
        if len(inputs) == 0:
            raise Exception("MIN function requires at least 1 parameter")

        if len(inputs) == 1:
            expr = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, inputs[0])
            minimum = expr.min()
            data_types[node['outputs'][0]] = minimum
            return minimum

        minimum = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        for n in node['inputs'][1:]:
            second_minimum = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, n)
            minimum = ibis.least(minimum, second_minimum)

        data_types[node['outputs'][0]] = minimum
        return minimum

    @staticmethod
    def mod(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 2:
            raise Exception("MOD function requires 2 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
        e = e1.__mod__(e2)
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def modulus(table, node, data_types, inter_exps):
        return Math.mod(table, node, data_types, inter_exps)

    @staticmethod
    def pi(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("PI function requires no parameters")
        e = ibis.literal(math.pi)
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def power(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 2:
            raise Exception("POWER function requires 2 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
        e = e1 ** e2
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def product(table, node, data_types, inter_exps):
        if node['inputs'].__len__() < 2:
            raise Exception("PRODUCT function requires atleast 2 parameters")

        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        for n in node['inputs'][1:]:
            e = e * FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, n)

        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def quotient(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 2:
            raise Exception("QUOTIENT function requires 2 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
        e = e1 // e2
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def radians(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("RADIANS function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.radians()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def round(table, node, data_types, inter_exps):
        if len(node["inputs"]) != 2:
            raise Exception("ROUND function requires exactly 2 parameters")

        # Build ibis expressions
        e1 = FormulaSQLUtils.build_ibis_expression(
            table, data_types, inter_exps, node["inputs"][0]
        )
        e2 = FormulaSQLUtils.build_ibis_expression(
            table, data_types, inter_exps, node["inputs"][1]
        )

        # Force floating-point math to avoid BIGINT / DECIMAL overflow
        try:
            e1 = e1.cast("double")
        except Exception:
            pass

        # Handle NULL / invalid round digits and ensure Postgres-safe INTEGER
        try:
            e2 = e2.coalesce(0).cast("int32").clip(-15, 15)
        except Exception:
            e2 = 0

        # Apply ROUND safely
        try:
            e = e1.round(e2)
        except Exception:
            e = e1.round(0)

        data_types[node["outputs"][0]] = "numeric"
        return e

    @staticmethod
    def rounddown(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 2:
            raise Exception("ROUNDDOWN function requires 2 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
        place = ibis.literal(10) ** e2
        e = (e1 * place).floor() / place
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def roundup(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 2:
            raise Exception("ROUNDUP function requires 2 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
        place = ibis.literal(10) ** e2
        e = (e1 * place).ceil() / place
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def sign(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("SIGN function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.sign()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def sin(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("SIN function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.sin()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def sqrt(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("SQRT function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.sqrt()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def sqrtpi(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("SQRTPI function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.sqrt() * ibis.literal(math.pi)
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def sum(table, node, data_types, inter_exps):
        if node['inputs'].__len__() < 2:
            raise Exception("SUM function requires atleast 2 parameters")

        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        for n in node['inputs'][1:]:
            e = e + FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, n)

        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def sumsq(table, node, data_types, inter_exps):
        if node['inputs'].__len__() < 2:
            raise Exception("SUMSQ function requires atleast 2 parameters")

        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        for n in node['inputs'][1:]:
            e = e + FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, n) ** 2

        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def tan(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("TAN function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.tan()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def trunc(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 2:
            raise Exception("TRUNC function requires 2 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
        place = ibis.literal(10) ** e2
        e = (e1 < 0).ifelse((e1 * place).ceil() / place, (e1 * place).floor() / place)
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def average(table, node, data_types, inter_exps):
        if node['inputs'].__len__() < 2:
            raise Exception("AVERAGE function requires atleast 2 parameters")

        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        for n in node['inputs'][1:]:
            e = e + FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, n)

        e = e / node['inputs'].__len__()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def difference(table, node, data_types, inter_exps):
        if len(node['inputs']) != 2:
            raise Exception("DIFFERENCE function requires 2 parameters")

        col1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        col2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])

        col1_type = col1.type()
        col2_type = col2.type()

        # Case 1: Numeric difference
        if isinstance(col1_type, IbisDataType.NUMERIC_TYPES) and isinstance(col2_type, IbisDataType.NUMERIC_TYPES):
            col = col1 - col2

        # Case 2: Timestamp - Timestamp
        elif isinstance(col1_type, IbisDataType.TIMESTAMP) and isinstance(col2_type, IbisDataType.TIMESTAMP):
            col1 = col1.cast("int")
            col2 = col2.cast("int")
            col = (col1 - col2).cast("float") / 86400  # convert seconds → days

        # Case 3: Date - Date
        elif isinstance(col1_type, IbisDataType.DATE) and isinstance(col2_type, IbisDataType.DATE):
            col = (col1 - col2).cast("int")

        # Case 4: Date - Timestamp (mixed types)
        elif ((isinstance(col1_type, IbisDataType.DATE) and isinstance(col2_type, IbisDataType.TIMESTAMP))
              or isinstance(col1_type, IbisDataType.TIMESTAMP) and isinstance(col2_type, IbisDataType.DATE)):
            # Promote date → timestamp to align types
            col1 = _bq_timestamp_cast(table, col1)
            col2 = _bq_timestamp_cast(table, col2)
            col1 = col1.cast("int")
            col2 = col2.cast("int")
            col = (col1 - col2).cast("float") / 86400

        # Unsupported
        else:
            first_param = node['inputs'][0]
            second_param = node['inputs'][1]
            raise Exception(
                f'The datatype of columns "{first_param}", "{second_param}" are not supported for DIFFERENCE.')

        data_types[node['outputs'][0]] = 'int'
        return col

    # =========================================================================
    # Statistical Functions
    # =========================================================================

    @staticmethod
    def median(table, node, data_types, inter_exps):
        """Returns the median value.

        PostgreSQL does not support percentile_cont with OVER (window).
        We eagerly compute the scalar aggregate and return it as a
        literal to broadcast across all rows.
        """
        if len(node['inputs']) != 1:
            raise Exception("MEDIAN function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        # Compute the median as a scalar aggregate to avoid PostgreSQL's
        # "OVER is not supported for ordered-set aggregate" error.
        # Execute the aggregate query and return the result as a literal.
        try:
            result = table.aggregate(_median_result=e.median()).execute()
            median_val = result['_median_result'].iloc[0]
            e = ibis.literal(median_val)
        except Exception:
            # Fallback: direct method (works on DuckDB and other backends
            # that support percentile_cont with OVER)
            e = e.median()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def quantile(table, node, data_types, inter_exps):
        """Returns the value at a given quantile (0-1).

        PostgreSQL does not support percentile_cont with OVER (window).
        We eagerly compute the scalar aggregate and return it as a
        literal.
        """
        if len(node['inputs']) != 2:
            raise Exception("QUANTILE function requires 2 parameters: QUANTILE(column, quantile)")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        q = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])

        if hasattr(q, 'op') and hasattr(q.op(), 'value'):
            q_val = float(q.op().value)
        else:
            q_val = float(node['inputs'][1])

        # Compute the quantile as a scalar aggregate to avoid PostgreSQL's
        # "OVER is not supported for ordered-set aggregate" error.
        try:
            result = table.aggregate(_quantile_result=e.quantile(q_val)).execute()
            quantile_val = result['_quantile_result'].iloc[0]
            e = ibis.literal(quantile_val)
        except Exception:
            # Fallback: direct method (works on DuckDB, etc.)
            e = e.quantile(q_val)
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def variance(table, node, data_types, inter_exps):
        """Returns the variance of a column."""
        if len(node['inputs']) != 1:
            raise Exception("VARIANCE function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.var()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def stddev(table, node, data_types, inter_exps):
        """Returns the standard deviation of a column."""
        if len(node['inputs']) != 1:
            raise Exception("STDDEV function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.std()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def cov(table, node, data_types, inter_exps):
        """Returns the covariance between two columns."""
        if len(node['inputs']) != 2:
            raise Exception("COV function requires 2 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
        e = e1.cov(e2)
        data_types[node['outputs'][0]] = 'numeric'
        return e

    # =========================================================================
    # Additional Numeric Functions
    # =========================================================================

    @staticmethod
    def log2(table, node, data_types, inter_exps):
        """Returns the base-2 logarithm of a number."""
        if len(node['inputs']) != 1:
            raise Exception("LOG2 function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.log2()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def clip(table, node, data_types, inter_exps):
        """Clips values to be within a specified range."""
        if len(node['inputs']) != 3:
            raise Exception("CLIP function requires 3 parameters: CLIP(value, lower, upper)")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        lower = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
        upper = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][2])
        e = e.clip(lower, upper)
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def negate(table, node, data_types, inter_exps):
        """Returns the negation of a number."""
        if len(node['inputs']) != 1:
            raise Exception("NEGATE function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.negate()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def random(table, node, data_types, inter_exps):
        """Returns a random float between 0 and 1."""
        if len(node['inputs']) != 1:
            raise Exception("RANDOM function requires 0 parameters")
        e = ibis.random()
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def e(table, node, data_types, inter_exps):
        """Returns Euler's number (approximately 2.71828)."""
        if len(node['inputs']) != 1:
            raise Exception("E function requires 0 parameters")
        e = ibis.literal(math.e)
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def greatest(table, node, data_types, inter_exps):
        """Returns the greatest value among the arguments."""
        if len(node['inputs']) < 2:
            raise Exception("GREATEST function requires at least 2 parameters")

        exprs = [FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, inp)
                 for inp in node['inputs']]
        e = ibis.greatest(*exprs)
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def least(table, node, data_types, inter_exps):
        """Returns the least value among the arguments."""
        if len(node['inputs']) < 2:
            raise Exception("LEAST function requires at least 2 parameters")

        exprs = [FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, inp)
                 for inp in node['inputs']]
        e = ibis.least(*exprs)
        data_types[node['outputs'][0]] = 'numeric'
        return e
