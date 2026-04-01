import ibis
import ibis.expr.datatypes as dt

from formulasql.utils.constants import IbisDataType
from formulasql.utils.formulasql_utils import FormulaSQLUtils


class Operators:

    def __init__(self): pass

    @staticmethod
    def division(table, node, data_types, inter_exps):
        params = node['inputs']
        p1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])
        p2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[1])
        return p1 / p2.nullif(0)

    @staticmethod
    def modulus(table, node, data_types, inter_exps):
        params = node['inputs']
        p1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])
        p2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[1])
        return p1 % p2

    @staticmethod
    def multiplication(table, node, data_types, inter_exps):
        params = node['inputs']
        p1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])
        p2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[1])
        return p1 * p2

    @staticmethod
    def addition(table, node, data_types, inter_exps):
        params = node['inputs']
        p1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])
        p2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[1])
        if isinstance(p1.type(), IbisDataType.TEMPORAL_TYPES) and isinstance(p2.type(), IbisDataType.NUMERIC_TYPES):
            return p1 + p2.cast(dt.Interval("d"))
        elif isinstance(p2.type(), IbisDataType.TEMPORAL_TYPES) and isinstance(p1.type(), IbisDataType.NUMERIC_TYPES):
            return p2 + p1.cast(dt.Interval("d"))
        return p1 + p2

    @staticmethod
    def addition_u(table, node, data_types, inter_exps):
        params = node['inputs']
        p1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])
        return p1

    @staticmethod
    def subtraction(table, node, data_types, inter_exps):
        params = node['inputs']
        p1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])
        p2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[1])
        if isinstance(p1.type(), IbisDataType.TEMPORAL_TYPES) and isinstance(p2.type(), IbisDataType.NUMERIC_TYPES):
            return p1 - p2.cast(dt.Interval("d"))
        elif isinstance(p2.type(), IbisDataType.TEMPORAL_TYPES) and isinstance(p1.type(), IbisDataType.NUMERIC_TYPES):
            return p2 - p1.cast(dt.Interval("d"))
        elif isinstance(p1.type(), IbisDataType.TEMPORAL_TYPES) and isinstance(p2.type(), IbisDataType.TEMPORAL_TYPES):
            res = p1.cast(ibis.expr.datatypes.core.Date) - p2.cast(ibis.expr.datatypes.core.Date)
            return res.cast("int64")
        return p1 - p2

    @staticmethod
    def subtraction_u(table, node, data_types, inter_exps):
        params = node['inputs']
        p1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])
        return -p1

    @staticmethod
    def ampersand(table, node, data_types, inter_exps):
        params = node['inputs']
        p1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0]).cast('string')
        p2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[1]).cast('string')
        return p1.concat(p2)

    @staticmethod
    def equal(table, node, data_types, inter_exps):
        params = node['inputs']
        p1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])
        p2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[1])
        data_types[node['outputs'][0]] = 'boolean'
        return p1 == p2

    @staticmethod
    def not_equal(table, node, data_types, inter_exps):
        params = node['inputs']
        p1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])
        p2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[1])
        data_types[node['outputs'][0]] = 'boolean'
        return p1 != p2

    @staticmethod
    def greater_than(table, node, data_types, inter_exps):
        params = node['inputs']
        p1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])
        p2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[1])
        data_types[node['outputs'][0]] = 'boolean'
        return p1 > p2

    @staticmethod
    def greater_than_or_equal(table, node, data_types, inter_exps):
        params = node['inputs']
        p1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])
        p2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[1])
        data_types[node['outputs'][0]] = 'boolean'
        return p1 >= p2

    @staticmethod
    def less_than(table, node, data_types, inter_exps):
        params = node['inputs']
        p1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])
        p2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[1])
        data_types[node['outputs'][0]] = 'boolean'
        return p1 < p2

    @staticmethod
    def less_than_or_equal(table, node, data_types, inter_exps):
        params = node['inputs']
        p1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])
        p2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[1])
        data_types[node['outputs'][0]] = 'boolean'
        return p1 <= p2

    @staticmethod
    def and_(table, node, data_types, inter_exps):
        params = node['inputs']
        p1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])
        p2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[1])
        data_types[node['outputs'][0]] = 'boolean'
        return p1 & p2

    @staticmethod
    def or_(table, node, data_types, inter_exps):
        params = node['inputs']
        p1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])
        p2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[1])
        data_types[node['outputs'][0]] = 'boolean'
        return p1 | p2

    @staticmethod
    def not_(table, node, data_types, inter_exps):
        params = node['inputs']
        p1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])
        data_types[node['outputs'][0]] = 'boolean'
        return ~p1

    @staticmethod
    def xor_(table, node, data_types, inter_exps):
        params = node['inputs']
        p1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])
        p2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[1])
        data_types[node['outputs'][0]] = 'boolean'
        return p1 ^ p2

    @staticmethod
    def true(table, node, data_types, inter_exps, params):
        data_types[node['outputs'][0]] = 'boolean'
        return ibis.literal(True)

    @staticmethod
    def false(table, node, data_types, inter_exps, params):
        data_types[node['outputs'][0]] = 'boolean'
        return ibis.literal(False)

    @staticmethod
    def null(table, node, data_types, inter_exps, params):
        data_types[node['outputs'][0]] = 'null'
        return ibis.literal(None)
