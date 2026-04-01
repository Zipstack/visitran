import math

import ibis

from formulasql.utils.formulasql_utils import FormulaSQLUtils


class BaseLogics:

    @staticmethod
    def duplicate(table, node, data_types, inter_exps):
        params = node['inputs']
        if node['inputs'].__len__() != 1:
            raise Exception("DUPLICATE function requires 1 parameters")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        inter_exps[node['outputs'][0]] = e
        return e
    @staticmethod
    def isin(table, node, data_types, inter_exps):
        params = node['inputs']
        if node['inputs'].__len__() < 2:
            raise Exception("ISIN function requires atleast 2 parameters")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        arr: list = []
        for index, ele in enumerate(params[1:]):
            arr.append(FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, ele))
        return e.isin(arr)

    @staticmethod
    def notin(table, node, data_types, inter_exps):
        params = node['inputs']
        if node['inputs'].__len__() < 2:
            raise Exception("NOTIN function requires atleast 2 parameters")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        arr: list = []
        for index, ele in enumerate(params[1:]):
            arr.append(FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, ele))
        return e.notin(arr)
