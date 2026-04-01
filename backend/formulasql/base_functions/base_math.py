import math

import ibis

from formulasql.utils.formulasql_utils import FormulaSQLUtils



class BaseMath:

    @staticmethod
    def gestep(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 1:
            e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
            e2 = ibis.literal(0)
        elif node['inputs'].__len__() == 2:
            e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
            e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
        else:
            raise Exception("GESTEP function requires 1 or 2 parameters")

        e = (e1 >= e2).ifelse(1, 0)
        data_types[node['outputs'][0]] = 'numeric'
        return e

