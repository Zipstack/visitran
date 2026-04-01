import ibis


class FormulaSQLUtils:

    def __init__(self):
        pass

    @staticmethod
    def _num(s):
        try:
            int_val = int(s)
            return ibis.literal(int_val).cast("int32")
        except ValueError:
            float_val = float(s)
            return ibis.literal(float_val)

    @staticmethod
    def build_ibis_expression(table, data_types, inter_exps, p):
        if data_types[p] == "numeric":
            return FormulaSQLUtils._num(p)
        if data_types[p] == "none":
            return ibis.literal(None)
        elif data_types[p] == "string":
            p = p.replace('"', "")
            return ibis.literal(p)
        elif data_types[p] == "boolean":
            return ibis.literal(str(p).lower() == "true")
        elif data_types[p] == "column":
            for _exp in inter_exps:
                if p.lower() == _exp.lower():
                    return inter_exps[_exp]

            _columns = table.columns
            for _column in _columns:
                if _column.lower() == p.lower():
                    return table[_column]
            return table[p.lower()]

    @staticmethod
    def build_string_ibis_constant_exp(p):
        p = p.replace('"', "")
        return ibis.literal(p)
