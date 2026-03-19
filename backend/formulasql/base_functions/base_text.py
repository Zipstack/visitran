from formulasql.utils.formulasql_utils import FormulaSQLUtils

# Map common user-facing type names to ibis-compatible type strings
_TYPE_ALIASES = {
    "TEXT": "string",
    "VARCHAR": "string",
    "STRING": "string",
    "CHAR": "string",
    "INT": "int64",
    "INTEGER": "int64",
    "BIGINT": "int64",
    "SMALLINT": "int16",
    "TINYINT": "int8",
    "FLOAT": "float64",
    "DOUBLE": "float64",
    "DECIMAL": "decimal",
    "NUMERIC": "float64",
    "REAL": "float32",
    "BOOL": "boolean",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
    "DATETIME": "timestamp",
    "TIME": "time",
}


class BaseText:
    @staticmethod
    def cast(table, node, data_types, inter_exps):
        if node["inputs"].__len__() == 2:
            e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
            cast_to = node["inputs"][1].replace('"', "").strip()
            # Resolve common type aliases to ibis-compatible names
            cast_to = _TYPE_ALIASES.get(cast_to.upper(), cast_to)
            e1 = e1.cast(cast_to)
        else:
            raise Exception("CAST function requires 2 parameters")
        data_types[node["outputs"][0]] = cast_to
        return e1
