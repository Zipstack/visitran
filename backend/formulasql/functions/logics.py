import ibis
from ibis import null

from formulasql.utils.formulasql_utils import FormulaSQLUtils

try:
    from formulasql.base_functions.base_logics import BaseLogics as Base
except:
    from abc import ABC as Base


def ensure_typed_null(expr, fallback_type):
    """Checking for "empty" false/true branches if typed NULL the we sould to
    cast them.

    Treats:
      - None
      - ibis.NA / null()
      - empty-string literal (ibis.literal(''))
    as a typed NULL with the fallback_type.
    """
    # direct None or ibis null()
    if expr is None or (hasattr(expr, "equals") and expr.equals(null())):
        return ibis.literal(None).cast(fallback_type)

    try:
        empty_literal = ibis.literal("")
        if hasattr(expr, "equals") and expr.equals(empty_literal):
            return ibis.literal(None).cast(fallback_type)
    except Exception as e:
        print("cast convertion and fallback failed to load, returning default expression")
        print(fallback_type, e)
        pass

    return expr


class Logics(Base):

    @staticmethod
    def if_(table, node, data_types, inter_exps):
        params = node["inputs"]
        if node["inputs"].__len__() != 3:
            raise Exception("IF function requires 3 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[1])
        e3 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[2])

        # Infer types where possible
        if e2 is not None and not e2.equals(null()):
            e3 = ensure_typed_null(e3, e2.type())
        elif e3 is not None and not e3.equals(null()):
            e2 = ensure_typed_null(e2, e3.type())
        else:
            raise ValueError("Cannot infer type for IF expression because both true/false values are NULL")

        return e1.ifelse(e2, e3.cast(e2.type()))

    @staticmethod
    def ifna(table, node, data_types, inter_exps):
        params = node["inputs"]
        if node["inputs"].__len__() != 2:
            raise Exception("IFNA function requires 2 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[1])
        ex = e1 == ibis.NA

        return ex.ifelse(e2.cast(e1.type()), e1)

    @staticmethod
    def ifs(table, node, data_types, inter_exps):
        params = node["inputs"]

        # Handle Excel-style TRUE, "Other"
        if len(params) >= 4 and len(params) % 2 == 0:
            second_last = params[-2]
            if isinstance(second_last, str) and second_last.strip().upper() == "TRUE":
                params = params[:-2] + [params[-1]]

        default_value = params[-1]
        if default_value == "NONE":
            default_expr = ibis.NA
        elif default_value.upper() == "NULL":
            default_expr = ibis.NA
        elif default_value.upper() in ("TRUE", "FALSE"):
            default_expr = bool(default_value.upper() == "TRUE")
        elif default_value in inter_exps:
            default_expr = inter_exps[default_value]
        elif default_value.__contains__("'") or default_value.__contains__('"'):
            dv = default_value.strip('"').strip("'")
            if dv.replace(".", "", 1).isdigit():
                default_expr = ibis.literal(float(dv))
            else:
                default_expr = ibis.literal(dv)
        else:
            default_expr = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, default_value)

        e = default_expr
        pairs = params[:-1]

        for i in range(len(pairs) - 2, -1, -2):
            cond_expr_str = pairs[i]
            true_expr_str = pairs[i + 1]

            cond = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, cond_expr_str)

            try:
                true_val = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, true_expr_str)
            except Exception:
                tv = str(true_expr_str).strip('"').strip("'")
                if tv.replace(".", "", 1).isdigit():
                    true_val = ibis.literal(float(tv))
                else:
                    true_val = ibis.literal(tv)
            e = cond.ifelse(true_val, e)
        return e

    @staticmethod
    def switch(table, node, data_types, inter_exps):
        params = node["inputs"]
        # SWITCH(expression, val1, res1, val2, res2, ..., [default])
        # AST preserves user order: params[0]=expression, then val-result pairs,
        # with optional default as last odd param
        if len(params) < 3:
            raise Exception("SWITCH function requires at least 3 parameters: SWITCH(expression, val1, result1)")

        # params[0] is the expression to match against
        s0 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])

        # Determine if there's a default value (odd number of remaining params)
        remaining = params[1:]
        if len(remaining) % 2 == 1:
            # Last param is the default
            default_param = remaining[-1]
            pairs = remaining[:-1]
        else:
            default_param = None
            pairs = remaining

        # Build default expression
        if default_param is None:
            oe = ibis.NA
        elif default_param == "NONE" or (isinstance(default_param, str) and default_param.upper() == "NULL"):
            oe = ibis.NA
        elif default_param in inter_exps:
            oe = inter_exps[default_param]
        else:
            oe = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, default_param)

        # Determine the result type from the first result expression
        # so all branches return the same type
        first_result = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, pairs[1])
        result_type = first_result.type()

        # Cast default to match result type
        if oe is not ibis.NA:
            try:
                oe = oe.cast(result_type)
            except Exception:
                pass

        # Iterate value-result pairs in reverse with step -2
        for i in range(len(pairs) - 1, 0, -2):
            val_expr = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, pairs[i - 1])
            res_expr = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, pairs[i])
            # Cast comparison value to match the expression type
            try:
                val_expr = val_expr.cast(s0.type())
            except Exception:
                pass
            # Cast result to match the common result type
            try:
                res_expr = res_expr.cast(result_type)
            except Exception:
                pass
            cond = s0 == val_expr
            oe = cond.ifelse(res_expr, oe)
        data_types[node["outputs"][0]] = data_types.get(pairs[1], "string")
        return oe

    @staticmethod
    def choose(table, node, data_types, inter_exps):
        if node["inputs"].__len__() < 2:
            raise Exception("CHOOSE function requires at least 2 parameters")
        idx = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        e = ibis.NA
        for i in range(1, node["inputs"].__len__()):
            cond = idx == i
            e = cond.ifelse(FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][i]), e)
        return e

    @staticmethod
    def isblank(table, node, data_types, inter_exps):
        params = node["inputs"]
        if node["inputs"].__len__() != 1:
            raise Exception("ISBLANK function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        e = e.isnull()
        data_types[node["outputs"][0]] = "boolean"
        return e

    @staticmethod
    def iseven(table, node, data_types, inter_exps):
        params = node["inputs"]
        if node["inputs"].__len__() != 1:
            raise Exception("ISEVEN function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0]) % 2 == 0
        data_types[node["outputs"][0]] = "boolean"
        return e

    @staticmethod
    def isodd(table, node, data_types, inter_exps):
        params = node["inputs"]
        if node["inputs"].__len__() != 1:
            raise Exception("ISODD function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0]) % 2 == 1
        data_types[node["outputs"][0]] = "boolean"
        return e

    @staticmethod
    def isna(table, node, data_types, inter_exps):
        params = node["inputs"]
        if node["inputs"].__len__() != 1:
            raise Exception("ISNA function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0]) == ibis.NA
        data_types[node["outputs"][0]] = "boolean"
        return e

    @staticmethod
    def istext(table, node, data_types, inter_exps):
        params = node["inputs"]
        if len(params) != 1:
            raise Exception("ISTEXT requires 1 parameter")

        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])
        # Case 1: If ibis type is string, it's text
        if hasattr(e, "type") and e.type().is_string():
            return ibis.literal(True)
        # Otherwise, it's not text
        return ibis.literal(False)

    @staticmethod
    def isnumber(table, node, data_types, inter_exps):
        params = node["inputs"]
        if len(params) != 1:
            raise Exception("ISNUMBER requires 1 parameter")

        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])

        # Case 1: Already numeric type
        if hasattr(e, "type") and (e.type().is_integer() or e.type().is_floating()):
            return ibis.literal(True)

        # Case 2: String type → check with regex
        return e.re_search(r"^\d*\.?\d+$") == True

    @staticmethod
    def true_(table, node, data_types, inter_exps):
        params = node["inputs"]
        if node["inputs"].__len__() != 1:
            raise Exception("TRUE function requires 0 parameters")
        e = ibis.literal(True)
        data_types[node["outputs"][0]] = "boolean"
        return e

    @staticmethod
    def false_(table, node, data_types, inter_exps):
        params = node["inputs"]
        if node["inputs"].__len__() != 1:
            raise Exception("TRUE function requires 0 parameters")
        e = ibis.literal(False)
        data_types[node["outputs"][0]] = "boolean"
        return e

    @staticmethod
    def between(table, node, data_types, inter_exps):
        params = node["inputs"]
        if node["inputs"].__len__() != 3:
            raise Exception("BETWEEN function requires 3 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[0])
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[1])
        e3 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, params[2])
        return e1.between(e2, e3)

    # =========================================================================
    # Null/Type Functions
    # =========================================================================

    @staticmethod
    def fill_null(table, node, data_types, inter_exps):
        """Replaces null values with a specified value."""
        if len(node["inputs"]) != 2:
            raise Exception("FILL_NULL function requires 2 parameters: FILL_NULL(column, replacement)")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        replacement = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][1])

        # Cast replacement to match column type for compatibility
        # (e.g. string column with string replacement, numeric column with numeric replacement)
        try:
            col_type = e.type()
            replacement = replacement.cast(col_type)
        except Exception:
            pass

        e = e.fill_null(replacement)
        data_types[node["outputs"][0]] = data_types.get(node["inputs"][0], "string")
        return e

    @staticmethod
    def nullif(table, node, data_types, inter_exps):
        """Returns null if the two arguments are equal, otherwise returns the
        first argument."""
        if len(node["inputs"]) != 2:
            raise Exception("NULLIF function requires 2 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][1])

        # Cast comparison value to match column type so equality check works correctly
        try:
            col_type = e1.type()
            e2 = e2.cast(col_type)
        except Exception:
            pass

        e = e1.nullif(e2)
        data_types[node["outputs"][0]] = data_types.get(node["inputs"][0], "string")
        return e

    @staticmethod
    def isnan(table, node, data_types, inter_exps):
        """Returns true if the value is NaN (Not a Number)."""
        if len(node["inputs"]) != 1:
            raise Exception("ISNAN function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])

        # isnan() requires floating-point type; cast integer columns to float64
        try:
            if e.type().is_integer():
                e = e.cast("float64")
        except Exception:
            pass

        e = e.isnan()
        data_types[node["outputs"][0]] = "boolean"
        return e

    @staticmethod
    def isinf(table, node, data_types, inter_exps):
        """Returns true if the value is infinite."""
        if len(node["inputs"]) != 1:
            raise Exception("ISINF function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])

        # isinf() requires floating-point type; cast integer columns to float64
        try:
            if e.type().is_integer():
                e = e.cast("float64")
        except Exception:
            pass

        e = e.isinf()
        data_types[node["outputs"][0]] = "boolean"
        return e

    @staticmethod
    def try_cast(table, node, data_types, inter_exps):
        """Attempts to cast a value to a specified type, returning null on
        failure."""
        if len(node["inputs"]) != 2:
            raise Exception("TRY_CAST function requires 2 parameters: TRY_CAST(value, type)")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        target_type = node["inputs"][1].strip('"').strip("'").lower()
        e = e.try_cast(target_type)
        data_types[node["outputs"][0]] = target_type
        return e

    @staticmethod
    def coalesce(table, node, data_types, inter_exps):
        """Returns the first non-null value from the arguments."""
        if len(node["inputs"]) < 2:
            raise Exception("COALESCE function requires at least 2 parameters")
        exprs = [FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, inp) for inp in node["inputs"]]
        e = ibis.coalesce(*exprs)
        data_types[node["outputs"][0]] = data_types.get(node["inputs"][0], "string")
        return e
