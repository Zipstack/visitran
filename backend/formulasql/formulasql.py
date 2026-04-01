import re

import formulas
import ibis
from ibis.expr.types.relations import Table

from formulasql.functions.datetime import DateTime
from formulasql.functions.logics import Logics
from formulasql.functions.math import Math
from formulasql.functions.operators import Operators
from formulasql.functions.text import Text
from formulasql.functions.window import Window


class FormulaSQL:

    def __init__(self, table: Table, column_name: str, formula: str):
        self.formula = formula
        self.table = table
        self.column_name = column_name
        self.ast = formulas.Parser().ast(formula)[1]
        self.data_types = {}
        self.inter_exps = {}

    def print_ast(self):
        print("AST:")
        nodes = self.ast.dsp.solution.nodes
        for key, node in nodes.items():
            print(key, "|", node)

    def __get_data_type(self, key: str):
        try:
            float(key)
            return "numeric"
        except ValueError:
            if key.isnumeric():
                return "numeric"
            elif key.startswith('"') and key.endswith('"'):
                return "string"
            elif key == "NULL" or key == "NONE":
                return "none"
            elif key == "true" or key == "false" or key == "TRUE" or key == "FALSE" or key == "True" or key == "False":
                return "boolean"
            else:
                return "column"

    def ibis_column(self):
        nodes = self.ast.dsp.solution.nodes

        exp = None
        for key, node in nodes.items():
            # print(key, '|', node)
            if node["type"] == "data":
                self.data_types[key] = self.__get_data_type(key)

            if node["type"] == "function":
                function_name = re.sub("<(.*?)>", "", key).lower()
                function_found = False
                if key.startswith("bypass"):
                    input_key = node["inputs"][0]
                    output_key = node["outputs"][0]

                    # Pass the expression from input to output

                    self.inter_exps[output_key] = self.inter_exps[input_key]
                    continue

                if node["inputs"].__len__() > 1:
                    self.data_types[node["outputs"][0]] = self.data_types[node["inputs"][1]]
                # -----------------------------------------------------------------
                # Basic math operations
                # -----------------------------------------------------------------
                if key.startswith("/"):
                    exp = Operators.division(self.table, node, self.data_types, self.inter_exps)
                    function_found = True
                elif key.startswith("%"):
                    exp = Operators.modulus(self.table, node, self.data_types, self.inter_exps)
                    function_found = True
                elif key.startswith("*"):
                    exp = Operators.multiplication(self.table, node, self.data_types, self.inter_exps)
                    function_found = True
                elif key.startswith("+"):
                    exp = Operators.addition(self.table, node, self.data_types, self.inter_exps)
                    function_found = True
                elif key.startswith("-"):
                    exp = Operators.subtraction(self.table, node, self.data_types, self.inter_exps)
                    function_found = True
                elif key.startswith("u+"):
                    exp = Operators.addition_u(self.table, node, self.data_types, self.inter_exps)
                    function_found = True
                elif key.startswith("u-"):
                    exp = Operators.subtraction_u(self.table, node, self.data_types, self.inter_exps)
                    function_found = True
                elif key.startswith("&"):
                    exp = Operators.ampersand(self.table, node, self.data_types, self.inter_exps)
                    function_found = True
                elif key.startswith("="):
                    exp = Operators.equal(self.table, node, self.data_types, self.inter_exps)
                    function_found = True
                elif key.startswith("<>") or key.startswith("!="):
                    exp = Operators.not_equal(self.table, node, self.data_types, self.inter_exps)
                    function_found = True
                # IMPORTANT: Check >= / <= BEFORE > / < to avoid
                # startswith(">") matching ">=" (same for "<" vs "<=")
                elif key.startswith(">="):
                    exp = Operators.greater_than_or_equal(self.table, node, self.data_types, self.inter_exps)
                    function_found = True
                elif key.startswith("<="):
                    exp = Operators.less_than_or_equal(self.table, node, self.data_types, self.inter_exps)
                    function_found = True
                elif key.startswith(">"):
                    exp = Operators.greater_than(self.table, node, self.data_types, self.inter_exps)
                    function_found = True
                elif key.startswith("<"):
                    exp = Operators.less_than(self.table, node, self.data_types, self.inter_exps)
                    function_found = True
                elif key.startswith("AND"):
                    exp = Operators.and_(self.table, node, self.data_types, self.inter_exps)
                    function_found = True
                elif key.startswith("OR"):
                    exp = Operators.or_(self.table, node, self.data_types, self.inter_exps)
                    function_found = True
                elif key.startswith("NOT"):
                    exp = Operators.not_(self.table, node, self.data_types, self.inter_exps)
                    function_found = True
                elif key.startswith("XOR"):
                    exp = Operators.xor_(self.table, node, self.data_types, self.inter_exps)
                    function_found = True
                # --------------------------------------------------
                # FIX: Add IFS support (minimal change)
                # --------------------------------------------------
                elif key.upper().startswith("IFS"):
                    exp = Logics.ifs(self.table, node, self.data_types, self.inter_exps)
                    function_found = True

                # -----------------------------------------------------------------
                # Named function lookup (only if not already matched above)
                # Uses getattr to find the function in each category class.
                # Stops at the FIRST match to avoid overwriting.
                # -----------------------------------------------------------------
                if not function_found:
                    for func_class in (Logics, DateTime, Text, Math, Window):
                        try:
                            func = getattr(func_class, function_name)
                            exp = func(self.table, node, self.data_types, self.inter_exps)
                            function_found = True
                            break
                        except AttributeError:
                            try:
                                func = getattr(func_class, function_name + "_")
                                exp = func(self.table, node, self.data_types, self.inter_exps)
                                function_found = True
                                break
                            except AttributeError:
                                pass

                if not function_found:
                    raise Exception("Formula not supported - " + key)

                if exp is None:
                    raise Exception("Formula not supported - " + key)
                self.inter_exps[node["outputs"][0]] = exp

                # print('inter_exps', self.inter_exps)

        if exp is None:
            if key.lower() in ("true", "false"):
                exp = ibis.literal(key.lower() == "true")
            elif key.isnumeric():
                exp = ibis.literal(float(key))
            else:
                exp = ibis.literal(key)
        exp = exp.name(self.column_name)
        return exp
