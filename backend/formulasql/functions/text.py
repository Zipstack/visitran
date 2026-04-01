import logging

from formulas import functions
from formulasql.utils.formulasql_utils import FormulaSQLUtils

logger = logging.getLogger(__name__)

try:
    from formulasql.base_functions.base_text import BaseText as Base
except:
    from abc import ABC as Base


# ---------------------------------------------------------------------------
# Monkey-patch ibis PostgreSQL RegexExtract to fix operator precedence.
# ibis generates:  col ~ '(' || pattern || ')'
# In PostgreSQL, ~ and || share the same precedence (left-associative), so
# this parses as (col ~ '(') || pattern || ')' — a text result, not boolean.
# The fix wraps the CONCAT in parentheses so it evaluates first.
# ---------------------------------------------------------------------------
try:
    import ibis.backends.sql.compilers.postgres as _pg_comp
    import sqlglot.expressions as _sge

    _original_visit_RegexExtract = _pg_comp.PostgresCompiler.visit_RegexExtract

    def _patched_visit_RegexExtract(self, op, *, arg, pattern, index):
        pattern = _sge.Paren(this=self.f.concat("(", pattern, ")"))
        matches = self.f.regexp_match(arg, pattern)
        return self.if_(
            arg.rlike(pattern),
            _sge.paren(matches, copy=False)[index],
            _sge.Null(),
        )

    _pg_comp.PostgresCompiler.visit_RegexExtract = _patched_visit_RegexExtract
except Exception:
    pass


class Text(Base):

    def __init__(self):
        pass

    @staticmethod
    def __num(s):
        try:
            return int(s)
        except ValueError:
            return float(s)

    @staticmethod
    def numbervalue(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("NUMBERVALUE function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.cast('string').cast('double')
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def clean(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("CLEAN function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
        e = e.re_replace(r'[^\x20-\x7E]+', '')
        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def code(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 1:
            raise Exception("CODE function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
        e = e.ascii_str().cast('int')
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def concatenate(table, node, data_types, inter_exps):
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast('string')
        for i in range(1, node['inputs'].__len__()):
            e = e + FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][i]).cast(
                'string')
        inter_exps[node['outputs'][0]] = e
        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def concat(table, node, data_types, inter_exps):
        return Text.concatenate(table, node, data_types, inter_exps)

    @staticmethod
    def exact(table, node, data_types, inter_exps):
        if node['inputs'].__len__() != 2:
            raise Exception("EXACT function requires 2 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast('string')
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1]).cast('string')
        e = e1 == e2
        data_types[node['outputs'][0]] = 'boolean'
        return e

    @staticmethod
    def find(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 2:
            e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast('string')
            e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1]).cast('string')
            e = e2.find(e1)
        elif node['inputs'].__len__() == 3:
            e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast('string')
            e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1]).cast('string')
            e3 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][2])
            e = e2.find(e1, start=e3 - 1)
        else:
            raise Exception("FIND function requires 2 or 3 parameters")

        data_types[node['outputs'][0]] = 'numeric'
        return e + 1

    @staticmethod
    def fixed(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 1:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
            e = e.round(0).cast('int').cast('string')
        elif node['inputs'].__len__() == 2:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0])
            e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])

            #  FIX: positional call, not keyword
            e = e.round(e2).cast('string')
        else:
            raise Exception("FIXED function requires 1 or 2 parameters")

        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def left(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 1:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e = e.left(1)
        elif node['inputs'].__len__() == 2:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
            e = e.left(e2)
        else:
            raise Exception("LEFT function requires 1 or 2 parameters")
        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def right(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 1:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e = e.right(1)
        elif node['inputs'].__len__() == 2:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
            e = e.right(e2)
        else:
            raise Exception("RIGHT function requires 1 or 2 parameters")
        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def mid(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 3:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
            e3 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][2])
            e = e.substr(e2 - 1, e3)
        else:
            raise Exception("MID function requires  3 parameters")
        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def len(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 1:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e = e.length()
        else:
            raise Exception("LEN function requires 1 parameter")
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def length(table, node, data_types, inter_exps):
        return Text.len(table, node, data_types, inter_exps)

    @staticmethod
    def lower(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 1:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e = e.lower()
        else:
            raise Exception("LOWER function requires 1 parameter")
        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def upper(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 1:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e = e.upper()
        else:
            raise Exception("UPPER function requires 1 parameter")
        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def proper(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 1:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e = e.capitalize()
        else:
            raise Exception("PROPER function requires 1 parameter")
        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def rept(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 2:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
            e = e.repeat(e2)
        else:
            raise Exception("REPEAT function requires 2 parameters")
        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def search(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 2:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1]).cast("string")
            e = e2.find(e) + 1
        else:
            raise Exception("SEARCH function requires 3 parameters")
        data_types[node['outputs'][0]] = 'numeric'
        return e

    @staticmethod
    def substitute(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 3:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1]).cast("string")
            e3 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][2]).cast("string")
            e = e.replace(e2, e3)
        else:
            raise Exception("SUBSTITUTE function requires 3 parameters")
        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def trim(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 1:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e = e.strip()
        else:
            raise Exception("TRIM function requires 1 parameter")
        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def ltrim(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 1:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e = e.lstrip()
        else:
            raise Exception("LTRIM function requires 1 parameter")
        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def rtrim(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 1:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e = e.rstrip()
        else:
            raise Exception("RTRIM function requires 1 parameter")
        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def contains(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 2:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1]).cast("string")
            e = e.contains(e2)
        else:
            raise Exception("CONTAINS function requires 2 parameters")
        data_types[node['outputs'][0]] = 'boolean'
        return e

    @staticmethod
    def endswith(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 2:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1]).cast("string")
            e = e.endswith(e2)
        else:
            raise Exception("ENDSWITH function requires 2 parameters")
        data_types[node['outputs'][0]] = 'boolean'
        return e

    @staticmethod
    def startswith(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 2:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1]).cast("string")
            e = e.startswith(e2)
        else:
            raise Exception("STARTSWITH function requires 2 parameters")
        data_types[node['outputs'][0]] = 'boolean'
        return e

    @staticmethod
    def hash(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 2:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast('string')
            e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1])
            e = e.hash(e2)
        else:
            raise Exception("HASH function requires 1 parameter")
        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def like(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 2:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1]).cast("string")
            e = e.like(e2)
        else:
            raise Exception("LIKE function requires 2 parameters")
        data_types[node['outputs'][0]] = 'boolean'
        return e

    @staticmethod
    def ilike(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 2:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1]).cast("string")
            e = e.ilike(e2)
        else:
            raise Exception("ILIKE function requires 2 parameters")
        data_types[node['outputs'][0]] = 'boolean'
        return e

    @staticmethod
    def join(table, node, data_types, inter_exps):
        if node['inputs'].__len__() < 3:
            raise Exception("IFS function requires odd number of parameters >= 3")
        len = node['inputs'].__len__()
        s0 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
        oe = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][len - 1]).cast("string")
        e = oe
        for i in range(len - 1, 1, -1):
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][i - 1]).cast("string")
            e = s0.join([e, oe])
            oe = e
        inter_exps[node['outputs'][0]] = e
        data_types[node['outputs'][0]] = data_types[node['inputs'][2]]
        return e

    @staticmethod
    def lpad(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 3:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1]).cast("int32")
            e3 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][2]).cast("string")
            e = e.lpad(e2, e3)
        else:
            raise Exception("LPAD function requires 3 parameters")
        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def rpad(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 3:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1]).cast("int32")
            e3 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][2]).cast("string")
            e = e.rpad(e2, e3)
        else:
            raise Exception("RPAD function requires 3 parameters")
        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def regex_extract(table, node, data_types, inter_exps):
        import re as _re
        if node['inputs'].__len__() == 3:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            # Pass pattern and index as raw Python str/int instead of ibis
            # expressions. On PostgreSQL, ibis wraps the pattern with
            # CONCAT("(", pattern, ")") and uses REGEXP_MATCH + array
            # indexing. Passing ibis Cast expressions produces complex SQL
            # that PostgreSQL rejects (CASE WHEN type mismatch).
            pattern = node['inputs'][1].strip('"').strip("'")
            index = int(node['inputs'][2])

            # On PostgreSQL, ibis wraps the pattern in an outer "()" group
            # and adds +1 to the index for 1-based array access.
            # If the pattern has NO user-defined capture groups,
            # the only valid array index is [1] (the outer wrap).
            # User index=1 means "get the match" → map to ibis index=0
            # so ibis generates array[0+1] = array[1] which works.
            #
            # If the pattern HAS capture groups like "([^@]+)@([^.]+)",
            # ibis wraps it as "(([^@]+)@([^.]+))":
            #   array[1] = whole match (outer wrap)
            #   array[2] = first inner group → ibis index=1 → correct
            # So user index=1 maps directly to ibis index=1.
            has_capture_groups = bool(_re.search(r'(?<!\\)\((?!\?)', pattern))
            if not has_capture_groups:
                index = 0

            e = e.re_extract(pattern, index)
        else:
            raise Exception("REGEX_EXTRACT function requires 3 parameters")
        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def regex_replace(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 3:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            # Pass pattern and replacement as raw Python strings
            pattern = node['inputs'][1].strip('"').strip("'")
            replacement = node['inputs'][2].strip('"').strip("'")
            e = e.re_replace(pattern, replacement)
        else:
            raise Exception("REGEX_REPLACE function requires 3 parameters")
        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def regex_search(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 2:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            # Pass pattern as raw Python string
            pattern = node['inputs'][1].strip('"').strip("'")
            e = e.re_search(pattern)
        else:
            raise Exception("REGEX_SEARCH function requires 2 parameters")
        data_types[node['outputs'][0]] = 'boolean'
        return e

    @staticmethod
    def reverse(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 1:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e = e.reverse()
        else:
            raise Exception("REVERSE function requires 1 parameter")
        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def timestamp(table, node, data_types, inter_exps):
        if node['inputs'].__len__() == 2:
            e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
            e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1]).cast("string")
            e = e.as_timestamp(e2)
        else:
            raise Exception("TIMESTAMP function requires 2 parameter")
        data_types[node['outputs'][0]] = 'timestamp'
        return e

    # =========================================================================
    # Additional String Functions
    # =========================================================================
    @staticmethod
    def capitalize(table, node, data_types, inter_exps):
        """Capitalizes the first letter of each word."""
        if node['inputs'].__len__() != 1:
            raise Exception("CAPITALIZE function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
        e = e.capitalize()
        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def translate(table, node, data_types, inter_exps):
        """Replaces characters in a string based on a mapping."""
        if node['inputs'].__len__() != 3:
            raise Exception("TRANSLATE function requires 3 parameters: TRANSLATE(string, from_chars, to_chars)")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
        from_chars = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1]).cast("string")
        to_chars = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][2]).cast("string")
        e = e.translate(from_chars, to_chars)
        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def levenshtein(table, node, data_types, inter_exps):
        """Returns the Levenshtein distance between two strings.

        Note: On PostgreSQL this requires the fuzzystrmatch extension.
        Enable it with: CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
        """
        if node['inputs'].__len__() != 2:
            raise Exception("LEVENSHTEIN function requires 2 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1]).cast("string")

        # Try to enable fuzzystrmatch extension for PostgreSQL backends
        try:
            backend = table._find_backend()
            if hasattr(backend, 'raw_sql'):
                backend.raw_sql("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch")
        except Exception:
            logger.warning("Failed to enable fuzzystrmatch extension for LEVENSHTEIN", exc_info=True)

        e = e1.levenshtein(e2)
        data_types[node['outputs'][0]] = 'int'
        return e

    @staticmethod
    def split(table, node, data_types, inter_exps):
        """Splits a string by a delimiter and returns an array."""
        if node['inputs'].__len__() != 2:
            raise Exception("SPLIT function requires 2 parameters: SPLIT(string, delimiter)")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
        delimiter = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1]).cast("string")
        e = e.split(delimiter)
        data_types[node['outputs'][0]] = 'array'
        return e

    @staticmethod
    def split_part(table, node, data_types, inter_exps):
        """Extracts the nth part from a string split by a delimiter.

        Usage: SPLIT_PART(string, delimiter, position)
        - position is 1-indexed (1 = first part, 2 = second part, etc.)

        Example: SPLIT_PART('a,b,c', ',', 2) returns 'b'
        """
        if node['inputs'].__len__() != 3:
            raise Exception("SPLIT_PART function requires 3 parameters: SPLIT_PART(string, delimiter, position)")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
        delimiter = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][1]).cast("string")
        position = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][2])
        # Split the string and get the element at position-1 (convert 1-indexed to 0-indexed)
        e = e.split(delimiter)[position - 1]
        data_types[node['outputs'][0]] = 'string'
        return e

    @staticmethod
    def ascii_(table, node, data_types, inter_exps):
        """Returns the ASCII code of the first character."""
        if node['inputs'].__len__() != 1:
            raise Exception("ASCII function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
        e = e.ascii_str()
        data_types[node['outputs'][0]] = 'int'
        return e

    @staticmethod
    def initcap(table, node, data_types, inter_exps):
        """Capitalizes the first letter of each word (alias for PROPER)."""
        if node['inputs'].__len__() != 1:
            raise Exception("INITCAP function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node['inputs'][0]).cast("string")
        e = e.capitalize()
        data_types[node['outputs'][0]] = 'string'
        return e
