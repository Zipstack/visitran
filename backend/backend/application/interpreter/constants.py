class Operators:
    CONTAINS = "CONTAINS"
    NOT_CONTAINS = "NOTCONTAINS"
    STARTSWITH = "STARTSWITH"
    ENDSWITH = "ENDSWITH"
    EQUAL_TO = "EQ"
    NOT_EQUAL_TO = "NEQ"
    LESS_THAN_EQUAL_TO = "LTE"
    GREATER_THAN_EQUAL_TO = "GTE"
    LESS_THAN = "LT"
    GREATER_THAN = "GT"
    IS_TRUE = "TRUE"
    IS_FALSE = "FALSE"
    LIKE = "LIKE"
    IN = "IN"
    NOT_IN = "NOTIN"
    NULL = "NULL"
    NOT_NULL = "NOTNULL"
    BETWEEN = "BETWEEN"

    IBIS_IS_EQUAL = " == {value}"
    IBIS_IS_NOT_EQUAL = " != {value}"
    IBIS_IS_TRUE = " == True"
    IBIS_IS_FALSE = " == False"
    IBIS_IS_IN = ".isin({value})"
    IBIS_IS_NOT_IN = ".notin({value})"

    IBIS_LESS_THAN_EQUAL_TO = " <= {value}"
    IBIS_GREATER_THAN_EQUAL_TO = " >= {value}"
    IBIS_LESS_THAN = " < {value}"
    IBIS_GREATER_THAN = " > {value}"
    IBIS_LIKE = ".like('{value}')"

    IBIS_NULL = ".isnull()"
    IBIS_NOT_NULL = ".notnull()"
    IBIS_CONTAINS = ".like('%{value}%')"
    IBIS_STARTSWITH = ".like('{value}%')"
    IBIS_ENDSWITH = ".like('%{value}')"

    FUNCTIONAL_OPERATORS = [IN, NOT_IN, CONTAINS, NOT_CONTAINS, STARTSWITH, ENDSWITH]
    NEGATIVE_OPERATORS = [
        NOT_CONTAINS,
    ]

    NO_RHS_OPERATORS = [
        IS_TRUE,
        IS_FALSE,
        NULL,
        NOT_NULL,
    ]

    @classmethod
    def get_operator_type(cls, _type: str, **kwargs) -> str:
        # NOTE: Instead of equal to and not equal to, used isin and isnotin statements from ibis
        # This is because ibis has some issues with str type columns mapping with multiple columns
        mapper = {
            cls.EQUAL_TO: cls.IBIS_IS_EQUAL,
            cls.NOT_EQUAL_TO: cls.IBIS_IS_NOT_EQUAL,
            cls.LESS_THAN: cls.IBIS_LESS_THAN,
            cls.GREATER_THAN: cls.IBIS_GREATER_THAN,
            cls.LESS_THAN_EQUAL_TO: cls.IBIS_LESS_THAN_EQUAL_TO,
            cls.GREATER_THAN_EQUAL_TO: cls.IBIS_GREATER_THAN_EQUAL_TO,
            cls.IS_TRUE: cls.IBIS_IS_TRUE,
            cls.IS_FALSE: cls.IBIS_IS_FALSE,
            cls.LIKE: cls.IBIS_LIKE,
            cls.IN: cls.IBIS_IS_IN,
            cls.NOT_IN: cls.IBIS_IS_NOT_IN,
            cls.NULL: cls.IBIS_NULL,
            cls.NOT_NULL: cls.IBIS_NOT_NULL,
            cls.CONTAINS: cls.IBIS_CONTAINS,
            cls.NOT_CONTAINS: cls.IBIS_CONTAINS,
            cls.STARTSWITH: cls.IBIS_STARTSWITH,
            cls.ENDSWITH: cls.IBIS_ENDSWITH,
        }
        if type_mapper := mapper.get(_type):
            return type_mapper.format(**kwargs)
        raise ValueError(f"Operator {_type} not supported in filters")


class ConditionTypes:
    AND = "AND"
    OR = "OR"

    @classmethod
    def get_condition_type(cls, _type: str) -> str:
        mapper = {cls.AND: " & ", cls.OR: " | "}
        return mapper.get(_type) or " & "


class JoinTypes:
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    OUTER = "outer"
    CROSS = "cross"

    INNER_JOIN = "inner_join"
    LEFT_JOIN = "left_join"
    RIGHT_JOIN = "right_join"
    FULL_JOIN = "outer_join"
    CROSS_JOIN = "cross_join"

    @classmethod
    def get_join_type(cls, join_type: str):
        _mapper = {
            cls.INNER: cls.INNER_JOIN,
            cls.LEFT: cls.LEFT_JOIN,
            cls.RIGHT: cls.RIGHT_JOIN,
            cls.FULL: cls.FULL_JOIN,
            cls.CROSS: cls.CROSS_JOIN,
        }
        return _mapper.get(join_type.lower(), "join")


class TemplateConstants:
    CLASS_NAME = "class_name"
    PARENT_CLASS = "parent_class"
    MATERIALIZATION_NAME = "materializer_name"
    SOURCE_SCHEMA_NAME = "source_schema_name"
    SOURCE_TABLE_NAME = "source_table_name"
    DESTINATION_SCHEMA_NAME = "destination_schema_name"
    DESTINATION_TABLE_NAME = "destination_table_name"
    DATABASE_NAME = "database_name"
    FILTERS = "filters"
    PARENT_DECLARATION = "parent_declaration"


class TemplateNames:
    EPHEMERAL_TABLE = "ephemeral_table"
    DESTINATION_TABLE = "destination_table"
    FILE_GENERATOR = "file_generator"
    STATEMENT = "statement"

    # Transformation templates
    FILTER = "filter"
    JOIN = "join"
    PIVOT = "pivot"
    DISTINCT = "distinct"
    COMBINE_COLUMN = "combine_column"
    RENAME_COLUMN = "rename_column"
    GROUPS_AND_AGGREGATION = "groups_and_aggregation"
    SYNTHESIZE = "synthesize"
    UNION = "unions"
    FIND_AND_REPLACE = "find_and_replace"
    SORT = "sort"
    COLUMN_REORDER = "column_reorder"
    WINDOW = "window"


class Aggregations:
    COUNT = "COUNT"
    AVG = "AVG"
    MEAN = "MEAN"
    SUM = "SUM"
    MIN = "MIN"
    MAX = "MAX"
    STD = "STD"
    STDDEV = "STDDEV"
    VARIANCE = "VARIANCE"

    mapper = {
        "COUNT": "count",
        "AVG": "mean",
        "MEAN": "mean",
        "SUM": "sum",
        "MIN": "min",
        "MAX": "max",
        "STD": "std",
        "STDDEV": "std",
        "VARIANCE": "var",
    }


class OperatorsToIbis:
    JOIN_MAPPER = {
        Operators.EQUAL_TO: "==",
        Operators.NOT_EQUAL_TO: "!=",
        Operators.LESS_THAN: "<",
        Operators.LESS_THAN_EQUAL_TO: "<=",
        Operators.GREATER_THAN: ">",
        Operators.GREATER_THAN_EQUAL_TO: ">=",
        Operators.IS_TRUE: "",
        Operators.IS_FALSE: "",
        Operators.LIKE: ".like({value})",
        Operators.IN: ".isin({value})",
        Operators.NULL: ".isnull()",
        Operators.NOT_NULL: ".notnull()",
        ConditionTypes.AND: " & ",
        ConditionTypes.OR: " | ",
    }


class SortOperators:
    ASC = "ASC"
    DESC = "DESC"

    SORT_MAPPERS = {ASC: 'ibis.asc("{value}")', DESC: 'ibis.desc("{value}")'}


class UnionConstants:
    UNION_LITERAL = "ibis.literal(None)"


class FindAndReplaceConstants:
    EXACT_TEXT = "EXACT_TEXT"
    TEXT = "TEXT"
    EMPTY = "EMPTY"
    LETTERS = "LETTERS"
    DIGITS = "DIGITS"
    SYMBOLS = "SYMBOLS"
    WHITESPACE = "WHITESPACE"
    CURRENCY = "CURRENCY"
    PUNCTUATION = "PUNCTUATION"
    REGEX = "REGEX"
    FILL_NULL = "FILL_NULL"

    FIND_VALUE = {
        EMPTY: "^$",
        LETTERS: "[A-Za-z]",
        DIGITS: "[0-9]",
        SYMBOLS: "[$@#%^*-+/]",
        WHITESPACE: " ",
        CURRENCY: "[€$£₹]",
        PUNCTUATION: "[.,!]",
        TEXT: "(?i)^{value}$",
        FILL_NULL: r"^([Nn][Uu][Ll][Ll]|[Nn][Oo][Nn][Ee]|)$",
    }

    REGEX_PATTERN_OPERATORS = [EMPTY, LETTERS, DIGITS, SYMBOLS, CURRENCY, PUNCTUATION, REGEX, TEXT]


class CombineColumns:
    COLUMN = "COLUMN"
    VALUE = "VALUE"
