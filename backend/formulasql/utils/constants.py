import ibis


class IbisDataType:
    NUMERIC_TYPES = (
        ibis.expr.datatypes.Integer,
        ibis.expr.datatypes.Floating,
        ibis.expr.datatypes.Decimal,
    )
    TEMPORAL_TYPES = (ibis.expr.datatypes.Timestamp, ibis.expr.datatypes.Date)
    STRING_TYPES = (ibis.expr.datatypes.String,)
    BOOLEAN_TYPES = ibis.expr.datatypes.Boolean
    TIMESTAMP = ibis.expr.datatypes.Timestamp
    DATE = ibis.expr.datatypes.Date
    STRING = (ibis.expr.datatypes.String,)
