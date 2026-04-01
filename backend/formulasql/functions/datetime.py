import datetime

import ibis
import ibis.expr.datatypes as dt

from formulasql.utils.constants import IbisDataType
from formulasql.utils.formulasql_utils import FormulaSQLUtils


def _is_bigquery_backend(table):
    """Check if the Ibis table is connected to a BigQuery backend."""
    try:
        backend = table._find_backend()
        return "bigquery" in type(backend).__module__.lower()
    except Exception:
        return False


def _bq_timestamp_cast(table, expr):
    """Cast to timezone-aware timestamp for BigQuery, plain timestamp
    otherwise."""
    if _is_bigquery_backend(table):
        return expr.cast(dt.Timestamp(timezone="UTC"))
    return expr.cast("timestamp")


class DateTime:

    @staticmethod
    def __num(s):
        try:
            return int(s)
        except ValueError:
            return float(s)

    @staticmethod
    def now(table, node, data_types, inter_exps):
        data_types[node["outputs"][0]] = "datetime"
        return ibis.now()

    @staticmethod
    def date(table, node, data_types, inter_exps):
        if node["inputs"].__len__() == 3:
            year = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
            month = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][1])
            day = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][2])
            e = ibis.date(year.cast("string") + "-" + month.cast("string") + "-" + day.cast("string")).cast("date")
            data_types[node["outputs"][0]] = "date"
        elif node["inputs"].__len__() == 2:
            e = ibis.literal(datetime.datetime.strptime(node["inputs"][0], node["inputs"][1]))
            data_types[node["outputs"][0]] = "date"
        elif node["inputs"].__len__() == 1:
            e = ibis.date(FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0]))
            data_types[node["outputs"][0]] = "date"
        else:
            raise Exception("DATE function requires minimum 1 to 3 parameters")
        return e

    @staticmethod
    def day(table, node, data_types, inter_exps):
        if node["inputs"].__len__() != 1:
            raise Exception("DAY function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        if isinstance(e.type(), IbisDataType.TEMPORAL_TYPES) or isinstance(e.type(), IbisDataType.STRING):
            e = e.cast("date").day()
        data_types[node["outputs"][0]] = "int"
        return e

    @staticmethod
    def month(table, node, data_types, inter_exps):
        if node["inputs"].__len__() != 1:
            raise Exception("MONTH function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        e = e.cast("date").month()
        data_types[node["outputs"][0]] = "int"
        return e

    @staticmethod
    def year(table, node, data_types, inter_exps):
        if node["inputs"].__len__() != 1:
            raise Exception("YEAR function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        e = e.cast("date").year()
        data_types[node["outputs"][0]] = "int"
        return e

    @staticmethod
    def days(table, node, data_types, inter_exps):
        """Returns the number of days between two dates as an integer.

        Uses epoch_seconds to avoid interval-to-int cast issues that
        occur on PostgreSQL and DuckDB when date subtraction involves
        interval expressions (e.g. from EDATE).
        """
        if node["inputs"].__len__() != 2:
            raise Exception("DAYS function requires 2 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][1])

        # Convert to timestamps and use epoch_seconds difference to avoid
        # interval-to-bigint cast errors on PostgreSQL/DuckDB
        t1 = _bq_timestamp_cast(table, e1)
        t2 = _bq_timestamp_cast(table, e2)
        e = ((t1.epoch_seconds() - t2.epoch_seconds()) / 86400).floor().cast("int64")

        data_types[node["outputs"][0]] = "int"
        return e

    @staticmethod
    def edate(table, node, data_types, inter_exps):
        if node["inputs"].__len__() != 2:
            raise Exception("EDATE function requires 2 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][1])
        # Add months interval and cast back to date so downstream
        # functions (e.g. DAYS) receive a proper date type, not interval
        e = (e1.cast("date") + e2.cast(dt.Interval("M"))).cast("date")
        data_types[node["outputs"][0]] = "date"
        return e

    @staticmethod
    def hours(table, node, data_types, inter_exps):
        if node["inputs"].__len__() != 1:
            raise Exception("HOURS function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        e = e.cast("time").hour()
        data_types[node["outputs"][0]] = "int"
        return e

    @staticmethod
    def minutes(table, node, data_types, inter_exps):
        if node["inputs"].__len__() != 1:
            raise Exception("MINUTES function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        e = e.cast("time").minute()
        data_types[node["outputs"][0]] = "int"
        return e

    @staticmethod
    def seconds(table, node, data_types, inter_exps):
        if node["inputs"].__len__() != 1:
            raise Exception("SECONDS function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        e = e.cast("time").second()
        data_types[node["outputs"][0]] = "int"
        return e

    @staticmethod
    def time(table, node, data_types, inter_exps):
        if node["inputs"].__len__() == 3:
            hour = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
            minute = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][1])
            second = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][2])
            e = ibis.time(hour.cast("string") + ":" + minute.cast("string") + ":" + second.cast("string"))
            e = e.cast("time")
        elif node["inputs"].__len__() == 1:
            e = ibis.time(FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0]))
            e = e.cast("time")
        else:
            raise Exception("DATE function requires minimum 1 to 3 parameters")

        data_types[node["outputs"][0]] = "time"
        return e

    @staticmethod
    def hour(table, node, data_types, inter_exps):
        if node["inputs"].__len__() != 1:
            raise Exception("HOUR function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        e = e.cast("time").hour()
        data_types[node["outputs"][0]] = "int"
        return e

    @staticmethod
    def minute(table, node, data_types, inter_exps):
        if node["inputs"].__len__() != 1:
            raise Exception("MINUTE function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        e = e.cast("time").minute()
        data_types[node["outputs"][0]] = "int"
        return e

    @staticmethod
    def second(table, node, data_types, inter_exps):
        if node["inputs"].__len__() != 1:
            raise Exception("SECOND function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        e = e.cast("time").second()
        data_types[node["outputs"][0]] = "int"
        return e

    @staticmethod
    def datetime(table, node, data_types, inter_exps):
        if node["inputs"].__len__() == 6:
            e = ibis.literal(
                datetime.datetime(
                    DateTime.__num(node["inputs"][0]),
                    DateTime.__num(node["inputs"][1]),
                    DateTime.__num(node["inputs"][2]),
                    DateTime.__num(node["inputs"][3]),
                    DateTime.__num(node["inputs"][4]),
                    DateTime.__num(node["inputs"][5]),
                )
            )
            data_types[node["outputs"][0]] = "datetime"
        elif node["inputs"].__len__() == 1:
            e = ibis.datetime(FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0]))
        else:
            raise Exception("DATETIME function requires minimum 1 or 6 parameters")
        return e

    @staticmethod
    def isoweeknum(table, node, data_types, inter_exps):
        if node["inputs"].__len__() != 1:
            raise Exception("ISOWEEKNUM function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        # Use ibis week_of_year() instead of strftime('%V') which
        # is not supported on all backends (e.g. PostgreSQL)
        e = e.cast("date").week_of_year()
        data_types[node["outputs"][0]] = "int"
        return e

    @staticmethod
    def today(table, node, data_types, inter_exps):
        e = ibis.literal(datetime.date.today())
        data_types[node["outputs"][0]] = "date"
        return e

    def weekday(table, node, data_types, inter_exps):
        if node["inputs"].__len__() != 1:
            raise Exception("WEEKDAY function requires 1 parameter")

        # Build the expression for the input (could be literal or column expression)
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])

        # Ensure we are working with a timestamp/date expression (not a plain string)
        # If the parser already tracks types, prefer that. This is defensive:
        try:
            # Prefer ibis.extract which produces EXTRACT(DOW FROM <expr>) in generated SQL
            e = ibis.extract("dow", e)
        except Exception:
            # Fallback: cast to date then use strftime safely and cast to int
            # (this fallback kept for environments where `extract` isn't available)
            e = e.cast("date").strftime("D").cast("int")

        data_types[node["outputs"][0]] = "int"
        return e

    @staticmethod
    def weeknum(table, node, data_types, inter_exps):
        if node["inputs"].__len__() != 1:
            raise Exception("WEEKNUM function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        e = e.strftime("%U").cast("int")
        data_types[node["outputs"][0]] = "int"
        return e

    @staticmethod
    def datetimediff(table, node, data_types, inter_exps):
        if node["inputs"].__len__() != 3:
            raise Exception("DATETIMEDIFF function requires 3 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][1])
        e = e1.cast("datetime") - e2.cast("datetime")
        e = e.cast("double")
        d_div = 86400.0
        m_div = d_div * 30.0
        y_div = d_div * 365.0
        if node["inputs"][2] == '"D"':
            e = e / d_div
            e = e.floor()
            e = e.cast("int")
        elif node["inputs"][2] == '"M"':
            e = e / m_div
            e = e.floor()
            e = e.cast("int")
        elif node["inputs"][2] == '"Y"':
            e = e / y_div
            e = e.floor()
            e = e.cast("int")
        else:
            raise Exception("DATETIMEDIFF function requires supports only D, M, Y as 3rd parameter")
        data_types[node["outputs"][0]] = "numeric"
        return e

    @staticmethod
    def datediff(table, node, data_types, inter_exps):
        if node["inputs"].__len__() != 3:
            raise Exception("DATEDIFF function requires 3 parameters")
        e1 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        e2 = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][1])
        e = e1.cast("date") - e2.cast("date")
        e = e.cast("double")
        m_div = 30.0
        y_div = 365.0
        if node["inputs"][2] == '"D"':
            e = e.floor()
            e = e.cast("int")
        elif node["inputs"][2] == '"M"':
            e = e / m_div
            e = e.floor()
            e = e.cast("int")
        elif node["inputs"][2] == '"Y"':
            e = e / y_div
            e = e.floor()
            e = e.cast("int")
        else:
            raise Exception("DATEDIF function requires supports only D, M, Y as 3rd parameter")
        data_types[node["outputs"][0]] = "numeric"
        return e

    @staticmethod
    def n(table, node, data_types, inter_exps):
        if node["inputs"].__len__() != 1:
            raise Exception("N function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        # PostgreSQL cannot directly cast boolean to int;
        # use ifelse for boolean expressions, direct cast otherwise
        try:
            if e.type().is_boolean():
                e = e.ifelse(1, 0)
            else:
                e = e.cast("int")
        except Exception:
            e = e.cast("int")
        data_types[node["outputs"][0]] = "numeric"
        return e

    @staticmethod
    def quarter(table, node, data_types, inter_exps):
        """Returns the quarter (1-4) from a date."""
        if node["inputs"].__len__() != 1:
            raise Exception("QUARTER function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        e = e.cast("date").quarter()
        data_types[node["outputs"][0]] = "int"
        return e

    @staticmethod
    def day_of_year(table, node, data_types, inter_exps):
        """Returns the day of the year (1-366) from a date."""
        if node["inputs"].__len__() != 1:
            raise Exception("DAY_OF_YEAR function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        e = e.cast("date").day_of_year()
        data_types[node["outputs"][0]] = "int"
        return e

    @staticmethod
    def epoch_seconds(table, node, data_types, inter_exps):
        """Returns the Unix timestamp (seconds since 1970-01-01)."""
        if node["inputs"].__len__() != 1:
            raise Exception("EPOCH_SECONDS function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        e = _bq_timestamp_cast(table, e).epoch_seconds()
        data_types[node["outputs"][0]] = "numeric"
        return e

    @staticmethod
    def strftime(table, node, data_types, inter_exps):
        """Formats a date/time according to a format string."""
        if node["inputs"].__len__() != 2:
            raise Exception("STRFTIME function requires 2 parameters: STRFTIME(date, format)")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        format_str = node["inputs"][1].strip('"').strip("'")
        e = e.strftime(format_str)
        data_types[node["outputs"][0]] = "string"
        return e

    @staticmethod
    def millisecond(table, node, data_types, inter_exps):
        """Returns the millisecond component from a timestamp."""
        if node["inputs"].__len__() != 1:
            raise Exception("MILLISECOND function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        e = _bq_timestamp_cast(table, e).millisecond()
        data_types[node["outputs"][0]] = "int"
        return e

    @staticmethod
    def microsecond(table, node, data_types, inter_exps):
        """Returns the microsecond component from a timestamp."""
        if node["inputs"].__len__() != 1:
            raise Exception("MICROSECOND function requires 1 parameter")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        e = _bq_timestamp_cast(table, e).microsecond()
        data_types[node["outputs"][0]] = "int"
        return e

    @staticmethod
    def date_trunc(table, node, data_types, inter_exps):
        """Truncates a timestamp to the specified unit (year, month, day, hour,
        minute, second)."""
        if node["inputs"].__len__() != 2:
            raise Exception("DATE_TRUNC function requires 2 parameters: DATE_TRUNC(date, unit)")
        e = FormulaSQLUtils.build_ibis_expression(table, data_types, inter_exps, node["inputs"][0])
        unit = node["inputs"][1].strip('"').strip("'").strip().upper()
        # Map full unit names to ibis truncate codes
        unit_map = {
            "YEAR": "Y",
            "YEARS": "Y",
            "Y": "Y",
            "QUARTER": "Q",
            "Q": "Q",
            "MONTH": "M",
            "MONTHS": "M",
            "M": "M",
            "WEEK": "W",
            "WEEKS": "W",
            "W": "W",
            "DAY": "D",
            "DAYS": "D",
            "D": "D",
            "HOUR": "h",
            "HOURS": "h",
            "H": "h",
            "MINUTE": "m",
            "MINUTES": "m",
            "SECOND": "s",
            "SECONDS": "s",
            "S": "s",
        }
        truncate_unit = unit_map.get(unit, unit)
        e = e.truncate(truncate_unit)
        data_types[node["outputs"][0]] = "timestamp"
        return e
