class FieldLengthConstants:
    """Used to determine length of fields in a model."""

    ORG_NAME_SIZE = 64
    CRON_LENGTH = 256
    UUID_LENGTH = 36
    # Not to be confused with a connector instance
    CONNECTOR_ID_LENGTH = 128
    ADAPTER_ID_LENGTH = 128

    #
    PROJECT_DESC_LENGTH = 500


class RequestHeader:
    """Request header constants."""

    X_API_KEY = "X-API-KEY"
    Authorization = "Authorization"


class DataTypeIcon:
    """Icon constant for database explorer"""

    STRING = "FontColorsOutlined"
    NUMBER = "NumberOutlined"
    TIME = "ClockCircleOutlined"
    DATE = "CalendarOutlined"
    DATETIME = "CalendarOutlined"
    TIMESTAMP = "CalendarOutlined"
    BOOLEAN = "CheckCircleOutlined"
    FLOAT = "NumberOutlined"
    DATABASE = "DatabaseOutlined"
    TABLE = "TableOutlined"
    SCHEMA = "ClusterOutlined"
