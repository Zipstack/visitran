"""Constants for reserved project names and validation."""

from visitran.constants import BaseConstant


class ProjectNameConstants(BaseConstant):
    """Constants for project name validation.
    
    Attributes:
        RESERVED_NAMES (set): Set of reserved project names that cannot be used.
    """
    
    RESERVED_NAMES = {
        'test',
        'visitran',
        'snowflake',
        'bigquery',
        'duckdb',
        'postgres',
        'trino',
        'django',
        'flask',
        'fastapi',
        'redis',
        'celery',
        'time'
    }
    
    @classmethod
    def is_reserved_name(cls, name: str) -> bool:
        """Check if a name is reserved.
        
        Args:
            name: The name to check
            
        Returns:
            bool: True if the name is reserved, False otherwise
        """
        return name.lower() in cls.RESERVED_NAMES
