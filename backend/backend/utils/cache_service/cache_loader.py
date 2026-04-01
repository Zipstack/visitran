import logging

Logger = logging.getLogger(__name__)

try:
    from pluggable_apps.utils.cache_service import CacheService
    Logger.info("Pluggable CacheService Imported(Using Enterprise) ")
except ImportError:
    Logger.info("Failed to import Pluggable CacheService(Using OSS")
    from backend.utils.cache_service.oss_cache import OssCacheService as CacheService
