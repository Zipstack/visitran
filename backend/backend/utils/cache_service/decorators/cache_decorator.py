# common/cache/decorators.py
import logging
from functools import wraps

from django.http import JsonResponse

from backend.utils.cache_service.cache_loader import CacheService

Logger = logging.getLogger(__name__)


def cache_response(key_prefix: str, key_params: list[str]):
    def decorator(view_func):
        @wraps(view_func)
        def _wrapped(view_or_request, *args, **kwargs):
            # Determine if CBV or FBV
            if hasattr(view_or_request, "request"):
                # CBV
                request = view_or_request.request
            else:
                # FBV
                request = view_or_request

            # Construct cache key
            key_parts = [key_prefix]
            for k in key_params:
                if k in kwargs:
                    key_parts.append(str(kwargs[k]))
                elif k in request.GET:
                    key_parts.append(str(request.GET[k]))
                elif k in request.POST:
                    key_parts.append(str(request.POST[k]))
            cache_key = "_".join(key_parts)
            Logger.info(f"Cache prefix {key_prefix} with key :{cache_key}")
            # Try to fetch from cache
            cached_data = CacheService.get_key(cache_key)
            if cached_data:
                Logger.info(f"Cache-hit for key {cache_key}")
                return JsonResponse(cached_data, safe=False)

            Logger.info(f"Cache-miss for key {cache_key}")
            # Get actual response
            response = view_func(view_or_request, *args, **kwargs)

            method = getattr(request, "method", "").upper()
            status_code = getattr(response, "status_code", None)

            # Cache only if response is 200 and JSON serializable
            if response and method == "GET" and status_code == 200:
                # For DRF responses
                if hasattr(response, "data"):
                    CacheService.set_key(cache_key, response.data)
                # For Django JsonResponse
                elif hasattr(response, "content"):
                    try:
                        import json

                        content_data = json.loads(response.content)
                        CacheService.set_key(cache_key, content_data)
                    except Exception:
                        pass

            return response

        return _wrapped

    return decorator


def clear_cache(patterns: list[str]):
    def decorator(view_func):
        @wraps(view_func)
        def wrapped(view_or_request, *args, **kwargs):
            request = view_or_request if hasattr(view_or_request, "method") else args[0]
            response = None

            try:
                response = view_func(view_or_request, *args, **kwargs)
                method = getattr(request, "method", "").upper()
                status_code = getattr(response, "status_code", None)

                if response and method in ["POST", "PUT", "DELETE"] and status_code in [200, 201, 204]:
                    try:
                        context = {
                            **kwargs,
                            **request.GET.dict(),
                            **getattr(request, "data", {}),
                        }

                        for pattern in patterns:
                            try:
                                resolved_pattern = pattern.format(**context)
                                Logger.info(f"Clearing cache for pattern: {resolved_pattern}")
                                CacheService.clear_cache(resolved_pattern)
                            except KeyError as e:
                                Logger.warning(f"Missing context variable in pattern '{pattern}': {e}")
                            except Exception as e:
                                Logger.exception(f"Failed to clear cache for pattern {pattern}")

                    except Exception as e:
                        Logger.exception("Error during cache clearing block")

            except Exception as e:
                Logger.exception("Error executing view function")

            return response

        return wrapped

    return decorator
