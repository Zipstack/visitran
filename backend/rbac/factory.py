import inspect

from django.views import View
from rest_framework import viewsets
from rest_framework.renderers import JSONRenderer
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from backend.rbac.oss_decorator import OSSPermissionDecorator


def handle_permission(view_func):
    """Returns the appropriate decorator based on cloud plugin availability."""
    try:
        from pluggable_apps.user_access_control.cloud_decorator import CloudPermissionDecorator

        permission_class = CloudPermissionDecorator
    except (ImportError, RuntimeError):
        # RuntimeError occurs when model's app is not in INSTALLED_APPS
        permission_class = OSSPermissionDecorator

    def wrapped_view(view_or_request, *args, **kwargs):
        _is_FBV = True

        if isinstance(view_or_request, (viewsets.ViewSet, APIView, View)):
            _is_FBV = False
            view_instance = view_or_request
            request = args[0] if args else None
            resource_name = getattr(view_instance, "RESOURCE_NAME", None)
        elif isinstance(view_or_request, Request):
            request = view_or_request
            module = inspect.getmodule(view_func)
            resource_name = getattr(module, "RESOURCE_NAME", None)
        else:
            raise TypeError("Invalid request type passed to the view")

        if not request:
            return Response({"error": "Invalid request"}, status=400)

        permission_instance = permission_class()

        if not permission_instance.has_permission(request, resource_name):
            response = Response(
                {
                    "error_message": "FORBIDDEN: Requested resource have limited permission for current user or user's role."
                },
                status=403,
            )
            response.accepted_renderer = JSONRenderer()
            response.accepted_media_type = "application/json"
            response.renderer_context = {}
            return response
        if _is_FBV:
            return view_func(request, **kwargs)
        else:
            return view_func(*args, **kwargs)

    return wrapped_view
