from functools import wraps
from django.http import JsonResponse
from abc import ABC, abstractmethod


class BasePermissionDecorator(ABC):
    """Abstract Base Class for permission decorators."""


    @abstractmethod
    def has_permission(self, request, view_func):
        """Subclasses must implement this method to define permission logic."""
        pass

