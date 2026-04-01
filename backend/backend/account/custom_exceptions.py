"""Custom exceptions for account module."""

from rest_framework import status
from rest_framework.exceptions import APIException


class Forbidden(APIException):
    """Exception raised when access is forbidden."""

    status_code = status.HTTP_403_FORBIDDEN
    default_detail = "Access forbidden."
    default_code = "forbidden"


class UserNotExistError(APIException):
    """Exception raised when user does not exist."""

    status_code = status.HTTP_404_NOT_FOUND
    default_detail = "User does not exist."
    default_code = "user_not_found"


class MethodNotImplemented(APIException):
    """Exception raised when method is not implemented."""

    status_code = status.HTTP_501_NOT_IMPLEMENTED
    default_detail = "Method not implemented."
    default_code = "not_implemented"
