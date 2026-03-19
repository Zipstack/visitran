from typing import Any

from rest_framework import status
from rest_framework.renderers import JSONRenderer
from rest_framework.response import Response

from backend.errors.error_codes import BackendErrorMessages


class VisitranBackendBaseException(Exception):
    """Base exception class for all visitran cloud exceptions.

    This exception is used to construct the error messages using error
    code and params
    """

    def __init__(self, error_code: str, http_status_code: int = status.HTTP_400_BAD_REQUEST, **msg_args: Any):
        self._error_msg: str = error_code or BackendErrorMessages.DEFAULT_ERROR_MSG
        self._status_code: int = http_status_code
        self._msg_args: dict[str, Any] = msg_args
        self._is_markdown: bool = True
        self._error_message: str = self.__load_error_message()

    def __load_error_message(self) -> str:
        for key, value in self._msg_args.items():
            if isinstance(value, (list, tuple, set)):
                self._msg_args[key] = ", ".join(map(str, value))
        return self._error_msg.format(**self._msg_args)

    def to_response(self) -> Response:
        """Convert exception to properly formatted DRF Response"""
        response = Response(
            data=self.error_response(), status=self._status_code, headers={"Content-Type": "application/json"}
        )

        response.accepted_renderer = JSONRenderer()
        response.accepted_media_type = "application/json"
        response.renderer_context = {}
        return response

    def __str__(self) -> str:
        return self._error_message

    def error_args(self) -> dict[str, Any]:
        return self._msg_args

    @property
    def error_message(self) -> str:
        return self._error_message

    @property
    def status_code(self) -> int:
        return self._status_code

    @property
    def severity(self) -> str:
        return "Error"

    def error_response(self) -> dict[str, Any]:
        return {
            "status": "failed",
            "class": self.__class__.__name__,
            "error_message": self.error_message,
            "message_args": self._msg_args,
            "severity": self.severity,
            "is_markdown": self._is_markdown,
        }
