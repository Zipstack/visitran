from typing import Any

from visitran.errors.error_codes import ErrorCodeConstants


class VisitranBaseExceptions(Exception):
    """Base exception class for all visitran exceptions.

    This exception is used to construct the error messages using error
    code and params
    """

    def __init__(self, error_code: str, **kwargs: Any) -> None:
        self._error_code: str = error_code or ErrorCodeConstants.DEFAULT_ERROR_MSG
        self._msg_args: dict[str, Any] = kwargs
        self._error_msg: str = self.__load_error_message()
        self._is_markdown: bool = True

    def __load_error_message(self) -> str:
        for key, value in self._msg_args.items():
            if isinstance(value, (list, tuple, set)):
                self._msg_args[key] = ", ".join(map(str, value))
        return self._error_code.format(**self._msg_args)

    @property
    def severity(self) -> str:
        return "Error"

    @property
    def error_message(self) -> str:
        return self._error_msg

    def error_args(self):
        return self._msg_args

    def __str__(self) -> str:
        return self._error_msg

    def error_response(self) -> dict[str, Any]:
        response = {
            "status": "failed",
            "error_message": self.error_message,
            "message_args": self._msg_args,
            "severity": self.severity,
            "is_markdown": self._is_markdown,
        }
        if "is_rollback" in self._msg_args:
            response["is_rollback"] = self._msg_args["is_rollback"]
        return response
