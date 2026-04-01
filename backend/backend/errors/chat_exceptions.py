from rest_framework import status

from backend.errors.error_codes import BackendErrorMessages
from backend.errors.visitran_backend_base_exceptions import VisitranBackendBaseException


class ChatNotFound(VisitranBackendBaseException):
    """Raised if the given chat id is not found."""

    def __init__(self, chat_id: str):
        super().__init__(
            error_code=BackendErrorMessages.CHAT_NOT_FOUND,
            http_status_code=status.HTTP_404_NOT_FOUND,
            chat_id=chat_id,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class ChatMessageNotFound(VisitranBackendBaseException):
    """Raised if the chat message in given chat id is not found."""

    def __init__(self, chat_id: str, chat_name: str, chat_message_id: str):
        super().__init__(
            error_code=BackendErrorMessages.CHAT_MESSAGE_NOT_FOUND,
            http_status_code=status.HTTP_404_NOT_FOUND,
            chat_id=chat_id,
            chat_name=chat_name,
            chat_message_id=chat_message_id,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class InvalidChatPrompt(VisitranBackendBaseException):
    """Raised if the given chat prompt is invalid."""

    def __init__(self):
        super().__init__(
            error_code=BackendErrorMessages.INVALID_CHAT_PROMPT,
            http_status_code=status.HTTP_404_NOT_FOUND,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class InvalidChatMessageStatus(VisitranBackendBaseException):
    """Raised if the given chat message status is invalid."""

    def __init__(self, invalid_status: str, valid_status: list[str]):
        super().__init__(
            error_code=BackendErrorMessages.INVALID_CHAT_MESSAGE_STATUS,
            http_status_code=status.HTTP_404_NOT_FOUND,
            invalid_status=invalid_status,
            valid_status=valid_status,
        )

    @property
    def severity(self) -> str:
        return "Warning"


class InsufficientTokenBalance(VisitranBackendBaseException):
    """Raised when organization doesn't have sufficient token balance for the
    operation."""

    def __init__(self, tokens_required: int, tokens_available: float):
        super().__init__(
            error_code=BackendErrorMessages.INSUFFICIENT_TOKEN_BALANCE,
            http_status_code=status.HTTP_402_PAYMENT_REQUIRED,
            tokens_required=tokens_required,
            tokens_available=tokens_available,
        )

    @property
    def severity(self) -> str:
        return "Warning"
