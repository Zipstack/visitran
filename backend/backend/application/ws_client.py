"""WebSocket client for OSS → AI server communication.

Used instead of Redis Streams when running in OSS mode
(VISITRAN_AI_KEY is configured, connecting to cloud AI service).

Uses the synchronous `websocket-client` library (not async `websockets`)
because the OSS backend runs under eventlet, whose monkey-patching
breaks asyncio-based WebSocket libraries.
"""

import json
import logging
import socket
import ssl
from typing import Any

from django.conf import settings

logger = logging.getLogger(__name__)

# Local-only error code: keys not set in .env
ERROR_CODE_NOT_CONFIGURED = 0


class AIServerError(Exception):
    """Error from AI server with user-friendly message.

    Server-originated errors arrive with ``user_message`` and ``is_warning``
    already set by the AI server.  This class passes them through as-is.
    Only Scenario 1 (keys not configured) is built locally.
    """

    def __init__(
        self,
        code: int = 0,
        reason: str = "",
        raw_message: str = "",
        is_warning: bool | None = None,
        user_message: str | None = None,
    ):
        self.code = code
        self.reason = reason
        self.raw_message = raw_message
        self.is_credit_error = False

        if user_message:
            # Server provided a formatted message — use it directly
            self.user_message = user_message
            self.is_warning = is_warning if is_warning is not None else False
            self.is_credit_error = is_warning and "credit" in user_message.lower()
        elif code == ERROR_CODE_NOT_CONFIGURED:
            # Scenario 1: local check — key not in .env
            self.user_message = self._not_configured_message()
            self.is_warning = True
        else:
            # Fallback (e.g. old AI server without user_message)
            self.user_message = reason or raw_message or "An unknown error occurred"
            self.is_warning = is_warning if is_warning is not None else False

        super().__init__(self.user_message)

    @staticmethod
    def _not_configured_message() -> str:
        platform_url = getattr(settings, "VISITRAN_PLATFORM_URL", "https://app.visitran.com")
        return (
            "**AI Features Not Configured**\n\n"
            "To use Visitran AI, you need an API key. "
            f"Sign up or log in at [{platform_url}]({platform_url}), "
            "then go to **Settings → API Keys** and generate a new key.\n\n"
            "Add the following to your `.env` file:\n"
            "```\n"
            "VISITRAN_AI_KEY=vtk_<your-key>\n"
            "```\n"
            "Restart the backend server to apply changes."
        )

    def to_error_payload(self) -> dict:
        """Return structured error payload for Socket.IO emission."""
        return {
            "status": "failed",
            "error_message": self.user_message,
            "error_code": self.code,
            "is_credit_error": self.is_credit_error,
            "is_warning": self.is_warning,
        }


def _get_ws_url() -> str:
    """Build WebSocket URL from AI_SERVER_BASE_URL."""
    base = settings.AI_SERVER_BASE_URL.rstrip("/")
    # Convert http(s) to ws(s)
    if base.startswith("https://"):
        return base.replace("https://", "wss://", 1) + "/ws"
    return base.replace("http://", "ws://", 1) + "/ws"


def is_ws_mode() -> bool:
    """Check if OSS WebSocket mode is active (API key configured)."""
    return bool(settings.VISITRAN_AI_KEY)


def _is_cloud() -> bool:
    """Check if running in cloud mode, handling string 'False' from env."""
    val = getattr(settings, "IS_CLOUD", False)
    if isinstance(val, str):
        return val.lower() not in ("false", "0", "")
    return bool(val)


def check_oss_api_key_configured() -> None:
    """Raise AIServerError if OSS mode lacks API key or AI server URL."""
    if _is_cloud():
        return

    if not is_ws_mode():
        raise AIServerError(code=ERROR_CODE_NOT_CONFIGURED)

    # Key is set but AI server URL is missing or still the default localhost
    base_url = getattr(settings, "AI_SERVER_BASE_URL", "")
    if not base_url or "localhost" in base_url or "127.0.0.1" in base_url:
        platform_url = getattr(settings, "VISITRAN_PLATFORM_URL", "https://app.visitran.com")
        raise AIServerError(
            is_warning=True,
            user_message=(
                "**AI Server URL Not Configured**\n\n"
                "Your API key is set, but `AI_SERVER_BASE_URL` is missing or pointing to localhost.\n\n"
                "Add the following to your `.env` file:\n\n"
                "```\n"
                "AI_SERVER_BASE_URL=https://globe.visitran.com/ai\n"
                "```\n\n"
                f"Visit [{platform_url}]({platform_url}) for more details."
            ),
        )


def _connection_error(error: Exception) -> AIServerError:
    """Classify a network/connection error into a user-friendly AIServerError.

    These are LOCAL errors (AI server unreachable), so messages are
    built here. Eventlet monkey-patches stdlib, so specific exception
    types (ssl.SSLError, socket.gaierror) may not match — we also
    classify by error string content.
    """
    err_str = str(error).lower()

    if isinstance(error, ssl.SSLError) or "ssl" in err_str or "certificate" in err_str:
        logger.error(f"SSL error connecting to AI server: {error}")
        return AIServerError(
            user_message=(
                "**SSL Connection Error**\n\n"
                "Could not establish a secure connection to the AI server. "
                "Please verify that `AI_SERVER_BASE_URL` in your `.env` uses the correct "
                "protocol (`https://`) and that your network allows SSL connections."
            )
        )

    if (
        isinstance(error, socket.gaierror)
        or "nodename" in err_str
        or "name or service not known" in err_str
        or "getaddrinfo" in err_str
    ):
        logger.error(f"DNS resolution failed for AI server: {error}")
        return AIServerError(
            user_message=(
                "**Cannot Resolve AI Server Address**\n\n"
                "The AI server hostname could not be resolved. "
                "Please check that `AI_SERVER_BASE_URL` in your `.env` is correct "
                "and that your network/DNS configuration is working."
            )
        )

    if isinstance(error, ConnectionRefusedError) or "connection refused" in err_str or "connect call failed" in err_str:
        return AIServerError(
            user_message=(
                "**AI Server Unavailable**\n\n"
                "Cannot connect to the AI server. Please ensure the Visitran AI server "
                "is running and the `AI_SERVER_BASE_URL` in your `.env` is correct."
            )
        )

    if "timed out" in err_str or "timeout" in err_str:
        return AIServerError(
            is_warning=True,
            user_message=(
                "**Request Timed Out**\n\n"
                "The AI server did not respond in time. This can happen with "
                "complex queries or during high load. Please try again."
            ),
        )

    # Unclassified — generic message
    logger.error(f"Unexpected WebSocket error ({type(error).__name__}): {error}")
    return AIServerError(
        user_message=(
            "**AI Connection Error**\n\n"
            f"An unexpected error occurred: {error}\n\n"
            "Please check your `.env` configuration and try again."
        )
    )


def _process_ws_messages(ws, payload: dict[str, Any]) -> None:
    """Read and dispatch messages from the AI server WebSocket."""
    while True:
        raw = ws.recv()
        if not raw:
            break

        msg = json.loads(raw)
        msg_type = msg.get("type")

        if msg_type == "done":
            logger.info("AI server completed processing")
            break
        elif msg_type == "error":
            code = msg.get("code", -1)
            message = msg.get("message", "Unknown error")
            logger.error(f"AI server error (code={code}): {message}")
            raise AIServerError(
                code=code,
                reason=message,
                user_message=msg.get("user_message"),
                is_warning=msg.get("is_warning"),
            )
        elif msg_type == "sql_exec":
            sql_result = _execute_local_sql(msg, payload)
            ws.send(
                json.dumps(
                    {
                        "type": "sql_result",
                        "request_id": msg.get("request_id"),
                        "result": sql_result,
                    }
                )
            )
        elif msg_type == "stream":
            _publish_stream_to_local_redis(msg)
        elif msg_type == "status":
            logger.info(f"AI server status: {msg.get('message')}")
        else:
            logger.warning(f"Unknown message type from AI server: {msg_type}")


def _handle_bad_status(e) -> AIServerError:
    """Convert WebSocketBadStatusException to AIServerError."""
    status_code = getattr(e, "status_code", None)
    logger.error(f"AI server rejected connection (HTTP {status_code}): {e}")
    platform_url = getattr(settings, "VISITRAN_PLATFORM_URL", "https://app.visitran.com")
    if status_code in (401, 403):
        return AIServerError(
            user_message=(
                "**Invalid API Key**\n\n"
                "Your API key was rejected by the AI server. "
                f"Please verify your `VISITRAN_AI_KEY` in your `.env` file, "
                f"or generate a new key at [{platform_url}]({platform_url}) "
                "under **Settings → API Keys**."
            )
        )
    if status_code and 500 <= status_code < 600:
        return AIServerError(
            is_warning=True,
            user_message=(
                "**AI Server Error**\n\n"
                f"The AI server returned HTTP {status_code}. "
                "This is usually a temporary issue. Please try again in a few minutes."
            ),
        )
    return AIServerError(
        user_message=(
            "**Connection Rejected**\n\n"
            f"The AI server rejected the connection (HTTP {status_code}). "
            "Please verify your `AI_SERVER_BASE_URL` and `VISITRAN_AI_KEY` "
            "in your `.env` file are correct."
        )
    )


def _send_prompt_ws(payload: dict[str, Any]) -> None:
    """Send prompt payload to AI server via WebSocket (synchronous).

    Uses websocket-client (sync) instead of websockets (async) because
    the OSS backend runs under eventlet whose monkey-patching breaks
    asyncio-based WebSocket libraries.
    """
    import websocket

    ws_url = _get_ws_url()
    headers = [f"Authorization: Bearer {settings.VISITRAN_AI_KEY}"]

    logger.info(f"Connecting to AI server via WebSocket: {ws_url}")

    ws = None
    try:
        ws = websocket.create_connection(ws_url, header=headers, timeout=300)
        logger.info("Connected to AI server")

        ws.send(json.dumps({"type": "prompt", **payload}))
        logger.info("Prompt sent to AI server via WebSocket")

        _process_ws_messages(ws, payload)

    except AIServerError:
        raise
    except websocket.WebSocketBadStatusException as e:
        raise _handle_bad_status(e)
    except websocket.WebSocketTimeoutException as e:
        logger.error(f"AI server request timed out: {e}")
        raise AIServerError(
            is_warning=True,
            user_message=(
                "**Request Timed Out**\n\n"
                "The AI server did not respond in time. This can happen with "
                "complex queries or during high load. Please try again."
            ),
        )
    except websocket.WebSocketConnectionClosedException as e:
        logger.error(f"AI server closed connection: {e}")
        raise AIServerError(
            is_warning=True,
            user_message=(
                "**Connection Lost**\n\n"
                "The connection to the AI server was interrupted. "
                "This is usually a temporary issue. Please try again."
            ),
        )
    except Exception as e:
        raise _connection_error(e)
    finally:
        if ws:
            try:
                ws.close()
            except Exception:
                pass


def _execute_local_sql(msg: dict, payload: dict) -> dict:
    """Execute SQL locally when AI server requests it."""
    sql = msg.get("sql", "")
    logger.info(f"Executing local SQL from AI server: {sql[:100]}...")
    # TODO: Wire to local execution context
    return {"status": "error", "message": "Local SQL execution not yet implemented"}


def _publish_stream_to_local_redis(msg: dict) -> None:
    """Bridge WebSocketStreamer responses into local Redis.

    The 'data' field is already a JSON string matching ChatStreamer's format.
    We write it to local Redis exactly as ChatStreamer would have:
        xadd(channel_id, {"data": payload_str, "type": "message"})

    The existing listen_to_redis_stream() picks it up and delivers to frontend.
    """
    try:
        from backend.core.redis_client import RedisClient

        redis_client = RedisClient().redis_client
        if redis_client:
            channel_id = msg.get("channel_id")
            data = msg.get("data")
            if channel_id and data:
                redis_client.xadd(channel_id, {"data": data, "type": "message"})
    except Exception as e:
        logger.warning(f"Failed to publish stream to local Redis: {e}")


def send_event_via_websocket(payload: dict[str, Any]) -> None:
    """Send event to AI server via WebSocket.

    Drop-in replacement for send_event_to_llm_server() in OSS mode.
    """
    try:
        _send_prompt_ws(payload)
    except AIServerError:
        raise  # Preserve AIServerError for structured handling
    except Exception as e:
        logger.error(f"WebSocket communication with AI server failed: {e}")
        raise
