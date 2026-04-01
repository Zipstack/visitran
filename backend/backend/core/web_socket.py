import http
import logging
import os
import re
import traceback
from functools import wraps
from typing import Optional
from urllib.parse import parse_qs

import socketio
from django.conf import settings
from django.core.handlers.wsgi import WSGIHandler

from backend.core.routers.chat_message.constants import ChatMessageStatus
from backend.core.socket_session_manager import SocketSessionContext
from backend.core.utils import sanitize_data, redis_singleton_lock
from backend.core.redis_client import RedisClient
from backend.errors import SQLExtractionError
from backend.errors.visitran_backend_base_exceptions import VisitranBackendBaseException
from backend.server.django_conf import load_conf

load_conf()
redis_client = RedisClient()

sio: socketio.Server = socketio.Server(
    # Allowed values: {threading, eventlet, gevent, gevent_uwsgi}
    async_mode="threading",
    cors_allowed_origins="*",
    logger=False,
    engineio_logger=False,
    always_connect=True,
    client_manager=socketio.RedisManager(url=settings.SOCKET_IO_MANAGER_URL),
    # Connection health parameters - prevents dead connections from staying "alive"
    ping_interval=25,  # Send ping every 25 seconds (keeps connection alive through proxies)
    ping_timeout=60,   # Wait 60 seconds for pong response before disconnecting
    # Performance tuning
    max_http_buffer_size=1e8,  # 100MB for large messages (default 1MB)
    http_compression=True,      # Compress large payloads
    # Transport options - fallback to polling if WebSocket fails
    transports=['websocket', 'polling'],
)
logging.info(f"SocketIO server started with async_mode: {sio.async_mode}")
logging.info(f"SocketIO manager url : {settings.SOCKET_IO_MANAGER_URL}")
logging.info(f"SocketIO ping_interval: 25s, ping_timeout: 60s")
logging.info(f"SocketIO transports: ['websocket', 'polling']")

context_manager = SocketSessionContext()


def with_tenant_context(handler):
    @wraps(handler)
    def wrapper(sid, *args, **kwargs):
        from backend.utils.tenant_context import _get_tenant_context

        ctx = SocketSessionContext().get_context(sid)
        if ctx:
            tenant_ctx = _get_tenant_context()
            tenant_ctx.set_user(ctx.get("user"))
            tenant_ctx.set_tenant(ctx.get("tenant"), ctx.get("env"))

        return handler(sid, *args, **kwargs)

    return wrapper


@sio.event
def connect(sid: str, environ):
    _init_ai_socket_context(environ, sid)
    _init_logs_socket(environ, sid)


def _init_logs_socket(environ, sid):
    logging.info(f"[{os.getpid()}] Client with SID:{sid} connected")
    session_id = _get_user_session_id_from_cookies(sid, environ)
    if session_id:
        sio.enter_room(sid, session_id)
        sio.emit("session_id", {"session_id": session_id}, to=sid)
        logging.info(f"Entered room {session_id} for socket {sid}")


def _get_user_session_id_from_cookies(sid: str, environ: any) -> Optional[str]:
    """Get the user session ID from cookies.

    Args:
        sid (str): The socket ID of the client.
        environ (Any): The environment variables of the client.

    Returns:
        Optional[str]: The user session ID.
    """
    cookie_str = environ.get("HTTP_COOKIE")
    if not cookie_str:
        logging.warning(f"No cookies found in {environ} for the sid {sid}")
        return "Log_event_id"

    cookie = http.cookies.SimpleCookie(cookie_str)
    session_id = cookie.get(settings.SESSION_COOKIE_NAME)

    if not session_id:
        logging.warning(f"No session ID found in cookies for SID {sid}")
        return None

    return session_id.value


def _init_ai_socket_context(environ, sid):
    logging.info(f"Client {sid} connected{environ}")
    query = parse_qs(environ.get("QUERY_STRING", ""))
    user_id = query.get("user_id", [None])[0]
    proj_id = query.get("projectId", [None])[0]
    chat_id = query.get("chatId", [None])[0]
    org_id = query.get("organization_id", [None])[0]
    email = query.get("email", [None])[0]
    role = query.get("user_role", [None])[0]
    env = query.get("env", [None])[0]

    identifier = f"{org_id}_{proj_id}_{chat_id}"
    channel_id = redis_client.get(key=identifier, default=None)
    if channel_id:
        sio.enter_room(sid, channel_id)

    # Store in Singleton
    payload = {"id": user_id, "email": email, "username": email, "role": role}
    if not user_id:
        payload = {}
    context_manager.set_context(sid, user=payload, tenant=org_id, env=env)
    logging.info(f"[SOCKET] Connected SID {sid} user_name {email} - context stored.")


@sio.event
def disconnect(sid):
    logging.info(f"[disconnect] SID {sid} disconnecting. Active sessions: {context_manager.get_active_sessions()}")
    context_manager.clear_context(sid)
    logging.info(
        f"[SOCKET] Disconnected SID {sid} - context cleared. Remaining sessions: {context_manager.get_session_count()}")


@sio.on("stream_logs")
def stream_logs(sid, data):
    logging.info(f"[{os.getpid()}] Client with SID:{sid} connected")
    # session_id = _get_user_session_id_from_cookies(sid, environ)
    # if session_id:
    #     sio.enter_room(sid, session_id)
    #     sio.emit("session_id", {"session_id": session_id}, to=sid)
    #     logger.info(f"Entered room {session_id} for socket {sid}")
    # else:
    #     sio.disconnect(sid)


@sio.event
@with_tenant_context
def get_prompt_response(sid, data: dict):
    """This method is called from frontend when a prompt is given by the user
    from socket The prompt will be saved initially using chat API and then this
    method will be called to generate the prompt response, This method will
    internally call AI service and persist the response and thought_chain.

    :param sid: The current connected client address
    :param data: The payload (dict) from frontend, contains chatId,
        projectId, chatMessageId
    :return:
    """
    needed_args = ["chatId", "projectId", "chatMessageId", "channelId", "orgId"]
    missing_args = [arg for arg in needed_args if arg not in data]
    if missing_args:
        raise ValueError(f"Missing required arguments: {', '.join(missing_args)}")

    chat_id = data["chatId"]
    org_id = data["orgId"]
    project_id = data["projectId"]
    chat_message_id = data["chatMessageId"]
    channel_id = data["channelId"]
    user_details = data["sessionDetails"]

    logging.info(f" Socket enabled to get the prompt response - {data}")
    identifier = f"{org_id}_{project_id}_{chat_id}"
    redis_client.set(key=identifier, value=channel_id, ex=1800)
    sio.enter_room(sid, channel_id)

    try:
        try:
            from pluggable_apps.subscriptions.context import CloudLLMServerContext as LLMServerContext
        except ImportError:
            from backend.application.context.llm_context import LLMServerContext
        from backend.utils.tenant_context import TenantContext, _get_tenant_context

        # Setting the organization id in tenant context for sockets
        tenant_context: TenantContext = _get_tenant_context()
        tenant_context.set_tenant(org_id)
        user_details["username"] = user_details.get("email", "")
        tenant_context.set_user(user_details)

        llm_context = LLMServerContext(project_id=project_id)
        llm_context.process_prompt(
            sid=sid,
            channel_id=channel_id,
            chat_id=chat_id,
            chat_message_id=chat_message_id,
            is_retry=False,
            org_id=org_id,
        )

        _get_and_stream_token_usage(channel_id, chat_id, chat_message_id, org_id, sid)

    except Exception as e:
        logging.critical(f"Error in get_prompt_response - {str(e)}")

        # Check if this is a structured AIServerError (auth/credit errors from AI server)
        from backend.application.ws_client import AIServerError
        ai_err = None
        if isinstance(e, AIServerError):
            ai_err = e
        elif hasattr(e, '__cause__') and isinstance(e.__cause__, AIServerError):
            ai_err = e.__cause__

        if ai_err:
            error_message = ai_err.to_error_payload()
        else:
            error_message = {
                "status": "failed",
                "error_message": str(e),
            }

        send_socket_message(
            sid=sid,
            channel_id=channel_id,
            chat_id=chat_id,
            chat_message_id=chat_message_id,
            error_msg_content=error_message,
            prompt_status=ChatMessageStatus.FAILED,
            error_state="Prompt Response",
        )


def _get_and_stream_token_usage(channel_id, chat_id, chat_message_id, org_id, sid):
    # Get token usage data for this organization and chat message
    token_usage_data = get_token_usage_data(org_id, chat_message_id, chat_id)
    # Send initial token usage data
    send_socket_message(
        sid=sid,
        channel_id=channel_id,
        chat_id=chat_id,
        chat_message_id=chat_message_id,
        token_usage_data=token_usage_data,
    )


def send_socket_message(sid, channel_id, **kwargs):
    allowed_args = [
        "chat_id",
        "chat_message_id",
        "thought_chain_content",
        "technical_content",
        "summary",
        "prompt_status",
        "transformation_status",
        "generate_model_list",
        "generate_model_status",
        "error_msg_content",
        "error_state",
        "bytes_data",
        "query_result",
        "is_sql_query_runnable",
        "is_retry_transform",
        "discussion_status",
        "token_usage_data",  # Add token usage data
    ]

    unsupported_args = [arg for arg in kwargs.keys() if arg not in allowed_args]

    if unsupported_args:
        raise ValueError(f"Unsupported arguments passed: {', '.join(unsupported_args)}")

    logging.info(
        f"Sending thought chain to frontend in channel: {kwargs.get('channel_id')} "
        f"and the content: {kwargs.get('prompt_status')}"
    )
    kwargs["event_type"] = "prompt_response"
    kwargs["error_msg"] = kwargs.get("error_msg_content")
    kwargs["thought_chain"] = kwargs.get("thought_chain_content")

    # Remove keys where value is None
    data = {key: value for key, value in kwargs.items() if value is not None}

    sio.emit(
        event=channel_id,
        data=data,
        to=channel_id,
    )


def get_token_usage_data(organization_id: str, chat_message_id: str, chat_id: str = None):
    """Get token usage data for a specific chat message and organization.

    Returns balance info and token consumption data.
    """
    try:
        # Import here to avoid circular imports
        from pluggable_apps.subscriptions.services.token_service import TokenBalanceService
        from pluggable_apps.subscriptions.models.token_balance import TokenUsageHistory
        from backend.core.models.organization_model import Organization

        # Get organization
        organization = Organization.objects.get(organization_id=organization_id)

        # Get current balance for the organization
        balance_info = TokenBalanceService.get_balance_info(organization)

        # Get token consumption for this specific chat message using foreign key columns
        token_usage = TokenUsageHistory.objects.filter(
            organization=organization,
            chat_message_id=chat_message_id
        ).first()

        # Prepare response data
        token_data = {
            "organization_id": organization_id,
            "remaining_balance": balance_info['current_balance'],
            "total_consumed": balance_info['total_consumed'],
            "total_purchased": balance_info['total_purchased'],
            "utilization_percentage": balance_info.get('utilization_percentage', 0),
            "message_tokens_consumed": token_usage.tokens_used if token_usage else 0,
            "token_usage_found": token_usage is not None
        }

        return token_data
    except ImportError:
        pass
    except Exception as e:
        logging.error(f"Error in get_token_usage_data: {str(e)}")
        logging.error(traceback.format_exc())
        return None


@sio.on("transformation_applied")
@with_tenant_context
@redis_singleton_lock(ttl=600)
def handle_transformation_applied(sid, data):
    """Handle the transformation applied event from frontend :param sid: The
    current connected client address :param data: The payload from frontend
    containing channelId, chatId, chatMessageId, projectId."""
    chat_id = data["chatId"]
    project_id = data["projectId"]
    chat_message_id = data["chatMessageId"]
    channel_id = data["channelId"]
    org_id = data["orgId"]
    send_socket_message(
        sid=sid,
        channel_id=channel_id,
        chat_id=chat_id,
        chat_message_id=chat_message_id,
        transformation_status=ChatMessageStatus.RUNNING,
    )

    # Import here to avoid circular imports
    from backend.utils.tenant_context import TenantContext, _get_tenant_context
    try:
        from pluggable_apps.subscriptions.context import CloudLLMServerContext as LLMServerContext
    except ImportError:
        from backend.application.context.llm_context import LLMServerContext

    # Handle the transformation
    chat_message_context = LLMServerContext(project_id=project_id)
    try:

        logging.info("\n=== Processing Transformation ===\n")

        # Setting the organization id in tenant context for sockets
        tenant_context: TenantContext = _get_tenant_context()
        tenant_context.set_tenant(org_id)

        result = chat_message_context.transformation_save(
            chat_id=chat_id, chat_message_id=chat_message_id, channel_id=channel_id, sid=sid
        )
        logging.info(f"\nTransformation result: {result}")

        _get_and_stream_token_usage(channel_id, chat_id, chat_message_id, org_id, sid)
    except Exception as error_message:
        logging.critical(f"Error handling transformation: {str(error_message)}")
        logging.critical(traceback.format_exc())
        chat_message_context.persist_transformation_status(
            chat_message_id, ChatMessageStatus.FAILED, {"error_message": str(error_message)}
        )


@sio.on("transformation_retry")
def transform_retry(sid, data):
    """Handle the transformation applied event from frontend :param sid: The
    current connected client address :param data: The payload from frontend
    containing channelId, chatId, chatMessageId, projectId."""
    from backend.application.context.chat_message_context import ChatMessageContext
    from backend.utils.tenant_context import TenantContext, _get_tenant_context

    chat_id = data["chatId"]
    project_id = data["projectId"]
    chat_message_id = data["chatMessageId"]
    channel_id = data["channelId"]
    org_id = data["orgId"]
    try:
        # Setting the organization id in tenant context for sockets
        tenant_context: TenantContext = _get_tenant_context()
        tenant_context.set_tenant(org_id)

        chat_message_context = ChatMessageContext(project_id=project_id)
        chat_message_context.transform_retry(
            sid=sid, channel_id=channel_id, project_id=project_id, chat_id=chat_id, chat_message_id=chat_message_id
        )

    except Exception as error_message:
        logging.critical(f"Error handling transformation: {str(error_message)}")
        logging.critical(traceback.format_exc())
        # Send error message back to frontend
        send_socket_message(
            sid=sid,
            channel_id=channel_id,
            chat_id=chat_id,
            chat_message_id=chat_message_id,
            error_msg_content={"error_message": str(error_message)},
            error_state="Transform Retry",
        )

    finally:
        _get_and_stream_token_usage(channel_id, chat_id, chat_message_id, org_id, sid)


@sio.on("stop_chat_ai")
@with_tenant_context
def stop_chat_ai(sid, data):
    """Stop entire chatAi flow trhough frontend :param sid: The current
    connected client address :param data: The payload from frontend containing
    channelId, chatId, chatMessageId, projectId."""
    chat_id = data["chatId"]
    project_id = data["projectId"]
    chat_message_id = data["chatMessageId"]
    channel_id = data["channelId"]
    org_id = data["orgId"]
    try:
        # Import here to avoid circular imports
        from backend.utils.tenant_context import TenantContext, _get_tenant_context
        from backend.application.context.chat_message_context import ChatMessageContext

        logging.info("\n=== STOP ChatAi ===\n")

        # Setting the organization id in tenant context for sockets
        tenant_context: TenantContext = _get_tenant_context()
        tenant_context.set_tenant(org_id)

        # Handle the transformation
        chat_message_context = ChatMessageContext(project_id=project_id)
        result = chat_message_context.stop_chat(
            chat_id=chat_id, chat_message_id=chat_message_id, channel_id=channel_id, sid=sid
        )
        return result
    except Exception as error_message:
        logging.critical(f"Error while trying to stop ChatAi: {str(error_message)}")
        logging.critical(traceback.format_exc())
    finally:
        chat_message_context.persist_prompt_status(chat_message_id, ChatMessageStatus.FAILED)
        _get_and_stream_token_usage(channel_id, chat_id, chat_message_id, org_id, sid)

        send_socket_message(
            sid=sid,
            channel_id=channel_id,
            chat_id=chat_id,
            chat_message_id=chat_message_id,
            prompt_status=ChatMessageStatus.FAILED,
            transformation_status=ChatMessageStatus.FAILED,
        )
        disconnect(sid)


def extract_sql_from_markdown_block(text: str) -> str:
    blocks = re.findall(r"```sql\s*(.*?)\s*```", text, re.DOTALL | re.IGNORECASE)
    if blocks:
        return "\n\n".join(block.strip() for block in blocks) if blocks else text.strip()
    # 2. If no fenced block, try to match SQL-like statements
    sql_keywords = r"(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|WITH)\s"
    match = re.search(rf"({sql_keywords}[\s\S]+?)(?=$|\n\s*\n)", text, re.IGNORECASE)
    if match:
        sql = match.group(1).strip()
        return f"```sql\n{sql}\n```"

    # 3. If nothing found
    raise SQLExtractionError(text)


@sio.on("run_sql_query")
@with_tenant_context
def run_sql_query(sid, data):
    chat_id = data["chatId"]
    project_id = data["projectId"]
    chat_message_id = data["chatMessageId"]
    channel_id = data["channelId"]
    org_id = data["orgId"]
    try:
        # Import here to avoid circular imports
        from backend.utils.tenant_context import TenantContext, _get_tenant_context
        from backend.application.context.chat_message_context import ChatMessageContext

        send_socket_message(
            sid=sid,
            channel_id=channel_id,
            chat_id=chat_id,
            chat_message_id=chat_message_id,
            transformation_status=ChatMessageStatus.RUNNING,
        )

        logging.info("\n=== Executing SQL query ===\n")

        # Setting the organization id in tenant context for sockets
        tenant_context: TenantContext = _get_tenant_context()
        tenant_context.set_tenant(org_id)

        chat_message_context = ChatMessageContext(project_id=project_id)
        # Get the chat message using existing function
        chat_message = chat_message_context._get_chat_message(chat_id=chat_id, chat_message_id=chat_message_id)
        # Usage
        content = extract_sql_from_markdown_block(chat_message.response)
        result = chat_message_context.execute_sql_command(sql_command=content)
        sanitized_result = sanitize_data(result)
        chat_message_context.persist_transformation_status(chat_message_id, ChatMessageStatus.SUCCESS)
        send_socket_message(
            sid=sid,
            channel_id=channel_id,
            chat_id=chat_id,
            chat_message_id=chat_message_id,
            query_result=sanitized_result,
            transformation_status=ChatMessageStatus.SUCCESS,
        )
    except VisitranBackendBaseException as error_message:
        logging.error(f" Error running SQL query: {str(error_message)} ")
        chat_message_context.persist_transformation_status(
            chat_message_id, ChatMessageStatus.FAILED, {"error_message": str(error_message)}
        )
        send_socket_message(
            sid=sid,
            channel_id=channel_id,
            chat_id=chat_id,
            chat_message_id=chat_message_id,
            error_msg_content={"error_message": str(error_message)},
            error_state="SQL RUN",
            transformation_status=ChatMessageStatus.FAILED,
        )
    except Exception as error_message:
        logging.critical(f"Error running SQL query: {str(error_message)}")
        chat_message_context.persist_transformation_status(
            chat_message_id, ChatMessageStatus.FAILED, {"error_message": str(error_message)}
        )
        send_socket_message(
            sid=sid,
            channel_id=channel_id,
            chat_id=chat_id,
            chat_message_id=chat_message_id,
            error_msg_content={"error_message": str(error_message)},
            error_state="SQL RUN",
            transformation_status=ChatMessageStatus.FAILED,
        )

    finally:
        _get_and_stream_token_usage(channel_id, chat_id, chat_message_id, org_id, sid)


def start_server(django_app: WSGIHandler, namespace: str) -> socketio.WSGIApp:
    django_app = socketio.WSGIApp(sio, django_app, socketio_path=namespace)
    return django_app


def run_socket_server():
    """Runs the Socket.IO server."""
    # Create a Socket.IO server instance
    from eventlet import listen, wsgi

    socket_app = socketio.WSGIApp(sio)
    wsgi.server(listen((settings.SOCKET_HOST, settings.SOCKET_PORT)), socket_app)


@sio.on("subscribe_channel")
@with_tenant_context
def subscribe_channel(sid, data: dict):
    """Lightweight subscription to join a channel (room) to receive future
    streamed tokens.

    Frontend should call this on reconnect/reload to join existing in-
    flight stream's room.
    """
    channel_id = data.get("channelId") or data.get("channel_id")
    if not channel_id:
        logging.warning(f"subscribe_channel called without channelId from sid:{sid}")
        return

    try:
        sio.enter_room(sid, channel_id)
        logging.info(f"[SOCKET] SID {sid} joined room {channel_id} via subscribe_channel")
        # optional ack back to the new sid only
        sio.emit(event="subscribe_ack", data={"channel_id": channel_id, "status": "joined"}, to=sid)
    except Exception as e:
        logging.error(f"Error in subscribe_channel for {channel_id}: {e}")
        sio.emit(event="subscribe_ack", data={"channel_id": channel_id, "status": "error", "error": str(e)}, to=sid)
