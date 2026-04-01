import logging
import os
from typing import Any

from backend.core.web_socket import sio


def _emit_websocket_event(room: str, event: str, data: dict[str, Any]) -> None:
    """Emit websocket event
    Args:
        room (str): Room to emit event to
        event (str): Event name
        channel (str): Channel name
        data (bytes): Data to emit
    """
    payload = {"data": data}
    logging.info(f"data to emit to socket:  {data}")
    try:
        logging.debug(f"[{os.getpid()}] Push websocket event: {event}, {payload}")
        sio.emit(event, data=payload, room=room)
    except Exception as e:
        logging.error(f"Error emitting WebSocket event: {e}")


def handle_user_logs(room: str, event: str, message: dict[str, Any]) -> None:
    """Handle user logs from applications
    Args:
        message (dict[str, Any]): log message
    """

    if not room or not event:
        logging.warning(f"Message received without room and event: {message}")
        return

    # _store_execution_log(message)
    _emit_websocket_event(room, event, message)
