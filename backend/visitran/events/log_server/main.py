import asyncio
import contextlib
import logging
import queue
import threading
import time
from collections.abc import Generator
from typing import Any

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from visitran.events.log_helper import get_queue

log_queue = None


logger = logging.getLogger("log_server")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s - %(message)s")
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

app = FastAPI(debug=True)
PORT = 5000

if not log_queue:
    logging.info("log_queue is None")
    log_queue = get_queue()
# logger.info(f"M_main_Q: {id(log_queue)}")


@app.get("/ping")
def ping() -> Any:
    return "pong"


@app.websocket("/ws")
async def websocket_endpoint(*, websocket: WebSocket) -> None:
    await websocket.accept()
    try:
        # logger.info("loop start")
        while True:
            time.sleep(0.25)
            logger.debug(f"main_Q: {id(log_queue)}")

            logger.debug("loop begin")
            try:
                message = log_queue.get_nowait()  # type: ignore[union-attr]
                logger.debug("got message")
            except queue.Empty:
                message = None
                logger.debug("no message")
            if message is not None:
                logger.debug("sending message")
                await websocket.send_text(message)
                logger.debug("sent message")

            # Hack to check if client is still connected
            # Current version of websockets does not support checking of state in loop
            try:
                logger.debug("recv")
                await asyncio.wait_for(websocket.receive_text(), timeout=0.001)
                logger.debug("recv success")
            except asyncio.TimeoutError:
                logger.debug("recv timeout")
            except Exception:
                logger.debug("recv failed")
                break

    except WebSocketDisconnect:
        # logger.info(f"web socket Disconnect: {err}")
        await websocket.close()
        # logger.info("web socket close")

    # logger.info("Done.")
    await websocket.close()


class Server(uvicorn.Server):
    def install_signal_handlers(self) -> None:
        """This method is currently empty because signal handling is not
        required for this application.

        If signal handling becomes necessary in the future, the
        appropriate code can be added here.
        """
        pass

    @contextlib.contextmanager
    def run_in_thread(self) -> Generator[None, None, None]:
        thread = threading.Thread(target=self.run, name="uvicorn")
        thread.start()
        try:
            while not self.started:
                time.sleep(1e-3)
            yield
        finally:
            self.should_exit = True
            thread.join()


config = uvicorn.Config(app, host="127.0.0.1", port=PORT, log_level="info", loop="asyncio")
WEBSOCKET_SERVER = Server(config=config)

if __name__ == "__main__":
    with WEBSOCKET_SERVER.run_in_thread():
        # logger.info("Started Server")
        while True:
            time.sleep(1)
