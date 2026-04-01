from collections.abc import Generator
from queue import Queue
from unittest.mock import patch

import portpicker
import pytest
import uvicorn
from requests import Session
from websockets.sync.client import connect

from visitran.events import Server, app
from visitran.events.log_helper import LogHelper


@pytest.fixture(scope="session")
def start_ws_server() -> Generator[int, None, None]:
    port = portpicker.pick_unused_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="info", loop="asyncio")
    wsserver = Server(config=config)
    with wsserver.run_in_thread():
        yield port


@pytest.fixture(scope="session")
def client() -> Session:
    return Session()


@pytest.mark.unit
@pytest.mark.timeout(5)
@pytest.mark.minimal_core
class TestLogWebSockerServer:
    def test_ding(self, start_ws_server: Generator[int, None, None], client: Session) -> None:
        port = start_ws_server
        url = f"http://localhost:{port}/ping"
        response = client.get(url)
        assert response.status_code == 200
        assert response.content == b'"pong"'

    def test_logs(
        self,
        start_ws_server: Generator[int, None, None],
    ) -> None:
        port = start_ws_server
        # assert if get /ding endpoint works

        queue: Queue[str] = Queue()
        with (
            patch("visitran.events.log_helper.log_queue", new=queue),
            patch("visitran.events.log_server.main.log_queue", new=queue),
        ):
            with connect(f"ws://localhost:{port}/ws") as websock:
                ps1 = LogHelper.publish(message=LogHelper.log(message="Running mode.", level="INFO"))
                ps2 = LogHelper.publish(message=LogHelper.log(message="Execution failed.", level="ERROR"))
                assert ps1 is True
                assert ps2 is True
                # websock.send("Hello world!")
                message1 = websock.recv()
                message2 = websock.recv()
            assert message1 == '{"message": "Running mode.", "level": "INFO"}'
            assert message2 == '{"message": "Execution failed.", "level": "ERROR"}'
