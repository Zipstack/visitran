# Streaming logs to frontend

Log Service opens a web socket for streaming logs.

## Pre-Requisities

- fastapi
- uvicorn
- websockets

### Installation

- Ensure that you've sourced your virtual environment, else create/source with

```bash
python -m venv .venv
source ./venv/bin/activate
```

### Running the server

- Execute the following command from `logs_service/` (log server is listening at port 5000)

```bash
python main.py
```

## Testing the Connection

- Try hitting the following endpoint from any socket client and check if the connection is established to the socket.

```bash
python -m websockets ws://localhost:5000/ws
```
