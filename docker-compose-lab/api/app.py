from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock
import json

DATA_FILE = Path("/data/events.jsonl")
DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
file_lock = Lock()


class Handler(BaseHTTPRequestHandler):

    def reply(self, status, data):
        body = json.dumps(data).encode("utf-8")

        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self.reply(200, {"status": "ok"})

        elif self.path == "/events":
            with file_lock:
                events = (
                    [
                        json.loads(line)
                        for line in DATA_FILE.read_text().splitlines()
                    ]
                    if DATA_FILE.exists()
                    else []
                )
            self.reply(200, events)

        else:
            self.reply(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/events":
            return self.reply(404, {"error": "not found"})

        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 4096:
                return self.reply(400, {"error": "invalid length"})
            event = json.loads(self.rfile.read(length))
            if not isinstance(event, dict):
                return self.reply(400, {"error": "expected JSON object"})
        except ValueError:
            return self.reply(400, {"error": "invalid JSON"})

        with file_lock:
            with DATA_FILE.open("a", encoding="utf-8") as file:
                file.write(json.dumps(event) + "\n")

        self.reply(201, {"stored": True})


print("API listening on 0.0.0.0:8000", flush=True)

ThreadingHTTPServer(
    ("0.0.0.0", 8000), Handler
).serve_forever()
