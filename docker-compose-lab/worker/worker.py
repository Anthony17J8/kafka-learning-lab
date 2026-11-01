import os
import time
from urllib.request import urlopen

url = os.environ.get(
    "API_URL", "http://api:8000"
) + "/health"

while True:
    try:
        with urlopen(url, timeout=3) as response:
            print(
                "worker -> api:",
                response.status,
                response.read().decode(),
                flush=True
            )
    except OSError as error:
        print("worker error:", error, flush=True)

    time.sleep(5)
