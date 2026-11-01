import json
from pathlib import Path

LOG_FILE = Path("events.jsonl")


def append_event(event):
    with LOG_FILE.open("a", encoding="utf-8") as file:
        file.write(
            json.dumps(event, ensure_ascii=False) + "\n"
        )


def read_events():
    if not LOG_FILE.exists():
        return []

    events = []

    with LOG_FILE.open("r", encoding="utf-8") as file:
        for line in file:
            if line.strip():
                events.append(json.loads(line))

    return events


append_event({
    "event_id": "evt-101",
    "event_type": "OrderCreated",
    "event_version": 1,
    "data": {"order_id": 101}
})

append_event({
    "event_id": "evt-102",
    "event_type": "OrderCreated",
    "event_version": 1,
    "data": {"order_id": 102}
})

for event in read_events():
    print(event)
