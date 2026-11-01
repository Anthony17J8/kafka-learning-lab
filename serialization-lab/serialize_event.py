import json
from uuid import uuid4
from datetime import datetime, timezone


def create_event():
    return {
        "event_id": str(uuid4()),
        "event_type": "OrderCreated",
        "event_version": 1,
        "occurred_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "source": "order-service",
        "data": {
            "order_id": 123,
            "customer_id": 45,
            "amount_minor": 150000,
            "currency": "RUB"
        }
    }


event = create_event()

# Сериализация в JSON
json_string = json.dumps(
    event,
    ensure_ascii=False,
    allow_nan=False
)

# Кодирование в UTF-8
message_bytes = json_string.encode("utf-8")

print("Original type:", type(event))
print("JSON type:", type(json_string))
print("Bytes type:", type(message_bytes))
print("Bytes length:", len(message_bytes))

# Восстановление объекта
decoded_string = message_bytes.decode("utf-8")
restored_event = json.loads(decoded_string)

print("Restored event:", restored_event)

assert event == restored_event

print("Serialization successful!")
