import threading
from concurrent.futures import ThreadPoolExecutor

counter = {"value": 0}

barrier = threading.Barrier(2)


def increment():
    current = counter["value"]

    # Оба потока должны дойти сюда
    barrier.wait()

    counter["value"] = current + 1


with ThreadPoolExecutor(max_workers=2) as executor:
    futures = [
        executor.submit(increment),
        executor.submit(increment)
    ]

    for future in futures:
        future.result()

print("Counter:", counter["value"])
