import threading
from concurrent.futures import ThreadPoolExecutor

counter = {"value": 0}

lock = threading.Lock()
barrier = threading.Barrier(2)


def increment():
    barrier.wait()

    with lock:
        current = counter["value"]
        counter["value"] = current + 1


with ThreadPoolExecutor(max_workers=2) as executor:
    futures = [
        executor.submit(increment),
        executor.submit(increment)
    ]

    for future in futures:
        future.result()

print("Counter:", counter["value"])
