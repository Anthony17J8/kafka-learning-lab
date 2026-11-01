import time
from concurrent.futures import ThreadPoolExecutor

def send_notification(order_id):
    print(f"START notification {order_id}", flush=True)

    time.sleep(3)

    print(f"END notification {order_id}", flush=True)


if __name__ == "__main__":
    start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=3) as executor:

        future1 = executor.submit(send_notification, 101)
        future2 = executor.submit(send_notification, 102)
        future3 = executor.submit(send_notification, 103)

        print("All tasks submitted", flush=True)

    elapsed = time.perf_counter() - start

    print(f"Total time: {elapsed:.2f}s")
