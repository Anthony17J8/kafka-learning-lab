from multiprocessing import Process

counter = {"value": 0}


def worker():
    counter["value"] = 99
    print("Child process:", counter["value"])


if __name__ == "__main__":
    process = Process(target=worker)

    process.start()
    process.join()

    print("Parent process:", counter["value"])
