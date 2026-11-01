import time
from pathlib import Path

result_file = Path("task-result.txt")

print("Task started", flush=True)

time.sleep(10)

result_file.write_text(
    "Task completed successfully",
    encoding="utf-8"
)

print("Task completed", flush=True)
