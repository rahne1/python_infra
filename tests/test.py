import threading
import time
from client.client import Client


def worker(client, num_tasks):
    try:
        for i in range(num_tasks):
            print(f"adding task {i}")  # Optionally, reduce verbosity in production
            client.add_task(f"load test task {i}", priority=1, timeout=300)
    except Exception as e:
        print(f"error in worker thread: {e}")


def run_load_test(num_clients, tasks_per_client):
    clients = [Client() for _ in range(num_clients)]
    threads = []
    lock = threading.Lock()  # Lock for managing output if needed

    start_time = time.time()
    for idx, client in enumerate(clients):
        # Using the lock to prevent jumbled outputs
        with lock:
            print(f"starting client {idx + 1}")
        thread = threading.Thread(target=worker, args=(client, tasks_per_client))
        threads.append(thread)
        thread.start()

    for idx, thread in enumerate(threads):
        thread.join()
        with lock:
            print(f"thread {idx + 1} joined")

    end_time = time.time()
    total_tasks = num_clients * tasks_per_client
    duration = end_time - start_time
    tasks_per_second = total_tasks / duration
    print(
        f"test completed.\ntotal tasks: {total_tasks}\nduration: {duration:.2f}s\ntasks per second: {tasks_per_second:.2f}"
    )


if __name__ == "__main__":
    print("processing test...")
    run_load_test(num_clients=10, tasks_per_client=100)
