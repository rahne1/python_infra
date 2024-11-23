import heapq
import time
import json
import os
import threading
import uuid
from .priority_task import PriorityTask


class TaskQueue:
    def __init__(self, persistence_file="tasks.json"):
        self.tasks = []
        self.workers = []
        self.lock = threading.Lock()
        self.persistence_file = persistence_file
        self.load_tasks()

    def add_task(self, priority, task, timeout=300):
        task_id = str(uuid.uuid4())
        with self.lock:
            heapq.heappush(self.tasks, PriorityTask(priority, task_id, task, timeout))
            self.save_tasks()
        return task_id

    def get_task(self):
        with self.lock:
            while self.tasks:
                task = heapq.heappop(self.tasks)
                if time.time() - task.timestamp <= task.timeout:
                    return task
                print(f"task {task.task_id} timed out and has been discarded.")
            return None

    def add_worker(self, worker):
        with self.lock:
            self.workers.append(worker)

    def remove_worker(self, worker):
        with self.lock:
            self.workers.remove(worker)

    def get_free_worker(self):
        with self.lock:
            return (
                min(self.workers, key=lambda worker_tasks: worker_tasks.task_count)
                if self.workers
                else None
            )

    def save_tasks(self):
        with open(self.persistence_file, "w") as f:
            json.dump(
                [
                    {
                        "priority": task.priority,
                        "task_id": task.task_id,
                        "task": task.task,
                        "timestamp": task.timestamp,
                        "timeout": task.timeout,
                    }
                    for task in self.tasks
                ],
                f,
            )

    def load_tasks(self):
        if os.path.exists(self.persistence_file):
            with open(self.persistence_file, "r") as f:
                tasks_data = json.load(f)
                for task_data in tasks_data:
                    task = PriorityTask(
                        task_data["priority"],
                        task_data["task_id"],
                        task_data["task"],
                        task_data["timeout"],
                    )
                    task.timestamp = task_data["timestamp"]
                    heapq.heappush(self.tasks, task)
