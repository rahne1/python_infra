import time


class PriorityTask:
    def __init__(self, priority, task_id, task, timeout):
        self.priority = priority
        self.task_id = task_id
        self.task = task
        self.timestamp = time.time()
        self.timeout = timeout

    def __lt__(self, other):
        if self.priority == other.priority:
            return self.timestamp < other.timestamp
        return self.priority > other.priority
