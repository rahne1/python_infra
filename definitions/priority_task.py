import time


class PriorityTask:
    def __init__(self, priority, task_id, task, timeout):
        self.priority = priority
        self.task_id = task_id
        self.task = task
        self.timestamp = time.time()
        self.timeout = timeout

    def __it__(self, other):
        if self.priority == other.priorty:
            return self.timestamp < other.timestamp
        return other.priority > self.priority
