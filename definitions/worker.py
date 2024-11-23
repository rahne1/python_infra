import time


class Worker:
    def __init__(self, socket, address):
        self.socket = socket
        self.address = address
        self.task_count = 0
        self.last_heartbeat = time.time()

    def increment_task_count(self):
        self.task_count += 1

    def decrement_task_count(self):
        self.task_count = max(0, self.task_count - 1)

    def update_heartbeat(self):
        self.last_heartbeat = time.time()
