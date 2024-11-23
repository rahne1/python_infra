import json
import socket
import threading
import time
import uuid
from collections import defaultdict

from definitions.task_queue import TaskQueue
from definitions.worker import Worker
from shared.encryption import (add_hmac, decrypt_message, encrypt_message,
                               verify_hmac)


class TaskQueueServer:
    def __init__(self, host="0.0.0.0", port=5000):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.task_queue = TaskQueue()
        self.client_handlers = {}
        self.stats = defaultdict(int)
        self.stats_lock = threading.Lock()

    def start(self):
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.listen(100)
        print(f"server is here: {self.host}:{self.port}")

        heartbeat_thread =threading.Thread(target=self.check_worker_heartbeats)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()

        stats_thread = threading.Thread(target=self.print_stats)
        stats_thread.daemon = True
        stats_thread.start()

        while True:
            client_socket, address= self.sock.accept()
            client_handler= threading.Thread(target=self.handle_client, args=(client_socket, address))
            client_handler.start()
            self.client_handlers[address] = client_handler

            def handle_client(self, client_socket, address):
                worker = None
                try:
                    while True:
                        encrypted = client_socket.recv(1024)
                        if not encrypted:
                            break

                        data = decrypt_message(encrypted)
                        if not verify_hmac(data):
                            response = {'status': 'error', 'message': 'invalid hmac'}
                        elif data['type'] == 'add_task':
                            task_id = self.task_queue.add_task(data.get('priority', 0), data['task'], data.get('timeout', 300))
                            with self.stats_lock:
                                self.stats['tasks_added'] += 1
                        elif data['type'] == 'get_task':
                            if not worker:
                                worker = Worker(client_socket, address)
                                self.task_queue.add_worker(worker)
                                task = self.task_queue.get_task()
                                if task: 
                                    worker.increment_task_count()
                                    response = {'status': 'ok', 'task_id' : task.task_id, 'task': task.task}
                                    with self.stats_lock:
                                        self.stats['tasks_assigned'] += 1 
                                else:
                                    response = {'status': 'empty'}
                        elif data['type'] == 'task_competed':
                            if worker:
                                worker.decrement_task_count()
                            response = {'status': 'ok'}
                            with self.stats_lock:
                                self.stats['tasks_completed'] += 1
                        elif data['type'] == 'heartbeat':
                            if worker:
                                worker.update_heartbeat()
                            response = {'status': 'ok'}
                        else:
                            response = {'status': 'error', 'message': 'unknown type'}

                        encrypted_response = encrypt_message(add_hmac(response))
                        client_socket.sendall(encrypted_response)
                except Exception as e:
                    print(f'error handling client {address}:{e}')
                finally:
                    if worker:
                        self.task_queue.remove_worker(worker)
                    client_socket.close()
                    del self.client_handlers[address]
            
            def check_worker_heartbeats(self):
                while True:
                    current_time = time.time()
                    with self.task_queue.lock:
                        for worker in self.task_queue.workers[:]:
                            if current_time - worker.last_heartbeat > 30:
                                print(f'worker {worker.address} timed out')
                            self.task_queue.remove_worker(worker)
                    time.sleep(10)
            
            def print_stats(self):
                while True:
                    time.sleep(60)
                    with self.stats_lock:
                        print(f'stats: {dict(self.stats)}')
                        
            def shutdown(self):
                for handler in self.client_handlers.values():
                    handler.join()
                self.sock.close()
                
if __name__ == "__main__":
    server = TaskQueueServer()
    try:
        server.start()
    except KeyboardInterrupt:
        print('bye')
        server.shutdown()
    


                
        
