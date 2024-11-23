import json
import socket
import threading
import time
from collections import defaultdict
from definitions.task_queue import TaskQueue
from definitions.worker import Worker
from shared.encryption import add_hmac, decrypt_message, encrypt_message, verify_hmac


class TaskQueueServer:
    def __init__(self, host="0.0.0.0", port=5000):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.task_queue = TaskQueue()
        self.client_handlers = {}
        self.stats = defaultdict(int)
        self.stats_lock = threading.Lock()
        self.running = True  

    def check_worker_heartbeats(self):
        while self.running:
            try:
                current_time = time.time()
                with self.task_queue.lock:
                    for worker in self.task_queue.workers[:]:
                        if current_time - worker.last_heartbeat > 30:
                            print(f"worker {worker.address} timed out")
                            self.task_queue.remove_worker(worker)
                time.sleep(10)
            except Exception as e:
                print(f"Error in heartbeat checker: {e}")

    def start(self):
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind((self.host, self.port))
            self.sock.listen(100)
            print(f"server is listening on {self.host}:{self.port}")

            heartbeat_thread = threading.Thread(target=self.check_worker_heartbeats)
            heartbeat_thread.daemon = True
            heartbeat_thread.start()

            stats_thread = threading.Thread(target=self.print_stats)
            stats_thread.daemon = True
            stats_thread.start()

            while self.running:
                try:
                    client_socket, address = self.sock.accept()
                    print(f"New connection from {address}")
                    client_handler = threading.Thread(
                        target=self.handle_client, args=(client_socket, address)
                    )
                    client_handler.daemon = True
                    client_handler.start()
                    self.client_handlers[address] = client_handler
                except Exception as e:
                    print(f"Error accepting connection: {e}")

        except Exception as e:
            print(f"Server error: {e}")
            self.shutdown()

    def handle_client(self, client_socket, address):
        worker = None
        try:
            while True:
                try:
                    encrypted = client_socket.recv(1024)
                    if not encrypted:
                        print(f"Client {address} disconnected")
                        break

                    try:
                        data = decrypt_message(encrypted)
                        if data is None:
                            print(f"Decryption failed for {address}, no valid data.")
                            response = {
                                "status": "error",
                                "message": "decryption failed",
                            }
                    except Exception as e:
                        print(f"Decryption error from {address}: {e}")
                        response = {"status": "error", "message": "decryption error"}
                        break  

                    if not verify_hmac(data):
                        print(f"Invalid HMAC from {address}")
                        response = {"status": "error", "message": "invalid hmac"}
                    elif data["type"] == "add_task":
                        task_id = self.task_queue.add_task(
                            data.get("priority", 0),
                            data["task"],
                            data.get("timeout", 300),
                        )
                        response = {"status": "ok", "task_id": task_id}
                        with self.stats_lock:
                            self.stats["tasks_added"] += 1
                    elif data["type"] == "get_task":
                        if not worker:
                            worker = Worker(client_socket, address)
                            self.task_queue.add_worker(worker)
                            print(f"New worker registered: {address}")
                        task = self.task_queue.get_task()
                        if task:
                            worker.increment_task_count()
                            response = {
                                "status": "ok",
                                "task_id": task.task_id,
                                "task": task.task,
                            }
                            with self.stats_lock:
                                self.stats["tasks_assigned"] += 1
                        else:
                            response = {"status": "empty"}
                    elif data["type"] == "task_completed":
                        task_id = data.get("task_id")
                        if worker and task_id:
                            worker.decrement_task_count()
                            response = {"status": "ok"}
                            with self.stats_lock:
                                self.stats["tasks_completed"] += 1
                        else:
                            response = {
                                "status": "error",
                                "message": "invalid task completion",
                            }
                    elif data["type"] == "heartbeat":
                        if worker:
                            worker.update_heartbeat()
                        response = {"status": "ok"}
                    else:
                        response = {"status": "error", "message": "unknown type"}

                    try:
                        encrypted_response = encrypt_message(add_hmac(response))
                        client_socket.sendall(encrypted_response)
                    except Exception as e:
                        print(f"Error sending response to {address}: {e}")
                        break

                except socket.error as e:
                    print(f"Socket error with {address}: {e}")
                    break
                except Exception as e:
                    print(f"Error handling message from {address}: {e}")
                    break

        except Exception as e:
            print(f"Error handling client {address}: {e}")
        finally:
            if worker:
                print(f"Removing worker {address}")
                self.task_queue.remove_worker(worker)
            try:
                client_socket.close()
            except:
                pass
            self.client_handlers.pop(address, None)

    def print_stats(self):
        while self.running:
            try:
                time.sleep(60)
                with self.stats_lock:
                    print(f"Stats: {dict(self.stats)}")
            except Exception as e:
                print(f"Error printing stats: {e}")

    def shutdown(self):
        print("Shutting down server...")
        self.running = False
        for handler in self.client_handlers.values():
            handler.join(timeout=1)
        try:
            self.sock.close()
        except:
            pass
        print("Server shutdown complete")


if __name__ == "__main__":
    server = TaskQueueServer()
    try:
        server.start()
    except KeyboardInterrupt:
        print("\nShutdown requested...")
        server.shutdown()
