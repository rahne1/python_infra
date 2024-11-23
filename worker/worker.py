import socket
import json
import time
import random
import threading
from shared.encryption import encrypt_message, decrypt_message, add_hmac, verify_hmac
from definitions.worker import Worker as WorkerDef


class Worker(WorkerDef):
    def __init__(
        self, server_host="localhost", server_port=5000, max_retries=3, backoff_factor=2
    ):
        super().__init__(None, None)
        self.server_host = server_host
        self.server_port = server_port
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.worker_id = f"worker-{random.randint(1000, 9999)}"
        self.running = True
        self.sock = None
        self.heartbeat_thread = None
        self._lock = threading.Lock()

    def connect_with_retry(self):
        retries = 0
        while retries < self.max_retries and self.running:
            try:
                # Create new socket
                if self.sock:
                    try:
                        self.sock.close()
                    except:
                        pass

                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect((self.server_host, self.server_port))
                return True
            except socket.error as e:
                print(f"Connection error: {e}")
                retries += 1
                if retries < self.max_retries:
                    wait_time = self.backoff_factor**retries + random.uniform(0, 1)
                    print(f"Retrying in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
        return False

    def send_message(self, message):
        with self._lock:
            try:
                if not self.sock:
                    return False
                hmac_message = add_hmac(message)
                encrypted = encrypt_message(hmac_message)
                self.sock.sendall(encrypted)
                return True
            except (socket.error, Exception) as e:
                print(f"Send error: {e}")
                return False
            
    def receive_message(self):
        try:
            if not self.sock:
                return None
            self.sock.settimeout(30)  # 30 second timeout
            encrypted_response = self.sock.recv(1024)
            if not encrypted_response:
                return None

            response = decrypt_message(encrypted_response)
            if not verify_hmac(response):
                print("Invalid HMAC in server response")
                return None
            return response
        except socket.timeout:
            print("Receive timeout")
            return None
        except Exception as e:
            print(f"Receive error: {e}")
            return None


    def send_heartbeats(self):
        while self.running:
            try:
                if self.sock:
                    heartbeat_msg = {"type": "heartbeat", "worker_id": self.worker_id}
                    if not self.send_message(heartbeat_msg):
                        break
                time.sleep(10)
            except Exception as e:
                print(f"Heartbeat error: {e}")
                break

    def process_task(self, task):
        try:
            time.sleep(random.uniform(1, 5))  
            return {"result": "processed"}

        except Exception as e:
            print(f"Task processing error: {e}")
            return f"Error processing task: {str(e)}"

    def process_invalid_response(self, retries):
        if retries >= self.max_retries:
            print(f"Too many invalid responses, skipping task.")
            return False  
        else:
            wait_time = self.backoff_factor ** retries + random.uniform(0, 1)
            print(f"Retrying task after {wait_time:.2f}s due to invalid response (attempt {retries+1})")
            time.sleep(wait_time)  
            return True

    def start(self):
        """Main worker loop"""
        self.running = True
        retries = 0  

        while self.running:
            try:
                if not self.connect_with_retry():
                    print("Failed to connect, retrying...")
                    time.sleep(5)
                    continue

                print(f"Worker {self.worker_id} connected")

                if (
                    self.heartbeat_thread is None
                    or not self.heartbeat_thread.is_alive()
                ):
                    self.heartbeat_thread = threading.Thread(
                        target=self.send_heartbeats
                    )
                    self.heartbeat_thread.daemon = True
                    self.heartbeat_thread.start()

                while self.running:
                    request = {"type": "get_task", "worker_id": self.worker_id}
                    if not self.send_message(request):
                        break

                    response = self.receive_message()
                    if not response:
                        break
                    
                    if 'status' not in response or 'task_id' not in response or 'task' not in response:
                        print(f"Invalid response: {response}")
                        if not self.process_invalid_response(retries):
                            break  
                        retries += 1  
                        continue  

                    retries = 0  

                    if response["status"] == "ok":
                        task_id = response['task_id']
                        task = response['task']
                        print(f"Processing task {task_id}: {task}")
                        result = self.process_task(task)

                        completion_msg = {
                            "type": "task_completed",
                            "task_id": task_id,
                            "worker_id": self.worker_id,
                            "result": result,
                        }
                        if not self.send_message(completion_msg):
                            break
                        print(completion_msg)
                        print(f"Task {task_id} completed")

                    elif response["status"] == "empty":
                        print("No tasks available, waiting...")
                        time.sleep(5)
                    else:
                        print(f"Unexpected response: {response}")

            except Exception as e:
                print(f"Worker error: {e}")
            finally:
                if self.sock:
                    try:
                        self.sock.close()
                    except:
                        pass
                    self.sock = None
                time.sleep(5)  

    def stop(self):
        self.running = False
        if self.sock:
            try:
                self.sock.close()
            except:
                pass
        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=1)


if __name__ == "__main__":
    worker = Worker()
    try:
        worker.start()
    except KeyboardInterrupt:
        print("\nShutdown requested...")
        worker.stop()
