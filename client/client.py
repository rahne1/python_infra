import socket
import json
import random
import time
from shared.encryption import encrypt_message, decrypt_message, add_hmac, verify_hmac


class Client:
    def __init__(self, server_host="localhost", server_port=5000):
        self.server_host = server_host
        self.server_port = server_port

    def add_task(self, task_description, priority=0, timeout=300):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(10)
                sock.connect((self.server_host, self.server_port))
                request = add_hmac(
                    {
                        "type": "add_task",
                        "task": task_description,
                        "priority": priority,
                        "timeout": timeout,
                    }
                )
                encrypted_request = encrypt_message(request)
                sock.sendall(encrypted_request)

                encrypted_response = sock.recv(1024)
                response = decrypt_message(encrypted_response)
                print(response)

                # Verify HMAC
                if not verify_hmac(response):
                    print("Invalid HMAC in server response")
                    return None

                # Handle server response
                if response["status"] == "ok":
                    print(
                        f"Task added: {response['task_id']} with priority {priority} and timeout {timeout}"
                    )
                    return response["task_id"]
                else:
                    print(
                        f"Failed to add task: {response.get('message', 'unknown error')}"
                    )
                    return None

        except Exception as e:

            print(f"Error occurred while adding task: {e}")
            return None

    def get_task_result(self, task_id):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.server_host, self.server_port))
                request = add_hmac({"type": "get_task_result", "task_id": task_id})
                encrypted_request = encrypt_message(request)
                sock.sendall(encrypted_request)

                encrypted_response = sock.recv(1024)
                response = decrypt_message(encrypted_response)

                if not verify_hmac(response):
                    print(f"Invalid HMAC for task {task_id}")
                    return None

                if response["status"] == "ok":
                    return response["result"]
                else:
                    print(
                        f"Failed to get result for task {task_id}: {response.get('message', 'unknown error')}"
                    )
                    return None
        except Exception as e:
            print(f"Error occurred while fetching result for task {task_id}: {e}")
            return None


if __name__ == "__main__":
    client = Client()
    priorities = [0, 1, 2]
    timeouts = [20, 40, 60]
    task_ids = []

    for i in range(10):
        priority = random.choice(priorities)
        timeout = random.choice(timeouts)
        task_id = client.add_task(f"task {i + 1}", priority, timeout)
        if task_id:
            task_ids.append(task_id)

    time.sleep(15)

    for task_id in task_ids:
        result = client.get_task_result(task_id)
        if result:
            print(f"task {task_id} result: {result}")
        else:
            print(f"task {task_id} has no result yet")
