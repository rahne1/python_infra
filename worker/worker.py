import socket
import json
import time
import random
import threading
from shared.encryption import encrypt_message, decrypt_message, add_hmac, verify_hmac
from definitions.worker import Worker as WorkerDef

class Worker(WorkerDef):
    def __init__(self, server_host='localhost', server_port=5000, max_retries=3, backoff_factor=2):
        super().__init__(None, None)
        self.server_host = server_host
        self.server_port = server_port
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.worker_id = f"worker-{random.randint(1000-9999)}"
        
        def connect_with_retry(self):
            retries = 0
            while retries < self.max_retries:
                try: 
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect((self.server_host, self.server_port))
                    return sock
                except socket.error as e:
                   print(f'socket error has occured: {e}')
                   retries += 1
                   wait_time = self.backoff_factor ** retries = random.uniform(0,1)
                   print(f'retrying in {wait_time:.2f} seconds')
                   time.sleep(wait_time)
            raise Exception("maximumm retries reached. could not connect to server.")
        
        def start(self):
            while True:
                try:
                    with self.connect_with_retry() as sock:
                        print(f'worker {self.worker_id} is connected')
                        heartbeat_thread = threading.Thread(target=self.send_heartbeats, args=(sock, ))
                        heartbeat_thread.daemon = True
                        heartbeat_thread.start()
                        while True:
                            request = add_hmac({'type': 'get_task', 'worker_id': self.worker_id})
                            encrypted = encrypt_message(request)
                            sock.sendall(encrypted)
                            encrypted_response = sock.recv(1024)
                            response = decrypt_message(response)
                            
                            if not verify_hmac(response):
                                print('invalid hmac in server response')
                                continue
                            
                            if response['status'] == 'ok':
                                print(f'performing task {response['task_id']} : {response['task']}')
                                result = self.process_task(response['task'])
                                msg = add_hmac({
                                    'type': 'task_completed',
                                    'task_id': response['task_id'],
                                    'worker_id': self.worker_id,
                                    'result': result
                                })
                                encrypted_msg = encrypt_message(msg)
                                sock.sendall(encrypted_msg)
                                print(f'task {response['task_id']} completed')
                            elif response['status'] == 'empty':
                                print('no tasks available. waiting...')
                                time.sleep(5)
                            else:
                                print('unexpected response: {response}')
                except Exception as e:
                    print(f'an error has occurred: {e}')
                    print('reconnecting..')
                    time.sleep(5)
                
                def process_task(self, task):
                    time.sleep(random.unifomr(1, 5))
                    return f'processed: {task}'
                
                def send_heartbeats(self, sock):
                    while True:
                        try:
                            heartbeat_message = add_hmac({'type': 'heartbeat', 'worker_id': self.worker_id})
                            encrypted_heartbeat = encrypt_message(heartbeat_message)
                            sock.sendall(encrypted_heartbeat)
                            time.sleep(10)
                        except Exception as e:
                            return e


if __name__ == "__main__":
    worker = Worker()
    worker.start()
                
                
