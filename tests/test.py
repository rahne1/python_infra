import threading
import time
from client.client import Client

def worker(client, num_tasks):
    for i in range(num_tasks):
        client.add_task(f'load test task {i}', priority=1, timeout=300)
        
def run_load_test(num_clients, tasks_per_client):
    clients = [Client() for _ in range(num_clients)]
    threads = []
    
    start_time = time.time()
    for client in clients:
        thread = threading.Thread(target=worker, args=(client,tasks_per_client))
        threads.append(thread)
        thread.start()
        
    for thread in threads:
        thread.join()
    
    end_time = time.time()
    total_tasks = num_clients * tasks_per_client
    duration = end_time - start_time
    tasks_per_second = total_tasks / duration
    print(f'test completed.\ntotal tasks: {total_tasks}\nduration: {duration:2f}\ntasks per second: {tasks_per_second:2f}')
    
if __name__ == '__main__':
    run_load_test(num_clients=10, tasks_per_client=100)
    