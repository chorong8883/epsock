import signal
import time
import iosock
import threading
import socket
from multiprocessing.pool import ThreadPool

clients = []

default_bytes = b'abcdefghijklamopqrst'
default_length = len(default_bytes)


cpu_count = 8
pool = ThreadPool(cpu_count)

bytes_count = default_length * 5 * cpu_count * 10000
results = []

def get_bytes(count:int):
    data_bytes = b''
    for _ in range(count):
        data_bytes += default_bytes
    return data_bytes
bufstart = time.time()
for _ in range(cpu_count):
    result = pool.apply_async(get_bytes, args=(int((bytes_count/cpu_count)/default_length),))
    results.append(result)
    
send_bytes = b''
for r in results:
    bs = r.get()
    send_bytes += bs
bufend = time.time()
send_length = len(send_bytes)
print(f"send_bytes : {send_length}")
print(f"buffer time elapsed: {bufend - bufstart} send_length: {send_length}")


def worker(client:iosock.Client) -> bool:
    limit = 1
    recv_data = b''
    start = time.time()
    for i in range(limit):
        send_bytes_index = 0
        try:
            while send_bytes_index < send_length:
                try:
                    sended_length = client.send(send_bytes[send_bytes_index:])
                    send_bytes_index += sended_length
                except BlockingIOError as e:
                    if e.errno == socket.EAGAIN:
                        pass
                    else:
                        raise e
        except Exception as e:
            print(e)
        print(f"{i:2}, send_data: {send_bytes_index}")
        recv_temp_data = recv_data
        try:
            while len(recv_temp_data) < send_length:
                try:
                    temp_data = client.recv()
                    recv_temp_data += temp_data
                except BlockingIOError as e:
                    if e.errno == socket.EAGAIN:
                        pass
                    else:
                        raise e
        except Exception as e:
            print(e)

        if send_length == len(recv_temp_data):
            print(f"{i:2}, recv_data: {len(recv_temp_data)}")
            pass
        else:
            recv_data = recv_temp_data[send_length:]
            print(f"{i:2}, recv_data: {len(recv_temp_data)} pass : {len(recv_data)}")
    end = time.time()
    print('result time elapsed:', end - start)
    return True

    
def signal_handler(num_recv_signal, frame):
    print(f"Get Signal: {signal.Signals(num_recv_signal).name}")
    for c in clients:
        client:iosock.Client = c
        client.close()
        
if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGABRT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    worker_results = []
    for _ in range(128):
        client = iosock.Client()
        clients.append(client)
        client.connect('218.55.118.203', 59012)
        worker_result = pool.apply_async(worker, args=(client, ))
        worker_results.append(worker_result)
    
    for index, r in enumerate(worker_results):
        r.get()
        print(f"end {index}")
    
    pool.close()    
    pool.join()