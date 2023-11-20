import iosock
import signal
import threading
import time
import math
server = iosock.Server()

def signal_handler(num_recv_signal, frame):
    print(f"\nGet Signal: {signal.Signals(num_recv_signal).name}")
    server.stop()

default_bytes = b'abcdefghijklamopqrst'
default_length = len(default_bytes)

def recv_threading():
    cpu_count = 8
    check_length = default_length * 5 * cpu_count * 10000
    print(f"check_length {check_length}")
    count = 0
    start = time.time()
    
    recv_data = {}
    while True:
        recv_temp_data = server.recv()
        if not recv_temp_data:
            break
        fileno = recv_temp_data['fileno']
        data = recv_temp_data['data']
        
        if fileno in recv_data:
            recv_data[fileno] += data
        else:
            recv_data[fileno] = data
        
        if check_length == len(recv_data[fileno]):
            print(f"recv {count:2}, total: {len(recv_data[fileno]):7,}")
            send_data = recv_data.pop(fileno)
            server.send(fileno, send_data)
        # elif (check_length/10)*8 < len(recv_data[fileno]):
        #     print(f"recv [{fileno}] {count:2}, total: {len(recv_data[fileno]):7,}")
        
        count += 1
    end = time.time()
    print(f'time elapsed: {end - start}')
    

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGABRT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    server.start('218.55.118.203', 59012)
    
    recv_thread = threading.Thread(target=recv_threading)
    recv_thread.start()
    
    server.join()
    
    
