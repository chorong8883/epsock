import iosock
import signal
import threading
import time
import math
import errno
import multiprocessing
import ctypes
server = iosock.EpollServer()

starter = b'%w$d#'
closer = b'&sa@f#d$'


def signal_handler(num_recv_signal, frame):
    print(f"\nGet Signal: {signal.Signals(num_recv_signal).name}")
    server.close()
    # test_server.close()
    print("Server Close.")

import collections
count = collections.defaultdict(int)
recv_data = collections.defaultdict(bytes)
recvlen = collections.defaultdict(int)
locks = collections.defaultdict(threading.Lock)

def recv_callback(fileno, recv_bytes) -> list[bytes]:
    send_bytes = []
    if not fileno in count:
        count[fileno] = 0
    count[fileno] += 1

    if fileno in recv_data:
        recv_data[fileno] += recv_bytes
        recvlen[fileno] += len(recv_bytes)
    
    start_index = -1
    end_index = -1
    
    is_start = True
    is_len = True
    is_closer = True
    
    while is_start and is_len and is_closer:
        try:
            bit8_length = 1
            start_index = len(starter)
            end_index = len(starter)+bit8_length
            is_start = end_index <= len(recv_data[fileno]) and recv_data[fileno][:len(starter)] == starter
            length_of_length_bytes = recv_data[fileno][start_index:end_index]
            length_of_length = int.from_bytes(length_of_length_bytes, byteorder='little')
            
            start_index = end_index
            end_index = end_index + length_of_length
            is_len = end_index <= len(recv_data[fileno])
            
            length_bytes = recv_data[fileno][start_index:end_index]
            source_length = int.from_bytes(length_bytes, byteorder='little')
            
            start_index = end_index
            end_index = end_index+source_length
            is_closer = end_index+len(closer) <= len(recv_data[fileno]) and recv_data[fileno][end_index:end_index+len(closer)] == closer
        except IndexError:
            break
        
        if is_start and is_len and is_closer:
            send_bytes.append(recv_data[fileno][:end_index+len(closer)])
            
            recv_message_bytes:bytes = recv_data[fileno][start_index:end_index]
            # end = time.time()
            
            recv_bytes_replaced = recv_message_bytes.replace(b'abcdefghijklmnop qrstuvwxyz', b'')
            if recv_bytes_replaced != b'':
                recv_bytes_replaced = recv_message_bytes.replace(b'abcdefghijklmnop qrstuvwxyz', b'.')
            
                # time elapsed: {math.floor((end - time_recv_data[fileno])*100000)/100000:.5f}
                text_print = f'[{fileno:3}][{count[fileno]:5}] recv {len(recv_message_bytes):10}/{recvlen[fileno]:10} bytes. over:{len(recv_data[fileno]):10}  replaced:{recv_bytes_replaced}'
                print(text_print)
            
            recv_data[fileno] = recv_data[fileno][end_index+len(closer):]
    
    return send_bytes

is_running = multiprocessing.Value(ctypes.c_bool, True)
threads = []
lock = threading.Lock()

def recv_threading():
    while is_running.value:
        fileno, recv_bytes = server.recv()
        if fileno and recv_bytes:
            if not fileno in locks:
                locks[fileno] = threading.Lock()
                
            if not fileno in recv_data:
                recv_data[fileno] = b''
                recvlen[fileno] = 0
        
            # print(f"[{threading.get_ident()}] [{fileno}] lock. datalen:{len(data)}")
            send_bytes = recv_callback(fileno, recv_bytes)
            for send_byte in send_bytes:
                server.send(fileno, send_byte)
        elif fileno:
            server.shutdown_client(fileno)
        else:
            is_running.value = False
            
    print('Finish recv_threading')
    
if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGABRT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    server.listen('218.55.118.203', 59012)
    server.start(count_threads=2)
    for _ in range(2):
        rt = threading.Thread(target=recv_threading)
        rt.start()
        threads.append(rt)
    print("Server Start.")
    
    server.join()
    
    for rt in threads:
        rt.join()
    print("Join Receive Thread.")