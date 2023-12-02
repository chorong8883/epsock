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
recv_bytes_by_fileno = collections.defaultdict(bytes)
recvlen = collections.defaultdict(int)

def recv_callback(fileno, recv_bytes) -> list[bytes]:
    send_bytes = []
    if not fileno in count:
        count[fileno] = 0
    count[fileno] += 1

    if fileno in recv_bytes_by_fileno:
        recv_bytes_by_fileno[fileno] += recv_bytes
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
            is_start = end_index <= len(recv_bytes_by_fileno[fileno]) and recv_bytes_by_fileno[fileno][:len(starter)] == starter
            length_of_length_bytes = recv_bytes_by_fileno[fileno][start_index:end_index]
            length_of_length = int.from_bytes(length_of_length_bytes, byteorder='little')
            
            start_index = end_index
            end_index = end_index + length_of_length
            is_len = end_index <= len(recv_bytes_by_fileno[fileno])
            
            length_bytes = recv_bytes_by_fileno[fileno][start_index:end_index]
            source_length = int.from_bytes(length_bytes, byteorder='little')
            
            start_index = end_index
            end_index = end_index+source_length
            is_closer = end_index+len(closer) <= len(recv_bytes_by_fileno[fileno]) and recv_bytes_by_fileno[fileno][end_index:end_index+len(closer)] == closer
        except IndexError:
            break
        
        if is_start and is_len and is_closer:
            send_bytes.append(recv_bytes_by_fileno[fileno][:end_index+len(closer)])
            
            recv_message_bytes:bytes = recv_bytes_by_fileno[fileno][start_index:end_index]
            # end = time.time()
            
            recv_bytes_replaced = recv_message_bytes.replace(b'abcdefghijklmnop qrstuvwxyz', b'')
            if recv_bytes_replaced != b'':
                recv_bytes_replaced = recv_message_bytes.replace(b'abcdefghijklmnop qrstuvwxyz', b'.')
            
                # time elapsed: {math.floor((end - time_recv_data[fileno])*100000)/100000:.5f}
                text_print = f'[{fileno:3}][{count[fileno]:5}] recv {len(recv_message_bytes):10}/{recvlen[fileno]:10} bytes. over:{len(recv_bytes_by_fileno[fileno]):10}  replaced:{recv_bytes_replaced}'
                print(text_print)
            
            recv_bytes_by_fileno[fileno] = recv_bytes_by_fileno[fileno][end_index+len(closer):]
    
    return send_bytes

is_running = multiprocessing.Value(ctypes.c_bool, True)
threads = []

def recv_threading():
    while is_running.value:
        recv_data = server.recv()
        if recv_data:
            if not recv_data[0] in recv_bytes_by_fileno:
                recv_bytes_by_fileno[recv_data[0]] = b''
                recvlen[recv_data[0]] = 0
        
            send_bytes = recv_callback(recv_data[0], recv_data[1])
            for send_byte in send_bytes:
                server.send(recv_data[0], send_byte)
        else:
            is_running.value = False
            
    print('Finish recv_threading')
    
if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGABRT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # ip = '218.55.118.203'
    # port = 59012
    ip = 'localhost'
    port = 60809

    server.listen(ip, port)
    server.start(count_threads=1)
    for _ in range(5):
        rt = threading.Thread(target=recv_threading)
        rt.start()
        threads.append(rt)
    print("Server Start.")
    
    server.join()
    
    for rt in threads:
        rt.join()
    print("Join Receive Thread.")