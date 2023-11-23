import signal
import time
import iosock
import threading
import socket
import queue
import traceback
import collections
import select

client_num = 10
send_count = 10
data_sum_count = 14

def packing(source_bytes: bytes, starter: bytes = b'', closer: bytes = b'', byteorder:str = 'little') -> bytes:
    bit8_length = 1
    bit16_length = 2
    bit32_length = 4
    bit64_length = 8
    bit8_limit = 256
    bit16_limit = 65536
    bit32_limit = 4294967296
    bit64_limit = 18446744073709551616
    
    limit_bit = -1
    source_length = len(source_bytes)
    if source_length < bit8_limit:
        limit_bit = bit8_length
    elif source_length < bit16_limit:
        limit_bit = bit16_length
    elif source_length < bit32_limit:
        limit_bit = bit32_length
    elif source_length < bit64_limit:
        limit_bit = bit64_length
    
    packet = b''
    if 0<limit_bit:
        source_length_bytes = source_length.to_bytes(limit_bit, byteorder=byteorder)
        length_of_length = len(source_length_bytes)
        length_of_length_bytes = length_of_length.to_bytes(bit8_length, byteorder=byteorder)        
        packet = starter + length_of_length_bytes + source_length_bytes + source_bytes + closer
    return packet

def unpacking(source_bytes: bytes, byteorder: str = 'little') -> bytes:
    bit8_length = 1
    length_of_length = int.from_bytes(source_bytes[:bit8_length], byteorder=byteorder)
    source_length = int.from_bytes(source_bytes[bit8_length:(bit8_length+length_of_length)], byteorder=byteorder)
    start_index = bit8_length+length_of_length
    end_index = bit8_length+length_of_length+source_length
    if len(source_bytes) == end_index:
        return source_bytes[start_index:end_index]
    else:
        return None

send_bytes = b'abcdefghijklmnop qrstuvwxyz'
for _ in range(data_sum_count):
    send_bytes += send_bytes
send_bytes2 = b'abcdefghijklmnop qrstuvwxyz'
for _ in range(int(data_sum_count/2)):
    send_bytes2 += send_bytes2
    
print(f"data length:{len(send_bytes)} send_bytes2:{len(send_bytes2)}")

starter = b'%w$d#'
closer = b'&sa@f#d$'
packed_send_bytes = packing(send_bytes, starter, closer)
packed_send_bytes_length = len(packed_send_bytes)

packed_send_bytes2 = packing(send_bytes2, starter, closer)
packed_send_bytes_length2 = len(packed_send_bytes2)


kevents = collections.defaultdict(select.kevent)
clients = collections.defaultdict(iosock.Client)
recv_data = collections.defaultdict(iosock.Client)
locks = collections.defaultdict(threading.Lock)
import multiprocessing
import ctypes
is_running = multiprocessing.Value(ctypes.c_bool, True)

update_fd, detect_update_fd = socket.socketpair()
closer_fd, detect_close_fd = socket.socketpair()

def sending():
    for _ in range(client_num):
        if not is_running.value:
            break
        client = iosock.Client()
        client.connect('218.55.118.203', 59012)
        client_fileno = client.get_fileno()
        print(f"connect [{client_fileno}]")
        locks[client_fileno] = threading.Lock()
        clients[client_fileno] = client
        kevent = select.kevent(client_fileno)
        kevents[client_fileno] = kevent
        update_fd.send(b'update')
        
        for send_count_index in range(send_count):
            if not is_running.value:
                break
            with locks[client_fileno]:
                client.sendall(packed_send_bytes)
                client.sendall(packed_send_bytes2)
            print(f"[{send_count_index}] send [{client_fileno}] {len(packed_send_bytes):,} bytes")
            print(f"[{send_count_index}] send [{client_fileno}] {len(packed_send_bytes2):,} bytes")
        
    print("finish send")

def recving():
    kq = select.kqueue()
    try:
        while is_running.value:
            revents = kq.control(list(kevents.values()), 1000)
            for event in revents:
                if event.flags & select.KQ_EV_ERROR:
                    print("event.flags & select.KQ_EV_ERROR")
                    kevents.pop(event.ident)
                    continue
                    
                if event.ident == detect_close_fd.fileno():
                    print(f"event.ident == detect_close_fd.fileno() {event}")
                    is_running.value = False
                    continue
                
                elif event.ident == detect_update_fd.fileno():
                    continue
                    
                elif event.filter == select.KQ_FILTER_READ:
                    if event.flags & select.KQ_EV_EOF:
                        print(f"[{event.ident}]event.flags & select.KQ_EV_EOF")
                        kevents.pop(event.ident)
                        client : iosock.Client = clients.pop(event.ident)
                        client.close()
                
                    else:
                        client : iosock.Client = clients[event.ident]
                        data = b''
                        with locks[event.ident]:
                            data = client.recv()
                        if event.ident in recv_data:
                            recv_data[event.ident] += data
                        else:
                            recv_data[event.ident] = data
                        
                        fileno = event.ident
                        
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
                                recv_bytes:bytes = recv_data[fileno][start_index:end_index]
                                recv_data[fileno] = recv_data[fileno][end_index+len(closer):]
                                print(f"[{fileno}] {len(recv_bytes)} {recv_bytes[:10]}...{recv_bytes[-10:]}")
                                client : iosock.Client = clients.pop(fileno)
                                client.close()
                else:
                    print('else', event)
        
    except Exception as e:
        print(f"recver exception: {e}\n{traceback.format_exc()}")
    kq.close()
    print("finish recv")

def signal_handler(num_recv_signal, frame):
    print(f"Get Signal: {signal.Signals(num_recv_signal).name}")
    is_running.value = False
    try:
        closer_fd.shutdown(socket.SHUT_RDWR)
    except Exception as e:
        print(e)
    
if __name__ == '__main__':
    try:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGABRT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        kevent_close = select.kevent(detect_close_fd)
        kevents[detect_close_fd] = kevent_close
        kevent_update = select.kevent(detect_update_fd)
        kevents[detect_update_fd] = kevent_update
        
        sender_thread = threading.Thread(target=sending)
        recver_thread = threading.Thread(target=recving)
        
        sender_thread.start()
        recver_thread.start()
        
        sender_thread.join()
        recver_thread.join()
        
        for fd in clients.keys():
            try:
                client:iosock.Client = clients[fd]
                client.close()
            except Exception as e:
                print(e)
    except Exception as e:
        print(f"main exception: {e}\n{traceback.format_exc()}")
        