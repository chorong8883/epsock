import signal
import time
import iosock
import threading
import socket
import queue
import traceback
import collections
import select

client_num = 1
send_count = 1

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

send_bytes = b'abcdefghijklamopqrst'
for _ in range(15):
    send_bytes += send_bytes
    
starter = b'%w$d#'
closer = b'&sa@f#d$'
packed_send_bytes = packing(send_bytes, starter, closer)
packed_send_bytes_length = len(packed_send_bytes)

send_queue = queue.Queue()
kevents = collections.defaultdict(select.kevent)
clients = collections.defaultdict(iosock.Client)

import multiprocessing
import ctypes
is_running = multiprocessing.Value(ctypes.c_bool, True)

closer_fd, detect_close_fd = socket.socketpair()
        
def sending():
    kq = select.kqueue()
    try:
        sum_length = 0
        while is_running.value:
            revents = kq.control(list(kevents.values()), 1000)
            for event in revents:
                print(f"{event.ident} filter:{event.filter:#06x} flags:{event.flags:#06x} KQ_FILTER_READ:{select.KQ_FILTER_READ:#06x} KQ_EV_EOF:{select.KQ_EV_EOF:#06x}[{event.flags & select.KQ_EV_EOF:#06x}] KQ_EV_ENABLE:{select.KQ_EV_ENABLE:#06x} KQ_EV_ERROR:{select.KQ_EV_ERROR:#06x}[{event.flags & select.KQ_EV_ERROR:#06x}] KQ_EV_ADD:{select.KQ_EV_ADD:#06x}, KQ_EV_DELETE:{select.KQ_EV_DELETE:#06x}, KQ_EV_ONESHOT:{select.KQ_EV_ONESHOT:#06x}")
                if event.flags & select.KQ_EV_ERROR:
                    print("event.flags & select.KQ_EV_ERROR")
                    return
                    
                if event.ident == detect_close_fd.fileno():
                    print(f"event.ident == detect_close_fd.fileno() {event}")
                    is_running.value = False
                    continue
                    
                elif event.filter == select.KQ_FILTER_READ:
                    if event.flags & select.KQ_EV_EOF:
                        print("event.flags & select.KQ_EV_EOF")
                        kevents.pop(event.ident)
                        client : iosock.Client = clients.pop(event.ident)
                        client.close()
                
                    else:
                        client : iosock.Client = clients[event.ident]
                        data = client.recv()
                        sum_length += len(data)
                        print(f"{len(data)}/{sum_length} {data[:10]}...{data[-10:]}")
                
                else:
                    print('else', event)
        
                
            # client_fileno = send_queue.get()
            # if not client_fileno:
            #     break
            # client:iosock.Client = clients[client_fileno]
            
            # start = time.time()
            # for i in range(send_count):
            #     send_bytes_index = 0
            #     try:
            #         while send_bytes_index < packed_send_bytes_length:
            #             try:
            #                 sended_length = client.send(packed_send_bytes[send_bytes_index:])
            #                 send_bytes_index += sended_length
            #             except BlockingIOError as e:
            #                 if e.errno == socket.EAGAIN:
            #                     pass
            #                 else:
            #                     raise e
            #     except Exception as e:
            #         print(e)
            # end = time.time()
            # print('sending result time elapsed:', end - start)
        
            # client.setblocking(True)
        
            # recv_bytes = b''
            # try:
            #     while not -1<recv_bytes.find(closer):
            #         temp_data = client.recv()
            #         recv_bytes += temp_data
                    
            # except Exception as e:
            #     print('recv', e)
            # # print(recv_bytes)
            # packed_recv_bytes_removed = recv_bytes.removeprefix(starter)
            # packed_recv_bytes_removed = packed_recv_bytes_removed.removesuffix(closer)
            # unpacked_recv_bytes = unpacking(packed_recv_bytes_removed)
            # print(len(unpacked_recv_bytes))
            
    except Exception as e:
        print(f"worker exception: {e}\n{traceback.format_exc()}")
    kq.close()

def signal_handler(num_recv_signal, frame):
    print(f"Get Signal: {signal.Signals(num_recv_signal).name}")
    # is_running.value = False
    try:
        closer_fd.shutdown(socket.SHUT_RDWR)
    except Exception as e:
        print(e)
    send_queue.put_nowait(None)
    # for fd in clients.keys():
    #     try:
    #         client:iosock.Client = clients[fd]
    #         client.close()
    #     except Exception as e:
    #         print(e)

if __name__ == '__main__':
    try:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGABRT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        kevent = select.kevent(detect_close_fd)
        kevents[detect_close_fd] = kevent
        
        for _ in range(client_num):
            client = iosock.Client()
            client.connect('218.55.118.203', 59012)
            client_fileno = client.get_fileno()
            print(f"connect [{client_fileno}]")
            client.setblocking(True)
            client.send(packed_send_bytes)
            # client.setblocking(False)
            clients[client_fileno] = client
            kevent = select.kevent(client_fileno)
            kevents[client_fileno] = kevent
            
        sender_thread = threading.Thread(target=sending)
        sender_thread.start()
        sender_thread.join()
        
        for fd in clients.keys():
            try:
                client:iosock.Client = clients[fd]
                client.close()
            except Exception as e:
                print(e)
    except Exception as e:
        print(f"main exception: {e}\n{traceback.format_exc()}")
        send_queue.put_nowait(None)