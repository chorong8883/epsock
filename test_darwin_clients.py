import signal
import time
import iosock
import threading
import socket
import queue
import traceback
import collections
import select
import errno

client_num = 1
send_count = 5
data_sum_count1 = 12
data_sum_count2 = 11

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

send_bytes1 = b'abcdefghijklmnop qrstuvwxyz'
for _ in range(data_sum_count1):
    send_bytes1 += send_bytes1
send_bytes2 = b'abcdefghijklmnop qrstuvwxyz'
for _ in range(int(data_sum_count2)):
    send_bytes2 += send_bytes2
    
print(f"data count:{send_count}+{send_count} send_bytes:{(len(send_bytes1)+len(send_bytes2))*send_count*client_num} send_bytes1:{len(send_bytes1)} send_bytes2:{len(send_bytes2)}")

starter = b'%w$d#'
closer = b'&sa@f#d$'
packed_send_bytes = packing(send_bytes1, starter, closer)
packed_send_bytes_length = len(packed_send_bytes)

packed_send_bytes2 = packing(send_bytes2, starter, closer)
packed_send_bytes_length2 = len(packed_send_bytes2)


kevents = collections.defaultdict(select.kevent)
clients = collections.defaultdict(iosock.DarwinClient)
recv_data = collections.defaultdict(bytes)
recv_data_len = collections.defaultdict(int)
locks = collections.defaultdict(threading.Lock)
import multiprocessing
import ctypes
is_running = multiprocessing.Value(ctypes.c_bool, True)

update_event, update_listener = socket.socketpair()
close_event, close_listener = socket.socketpair()

def kqueueing():
    kq = select.kqueue()
    try:
        # print(type(kevents))
        count = collections.defaultdict(int)
        while is_running.value:
            revents = kq.control(list(kevents.values()), 1000)
            for event in revents:
                if event.flags & select.KQ_EV_ERROR:
                    print(f"[{event.ident}] [{'Bad FD' if event.data == 9 else event.data}] event.flags & select.KQ_EV_ERROR")
                    kevents.pop(event.ident)
                    continue
                    
                if event.ident == close_listener.fileno():
                    print(f"event.ident == detect_close_fd.fileno() {event}")
                    is_running.value = False
                    continue
                
                elif event.ident == update_listener.fileno():
                    print(f"event.ident == update_listener.fileno() in")
                    update_data = update_listener.recv(1024)
                    print(f"event.ident == update_listener.fileno() {update_data}")
                    continue
                    
                elif event.filter == select.KQ_FILTER_READ:
                    if event.flags & select.KQ_EV_EOF:
                        eof_message = f"[{event.ident:3}] event.flags & select.KQ_EV_EOF"
                        if event.ident in kevents:
                            eof_message += f" Pop Event({len(kevents):3}->"
                            e = kevents.pop(event.ident)
                            eof_message += f"{len(kevents):3})"
                        
                        if event.ident in clients:
                            client : iosock.DarwinClient = clients.pop(event.ident)
                            client.close()
                        
                        if event.ident in recv_data:
                            eof_message += f" remain:{len(recv_data[event.ident])}"
                            # if 0<len(recv_data[event.ident]):
                        else:
                            eof_message += f" remain false"

                        # print(eof_message)
                        
                    else:
                        client : iosock.DarwinClient = clients.get(event.ident)
                        if client:
                            data = b''
                            data = client.recv()
                            print(data)
                            
                            if event.ident in recv_data:
                                recv_data[event.ident] += data
                            else:
                                recv_data[event.ident] = data
                                
                            if not event.ident in recv_data_len:
                                recv_data_len[event.ident] = 0
                            
                            recv_data_len[event.ident] += len(data)
                            
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
                                    if event.ident in count:
                                        count[event.ident] += 1
                                    else:
                                        count[event.ident] = 1
                                        
                                    recv_bytes:bytes = recv_data[fileno][:end_index+len(closer)]
                                    recv_data[fileno] = recv_data[fileno][end_index+len(closer):]
                                    # print(f"[{fileno}] {len(recv_bytes)} {recv_bytes[:10]}...{recv_bytes[-10:]}")
                                    try:
                                        if send_count*2 == count[event.ident]:
                                            # print(f"[{fileno:2}] [{count[event.ident]:2}] recv {len(recv_bytes):,} bytes all:{recv_data_len[event.ident]} remain:{len(recv_data[fileno])} End. Try Close")
                                            client : iosock.DarwinClient = clients.get(fileno)
                                            client.shutdown()
                                        # elif send_count*2 < count[event.ident]:
                                            # print(f"[{fileno:2}] [{count[event.ident]:2}] Try Close Warning Count")
                                            # client : iosock.DarwinClient = clients.get(fileno)
                                            # client.shutdown()
                                        # else:
                                        #     print(f"[{fileno:2}] [{count[event.ident]:2}] recv {len(recv_bytes):,} bytes all:{recv_data_len[event.ident]} remain:{len(recv_data[fileno])}")
                                            
                                    except KeyError:
                                        pass
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
        close_event.shutdown(socket.SHUT_RDWR)
    except Exception as e:
        print(e)

from multiprocessing.pool import ThreadPool    

if __name__ == '__main__':
    try:
        pool = ThreadPool(16)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGABRT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        kevent_close = select.kevent(close_listener.fileno())
        kevents.update({close_listener.fileno() : kevent_close})
        kevent_update = select.kevent(update_listener.fileno())
        kevents.update({update_listener.fileno() : kevent_update})
        
        kqueueing_thread = threading.Thread(target=kqueueing)
        kqueueing_thread.start()
        
        for _ in range(client_num):
            if not is_running.value:
                break
            client = iosock.DarwinClient()
            client.connect('218.55.118.203', 59012)
            client_fileno = client.fileno()
            # print(f"connect [{client_fileno}]")
            locks[client_fileno] = threading.Lock()
            clients[client_fileno] = client
            kevent = select.kevent(client_fileno)
            kevents.update({client_fileno:kevent})
        
        update_event.send(b'update')
        
        client_filenos = list(clients.keys())
        
        res_pools = []
        for cf in client_filenos:
            def send_data(client_fileno):
                # send_count_index = 0
                send_bytes_len = 0
                for _ in range(send_count):
                    if not is_running.value:
                        break
                    try:
                        try:
                            client = clients.get(client_fileno)
                            if client:
                                client.sendall(packed_send_bytes)
                                send_bytes_len += len(packed_send_bytes)
                                client.sendall(packed_send_bytes2)
                                send_bytes_len += len(packed_send_bytes2)
                            # print(f"[{client_fileno:2}] [{send_count_index:2}] send {len(packed_send_bytes):,} bytes")
                            # send_count_index += 1
                            # print(f"[{client_fileno:2}] [{send_count_index:2}] send {len(packed_send_bytes2):,} bytes")
                            # send_count_index += 1
                        except BrokenPipeError:
                            pass
                        except OSError as e:
                            if e.errno == errno.EBADF:
                                pass
                            else:
                                raise e
                    except Exception as e:
                        print('send',  e, traceback.format_exc())
                    
                return send_bytes_len
            
            res_pool = pool.apply_async(send_data, args=(cf,))
            res_pools.append(res_pool)
            
        for res_pool in res_pools:
            _ = res_pool.get()
            # send_bytes_len = res_pool.get()
            # if send_bytes_len != (send_count * len(packed_send_bytes)) + (send_count * len(packed_send_bytes2)):
            #     print(send_bytes_len)
            
        print("finish send")

        kqueueing_thread.join()
        
        for fd in clients:
            try:
                client:iosock.DarwinClient = clients.get(fd)
                if client:
                    client.close()
            except Exception as e:
                print('main', e)
    except Exception as e:
        print(f"main exception: {e}\n{traceback.format_exc()}")
        