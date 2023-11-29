import socket
import select
import platform
import multiprocessing
import ctypes
import threading
import collections
import queue
import errno
import json
import traceback
from datetime import datetime

from contextlib import contextmanager

# import exception

class ClientBase():
    def __init__(self) -> None:
        self.__buffer_size = 8196
        self.__client_socket : socket.socket = None
        
    def connect(self, ip:str, port:int):
        self.__client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__client_socket.connect((ip, port))
        # self.__client_socket.setblocking(False)
    
    def shutdown(self):
        if self.__client_socket:
            try:
                self.__client_socket.shutdown(socket.SHUT_RDWR)
            except OSError as e:
                if e.errno == errno.ENOTCONN:
                    pass
                else:
                    raise e
        
    def close(self):
        self.__client_socket.close()
            
    def sendall(self, data:bytes):
        self.__client_socket.sendall(data)
    
    def send(self, data:bytes) -> int:
        return self.__client_socket.send(data)
        # non blocking
        # start_index = 0
        # end_index = len(data)
        # try:
        #     while start_index < end_index:
        #         if start_index != 0:
        #             print(f"{start_index}:{end_index}")
        #         len_send = self.__client_socket.send(data[start_index:end_index])
        #         if len_send <= 0:
        #             break
        #         start_index += len_send
        # except BlockingIOError as e:
        #     if e.errno == socket.EAGAIN:
        #         pass
        #     else:
        #         raise e
        # return start_index
        
    def recv(self):
        return self.__client_socket.recv(self.__buffer_size)
        # non blocking
        # recv_data = b''
        # try:
        #     while True:
        #         temp_recv_data = self.__client_socket.recv(self.__buffer_size)
        #         if temp_recv_data:
        #             recv_data += temp_recv_data
        #         else:
        #             break
        # except BlockingIOError as e:
        #     if e.errno == socket.EAGAIN:
        #         pass
        #     else:
        #         raise e
        # return recv_data
    
    def get_fileno(self) -> int:
        return self.__client_socket.fileno()
    
    def setblocking(self, is_block:bool):
        self.__client_socket.setblocking(is_block)
        
class KqueueServer:
    def __init__(self) -> None:
        self.__buffer_size = 10240
        self.__is_running = multiprocessing.Value(ctypes.c_bool, False)
        self.client_by_fileno = collections.defaultdict(dict)
        self.__kevent_by_fileno = collections.defaultdict(select.kevent)
        self.__recv_queue = queue.Queue()
        
        self.__detect_epollin_fileno_queue = queue.Queue()
        self.__send_fileno_queue = queue.Queue()
    
    def start(self, listen_ip:str, listen_port:int, is_blocking:bool = False, backlog:int = 5):
        self.__listen_socket = self.get_listener(listen_ip, listen_port, is_blocking, backlog)

        self.__is_running.value = True
        
        self.__kq_thread = threading.Thread(target=self.__kq_thread_function)
        self.__recv_work_thread = threading.Thread(target=self.__recv_work)
        self.__send_work_thread = threading.Thread(target=self.__send_work)
        
        self.__kq_thread.start()
        self.__recv_work_thread.start()
        self.__send_work_thread.start()
        
        listen_socket_fileno = self.__listen_socket.fileno()
        kevent = select.kevent(listen_socket_fileno)
        self.__kevent_by_fileno[listen_socket_fileno] = kevent
        
    def stop(self):
        self.__is_running.value = False
        self.__listen_socket.shutdown(socket.SHUT_RDWR)
        
    def join(self):
        self.__kq_thread.join()
        self.__recv_work_thread.join()
        self.__send_work_thread.join()
    
    def create_client(self, client_socket):
        return {
            "socket" : client_socket,
            "lock" : threading.Lock(),
            "send_buffer_queue" : queue.Queue(),
            "sending_buffer" : b''
        }
    
    def recv(self):
        return self.__recv_queue.get()
    
    def __recv_work(self):
        while self.__is_running.value:
            detect_fileno = self.__detect_epollin_fileno_queue.get()
            if not detect_fileno:
                self.__recv_queue.put_nowait(None)
                break
            
            result = b''
            with acquire_timeout(self.client_by_fileno[detect_fileno]['lock'], 1) as acqiured:
                try:
                    while True:
                        recv_bytes = self.client_by_fileno[detect_fileno]['socket'].recv(self.__buffer_size)
                        if recv_bytes == None or recv_bytes == -1 or recv_bytes == b'':
                            break
                        elif recv_bytes:
                            result += recv_bytes
                        else:
                            break
                except BlockingIOError as e:
                    if e.errno == socket.EAGAIN:
                        pass
                    else:
                        raise e
                if result is not None and result != b'':
                    self.__recv_queue.put_nowait({
                        "fileno": detect_fileno,
                        "data": result
                    })

#####################################################################################################################
#####################################################################################################################
#####################################################################################################################
    def send(self, fileno:int, data:bytes):
        self.client_by_fileno[fileno]['send_buffer_queue'].put_nowait(data)
        self.__send_fileno_queue.put_nowait(fileno)
    
    def __send_work(self):
        while self.__is_running.value:
            send_fileno = self.__send_fileno_queue.get()
            if not send_fileno:
                break
        
            client_data = self.client_by_fileno.get(send_fileno)
            if client_data:
                with acquire_timeout(self.client_by_fileno[send_fileno]['lock'], 1) as acqiured:
                    if acqiured:
                        if self.client_by_fileno[send_fileno]['sending_buffer'] == b'':
                            try:
                                self.client_by_fileno[send_fileno]['sending_buffer'] = self.client_by_fileno[send_fileno]['send_buffer_queue'].get_nowait()
                                self.client_by_fileno[send_fileno]['socket'].send(b'')
                            except queue.Empty:
                                return
                        
                        start_index = 0
                        end_index = len(self.client_by_fileno[send_fileno]['sending_buffer'])
                        try:
                            while start_index < end_index:
                                send_length = self.client_by_fileno[send_fileno]['socket'].send(self.client_by_fileno[send_fileno]['sending_buffer'][start_index:end_index])
                                if send_length <= 0:
                                    break
                                start_index += send_length
                        except BlockingIOError as e:
                            if e.errno == socket.EAGAIN:
                                pass
                            else:
                                raise e
                            
                        if 0 <= start_index < end_index:
                            self.client_by_fileno[send_fileno]['sending_buffer'] = self.client_by_fileno[send_fileno]['sending_buffer'][start_index:end_index]
                        else:
                            self.client_by_fileno[send_fileno]['sending_buffer'] = b''
                            self.client_by_fileno[send_fileno]['socket'].send(b'')
                        
                        if self.client_by_fileno[send_fileno]['sending_buffer'] != b'':
                            self.__send_fileno_queue.put_nowait(send_fileno)
                        elif not self.client_by_fileno[send_fileno]['send_buffer_queue'].empty():
                            self.__send_fileno_queue.put_nowait(send_fileno)
                        
                    else:
                        print('send lock timeout')
                            
#####################################################################################################################
#####################################################################################################################
#####################################################################################################################            
            
    
    def get_listener(self, listen_ip:str, listen_port:int, is_blocking:bool = False, backlog:int = 5) -> socket.socket:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setblocking(is_blocking)
        s.bind((listen_ip, listen_port))
        s.listen(backlog)
        return s 

    def __kq_thread_function(self):
        kq = select.kqueue()
        while self.__is_running.value:
            kevents = self.__kevent_by_fileno.values()
            events = kq.control(list(kevents), 1000)
            for event in events:
                if event.flags & select.KQ_EV_ERROR:
                    print("event.flags & select.KQ_EV_ERROR")
                    return
                    
                if event.ident == self.__listen_socket.fileno():
                    if event.flags & select.KQ_EV_EOF:
                        print(f"event.ident == detect_close_fd.fileno() {event}")
                        self.__is_running.value = False
                        continue
                    else:
                        client_socket, address = self.__listen_socket.accept()
                        client_socket_fileno = client_socket.fileno()
                        print(f"accept {client_socket_fileno} {address}")
                        client_socket.setblocking(False)
                        client = self.client_by_fileno.get(client_socket_fileno)
                        if client:
                            s:socket.socket = client["socket"]
                            try:
                                s.shutdown(socket.SHUT_RDWR)
                            except Exception as e:
                                pass
                            s.close()
                        client_data = self.create_client(client_socket)
                        self.client_by_fileno.update({client_socket_fileno : client_data})
                        
                        kevent = select.kevent(client_socket_fileno)
                        self.__kevent_by_fileno.update({client_socket_fileno:kevent})
                        
                elif event.filter == select.KQ_FILTER_READ:
                    if event.flags & select.KQ_EV_EOF:
                        print("event.flags & select.KQ_EV_EOF")
                        self.__kevent_by_fileno.pop(event.ident)
                        client_data = self.client_by_fileno.pop(event.ident)
                        client_data['socket'].shutdown(socket.SHUT_RDWR)
                        client_data['socket'].close()
                
                    else:
                        client_data = self.client_by_fileno.pop(event.ident)
                        data = client_data['socket'].recv()
                        print(data)
                
        kq.close()
        
        
        
class EpollServer():
    def __init__(self) -> None:
        self.__buffer_size = 8196
        self.__is_running = multiprocessing.Value(ctypes.c_bool, False)
        
        self.__listener_by_ip_port = collections.defaultdict(socket.socket)
        self.__listener_by_fileno = collections.defaultdict(socket.socket)
        self.__registered_eventmask_by_fileno = collections.defaultdict(int)
        self.__client_by_fileno = collections.defaultdict(socket.socket)
        
        self.__client_fileno_dict_by_listener_fileno = collections.defaultdict(dict)
        
        self.__close_lock_by_fileno = collections.defaultdict(threading.Lock)
        self.__send_lock_by_fileno = collections.defaultdict(threading.Lock)
        self.__recv_lock_by_fileno = collections.defaultdict(threading.Lock)
        self.__send_buffer_queue_by_fileno = collections.defaultdict(queue.Queue)
        self.__send_buffer_queue_max_by_fileno = collections.defaultdict(int)
        self.__sending_buffer_by_fileno = collections.defaultdict(bytes)
        self.__running_threads = []
        self.__running_thread_by_tid = collections.defaultdict(threading.Thread)
        
        self.__recv_queue = queue.Queue()
        self.__recv_queue_threads = collections.defaultdict(bool)
        self.__epoll : select.epoll = None
        
        self.__listener_eventmask = select.EPOLLIN | select.EPOLLPRI | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
        self.__recv_eventmask = select.EPOLLIN  | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
        self.__send_recv_eventmask = select.EPOLLIN | select.EPOLLOUT | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
        self.__closer_eventmask = select.EPOLLIN | select.EPOLLPRI | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
        
    @contextmanager
    def __acquire_timeout(self, lock:threading.Lock, timeout:float):
        result = lock.acquire(timeout=timeout)
        try:
            yield result
        finally:
            if result:
                lock.release()
    @contextmanager
    def __acquire_blocking(self, lock:threading.Lock, blocking:bool):
        result = lock.acquire(blocking=blocking)
        try:
            yield result
        finally:
            if result:
                lock.release()
    
    def listen(self, ip:str, port:int, backlog:int = 5):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # listener.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1) # Nagle's
        
        recv_buf_size = listener.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        send_buf_size = listener.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, recv_buf_size*2)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, send_buf_size*2)
        listener.setblocking(False)
        listener.bind((ip, port))
        listener.listen(backlog)
        
        listen_socket_fileno = listener.fileno()
        
        self.__listener_by_ip_port.update({f"{ip}:{port}":listener})
        self.__listener_by_fileno.update({listen_socket_fileno : listener})
        self.__send_buffer_queue_by_fileno.update({listen_socket_fileno : queue.Queue()})
        self.__sending_buffer_by_fileno.update({listen_socket_fileno : b''})
        self.__client_fileno_dict_by_listener_fileno.update({listen_socket_fileno : {}})
        
        if self.__epoll and not self.__epoll.closed:
            self.__epoll.register(listen_socket_fileno, self.__listener_eventmask)
            self.__registered_eventmask_by_fileno.update({listen_socket_fileno : self.__listener_eventmask})

    def unlisten(self, ip:str, port:int):
        try:
            listener = self.__listener_by_ip_port.get(f"{ip}:{port}")
            if listener:
                listener.shutdown(socket.SHUT_RDWR)
        except Exception as e:
            print(e)

    def start(self, count_thread:int):
        self.__is_running.value = True
        
        self.__epoll = select.epoll()
        self.__close_event, self.__close_event_listener = socket.socketpair()
        self.__epoll.register(self.__close_event_listener, self.__closer_eventmask)
        
        for _ in range(count_thread):
            et = threading.Thread(target=self.__epoll_thread_function)
            et.start()
            self.__running_threads.append(et)
            self.__running_thread_by_tid[et.ident] = et
            
        for fileno in self.__listener_by_fileno:
            if fileno in self.__registered_eventmask_by_fileno:
                if self.__registered_eventmask_by_fileno[fileno] != self.__listener_eventmask:
                    self.__epoll.modify(fileno, self.__listener_eventmask)
            else:
                self.__epoll.register(fileno, self.__listener_eventmask)
                self.__registered_eventmask_by_fileno.update({fileno : self.__listener_eventmask})

    def recv(self):
        tid = threading.get_ident()
        if not tid in self.__recv_queue_threads:
            self.__recv_queue_threads[tid] = True
        if self.__is_running.value:
            return self.__recv_queue.get()
        else:
            return None
    
    def send(self, send_fileno:int, data:bytes = None):
        try:
            self.__send_buffer_queue_max_by_fileno[send_fileno] += 1
            self.__send_buffer_queue_by_fileno[send_fileno].put_nowait(data)
        except KeyError:
            # removed
            pass
        try:
            self.__registered_eventmask_by_fileno[send_fileno] = self.__send_recv_eventmask
            self.__epoll.modify(send_fileno, self.__send_recv_eventmask)
            
        except FileNotFoundError:
            print("FileNotFoundError self.__epoll.modify(send_fileno, self.__send_eventmask)")
            pass
        except OSError as e:
            if e.errno == errno.EBADF:
                print("e.errno == errno.EBADF self.__epoll.modify(send_fileno, self.__send_eventmask)")
                pass
            elif e.errno == errno.EBADFD:
                print("e.errno == errno.EBADFD self.__epoll.modify(send_fileno, self.__send_eventmask)")
                pass
            else:
                raise e
    
    def join(self):
        for t in self.__running_threads:
            t:threading.Thread = t
            t.join()
                
    def close(self):
        self.__is_running.value = False
        self.__shutdown_listeners()
        
        for _ in self.__running_threads:
            self.__close_event.send(b'close')
            # print("close before recv")
            tid_bytes = self.__close_event.recv(32)
            tid = int.from_bytes(tid_bytes, byteorder='big')
            # print(f"close recv tid:{tid}")
            self.__running_thread_by_tid[tid].join()
            
        for _ in range(len(self.__recv_queue_threads)):
            self.__recv_queue.put_nowait(None)
    
    def recv(self):
        tid = threading.get_ident()
        if not tid in self.__recv_queue_threads:
            self.__recv_queue_threads[tid] = True
        if self.__is_running.value:
            return self.__recv_queue.get()
        else:
            return None
    
    def send(self, send_fileno:int, data:bytes = None):
        try:
            self.__send_buffer_queue_max_by_fileno[send_fileno] += 1
            self.__send_buffer_queue_by_fileno[send_fileno].put_nowait(data)
        except KeyError:
            # removed
            pass
        try:
            self.__registered_eventmask_by_fileno[send_fileno] = self.__send_recv_eventmask
            self.__epoll.modify(send_fileno, self.__send_recv_eventmask)
            
        except FileNotFoundError:
            print("FileNotFoundError self.__epoll.modify(send_fileno, self.__send_eventmask)")
            pass
        except OSError as e:
            if e.errno == errno.EBADF:
                print("e.errno == errno.EBADF self.__epoll.modify(send_fileno, self.__send_eventmask)")
                pass
            elif e.errno == errno.EBADFD:
                print("e.errno == errno.EBADFD self.__epoll.modify(send_fileno, self.__send_eventmask)")
                pass
            else:
                raise e
    
    def __shutdown_listeners(self):
        for fileno in self.__listener_by_fileno:
            self.__shutdown_listener(fileno)
            
    def __shutdown_listener(self, detect_fileno):
        listener = self.__listener_by_fileno.get(detect_fileno)
        if listener:
            listener.shutdown(socket.SHUT_RDWR)
        
    def __close_listener(self, detect_fileno):
        try:
            self.__epoll.unregister(detect_fileno)
        except FileNotFoundError:
            pass
        except OSError as e:
            if e.errno == errno.EBADF:
                print(f"e.errno == errno.EBADF __close_listener {detect_fileno}")
                # print(f"e.errno == errno.EBADF {detect_fileno}\n{traceback.format_exc()}")
            else:
                raise e
        listener = self.__listener_by_fileno.get(detect_fileno)
        if listener:
            listener.close()
            print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] Listner Close()")
        
    def __remove_listener(self, detect_fileno):
        try:
            listener = self.__listener_by_fileno.pop(detect_fileno)
        except KeyError:
            pass
    
    def __unregister(self, detect_fileno:int) -> bool:
        result = False
        try:
            _ = self.__registered_eventmask_by_fileno.pop(detect_fileno)
            self.__epoll.unregister(detect_fileno)
            result = True
        except KeyError:
            print(f"__close_client Key {detect_fileno}")
            pass
        except FileNotFoundError:
            print(f"__close_client FileNotFoundError __epoll.unregister {detect_fileno}")
            pass
        except OSError as e:
            if e.errno == errno.EBADF:
                print(f"__close_client e.errno == errno.EBADF {detect_fileno}")
            else:
                raise e
        return result
        
    def __shutdown_clients_by_listener(self, listener_fileno):
        client_fileno_dict = self.__client_fileno_dict_by_listener_fileno[listener_fileno]
        for client_fileno in client_fileno_dict:
            self.__shutdown_client(client_fileno)
        
    def __shutdown_client(self, detect_fileno:int):
        client_socket = self.__client_by_fileno.get(detect_fileno)
        if client_socket:
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except ConnectionResetError:
                print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] ConnectionResetError")
                
            except BrokenPipeError:
                print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] BrokenPipeError")
                
            except OSError as e:
                if e.errno == errno.ENOTCONN: # errno 107
                    print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] ENOTCONN")
                else:
                    raise e
    
    def __close_client(self, detect_fileno:int):
        client_socket = self.__client_by_fileno.get(detect_fileno)
        if client_socket:
            client_socket.close()
            # print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] Client Close()")

    def __remove_client(self, detect_fileno):
        try:
            self.__epoll.unregister(detect_fileno)
        except FileNotFoundError:
            pass
        except OSError as e:
            if e.errno == errno.EBADF:
                pass
        try:
            _ = self.__send_lock_by_fileno.pop(detect_fileno)
        except KeyError:
            pass
        try:
            _ = self.__recv_lock_by_fileno.pop(detect_fileno)
        except KeyError:
            pass
        try:
            _ = self.__client_by_fileno.pop(detect_fileno)
        except KeyError:
            pass
        len_send_buffer_queue = -1
        send_buffer_queue:queue.Queue = None
        try:
            send_buffer_queue = self.__send_buffer_queue_by_fileno.pop(detect_fileno)
            len_send_buffer_queue = len(send_buffer_queue.queue)
            while not send_buffer_queue.empty():
                _ = send_buffer_queue.get_nowait()
        except KeyError:
            pass
        sending_buffer:bytes = b''
        try:
            sending_buffer = self.__sending_buffer_by_fileno.pop(detect_fileno)
        except KeyError:
            pass
        send_count = 0
        try:
            send_count = self.__send_buffer_queue_max_by_fileno.pop(detect_fileno)
        except KeyError:
            pass
        
        if 0 < len_send_buffer_queue:
            print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] Try Close. send buffer remain:{len(sending_buffer)} bytes. queue remain:{len_send_buffer_queue}/{send_count}")
    
    def __epoll_accept(self, detect_fileno:int):
        try:
            listener = self.__listener_by_fileno.get(detect_fileno)
            if listener:
                client_socket, address = listener.accept()
                # print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] accept {client_socket.fileno():2}:{address}")
                client_socket_fileno = client_socket.fileno()
                client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                
                client_socket.setblocking(False)
                
                exist_client_socket = self.__client_by_fileno.get(client_socket_fileno)
                if exist_client_socket:
                    try:
                        exist_client_socket.shutdown(socket.SHUT_RDWR)
                    except OSError as e:
                        if e.errno == errno.ENOTCONN: # errno 107
                            pass
                        else:
                            raise e
                    
                    exist_client_socket.close()
                    try:
                        self.__epoll.unregister(client_socket)
                    except Exception as e:
                        print(e)
            
                self.__client_by_fileno.update({client_socket_fileno : client_socket})
                self.__close_lock_by_fileno.update({client_socket_fileno : threading.Lock()})
                self.__send_lock_by_fileno.update({client_socket_fileno : threading.Lock()})
                self.__recv_lock_by_fileno.update({client_socket_fileno : threading.Lock()})
                self.__send_buffer_queue_by_fileno.update({client_socket_fileno : queue.Queue()})
                self.__send_buffer_queue_max_by_fileno.update({client_socket_fileno : 0})
                self.__sending_buffer_by_fileno.update({client_socket_fileno : b''})
                if not detect_fileno in self.__client_fileno_dict_by_listener_fileno:
                    self.__client_fileno_dict_by_listener_fileno.update({detect_fileno : {}})
                self.__client_fileno_dict_by_listener_fileno[detect_fileno][client_socket_fileno] = True
                
                self.__registered_eventmask_by_fileno[client_socket_fileno] = self.__recv_eventmask
                self.__epoll.register(client_socket, self.__recv_eventmask)
                
        except BlockingIOError as e:
            if e.errno == socket.EAGAIN:
                pass
            else:
                raise e
    
    def __epoll_close_client(self, detect_fileno:int):
        close_lock = self.__close_lock_by_fileno.get(detect_fileno)
        if close_lock:
            with close_lock:
                if self.__unregister(detect_fileno):
                    self.__close_client(detect_fileno)
                    remain_data_length = len(self.__sending_buffer_by_fileno[detect_fileno])
                    remain_q_size = len(self.__send_buffer_queue_by_fileno[detect_fileno].queue)
                    self.__remove_client(detect_fileno)
                    exception_msg = f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] Epoll Client Closed."
                    if 0<remain_data_length:
                        exception_msg += f" B:{remain_data_length}"
                    if 0<remain_q_size:
                        exception_msg += f" Q:{remain_q_size}"
                    print(exception_msg)
    
    def __epoll_recv(self, detect_fileno:int):
        recv_lock = self.__recv_lock_by_fileno.get(detect_fileno)
        if recv_lock:
            with recv_lock:
                recv_bytes = b''
                client_socket = self.__client_by_fileno.get(detect_fileno)
                if client_socket:
                    is_eagain = False
                    try:
                        temp_recv_bytes = client_socket.recv(self.__buffer_size)
                        if temp_recv_bytes == None or temp_recv_bytes == -1 or temp_recv_bytes == b'':
                            print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] recv break")
                        else:
                            recv_bytes += temp_recv_bytes
                    except ConnectionError as e:
                        print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] ConnectionError {e}")
                        
                    except OSError as e:
                        if e.errno == socket.EAGAIN:
                            is_eagain  = True
                        elif e.errno == errno.EBADF:
                            print(f"e.errno == errno.EBADF senf {detect_fileno}")
                        else:
                            raise e

                    
                    if not is_eagain:
                        try:
                            self.__epoll.modify(detect_fileno, self.__registered_eventmask_by_fileno[detect_fileno])
                        except FileNotFoundError:
                            pass
                        except OSError as e:
                            if e.errno == errno.EBADF:
                                print(f"e.errno == errno.EBADF recv {detect_fileno}")
                                pass

                    
                    if recv_bytes:
                        self.__recv_queue.put_nowait({
                            "fileno": detect_fileno,
                            "data": recv_bytes
                        })
                
    def __epoll_send(self, detect_fileno:int):
        send_lock = self.__send_lock_by_fileno.get(detect_fileno)
        if send_lock:
            with send_lock:
                try:
                    if self.__sending_buffer_by_fileno[detect_fileno] == b'':
                        self.__sending_buffer_by_fileno[detect_fileno] = self.__send_buffer_queue_by_fileno[detect_fileno].get_nowait()
                    send_length = self.__client_by_fileno[detect_fileno].send(self.__sending_buffer_by_fileno[detect_fileno])
                    if 0<send_length:
                        self.__sending_buffer_by_fileno[detect_fileno] = self.__sending_buffer_by_fileno[detect_fileno][send_length:]
                except BlockingIOError as e:
                    if e.errno == socket.EAGAIN:
                        # remain_data_length = len(self.__sending_buffer_by_fileno[detect_fileno])
                        # remain_q_size = len(self.__send_buffer_queue_by_fileno[detect_fileno].queue)
                        # exception_msg = f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] send e.errno == socket.EAGAIN"
                        # if 0<remain_data_length:
                        #      exception_msg += f"data:{remain_data_length}"
                        # if 0<remain_q_size:
                        #     exception_msg += f"q:{remain_q_size}"
                        # print(exception_msg)
                        pass
                    else:
                        raise e
                except queue.Empty:
                    pass
                
                try:
                    if self.__sending_buffer_by_fileno[detect_fileno] != b'' or not self.__send_buffer_queue_by_fileno[detect_fileno].empty():
                        self.__registered_eventmask_by_fileno[detect_fileno] = self.__send_recv_eventmask
                        self.__epoll.modify(detect_fileno, self.__send_recv_eventmask)
                    else:
                        self.__registered_eventmask_by_fileno[detect_fileno] = self.__recv_eventmask
                        self.__epoll.modify(detect_fileno, self.__recv_eventmask)
                except OSError as e:
                    if e.errno == errno.EBADF:
                        print(f"e.errno == errno.EBADF send {detect_fileno}")
                
    def __epoll_thread_function(self):
        __is_running = True
        tid = threading.get_ident()
        print(f"{datetime.now()} [{tid}:TID] Start Epoll Work")
        try:
            while __is_running:
                events = self.__epoll.poll()
                for detect_fileno, detect_event in events:
                    if detect_event & select.EPOLLPRI:
                        print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] EPOLLPRI [{detect_event:#06x} & select.EPOLLPRI]")
                    
                    if detect_fileno == self.__close_event_listener.fileno():
                        self.__close_event_listener.send(tid.to_bytes(32, 'big'))
                        __is_running = False
                        
                    elif detect_fileno in self.__listener_by_fileno:
                        if detect_event & (select.EPOLLHUP | select.EPOLLRDHUP):
                            print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] Listener HUP")
                            self.__shutdown_clients_by_listener(detect_fileno)
                            if self.__unregister(detect_fileno):
                                self.__close_listener(detect_fileno)
                                self.__remove_listener(detect_fileno)
                            
                        elif detect_event & select.EPOLLIN:
                            self.__epoll_accept(detect_fileno)
                        
                        else:
                            print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] listen event else [{detect_event:#06x}]..?")
                    
                    elif detect_fileno in self.__client_by_fileno:
                        if detect_event & (select.EPOLLHUP | select.EPOLLRDHUP):
                            self.__epoll_close_client(detect_fileno)
                        else:
                            if detect_event & select.EPOLLOUT:
                                self.__epoll_send(detect_fileno)
                                
                            if detect_event & select.EPOLLIN:
                                self.__epoll_recv(detect_fileno)
                    else:
                        print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] Unknown Fileno. {detect_event:#06x}, exist:{detect_fileno in self.__client_by_fileno}")
                        
        except Exception as e:
            print(e, traceback.format_exc())
        
        print(f"{datetime.now()} [{tid}:TID] Finish Epoll Work")


    
class RelayServer:
    def __init__(self) -> None:
        from . import linux
        self.server = linux.RelayServer()
        
    def listen(self, listen_ip:str, external_port:int, internal_port:int, backlog:int = 5):
        self.server.listen(listen_ip, external_port, internal_port, backlog)
    
    def append_listen(self, listen_ip:str, external_port:int, internal_port:int, backlog:int = 5):
        self.server.append_listen(listen_ip, external_port, internal_port, backlog)
        
    def start(self, count_thread:int = 1):
        self.server.start(count_thread)
    
    def close(self):
        self.server.close()
        
    def join(self):
        self.server.join()
        
        
        

# class RelayClient:
#     def __init__(self) -> None:
#         self.__buffer_size = 8196
#         self.__is_running = multiprocessing.Value(ctypes.c_bool, False)
        




# class RelayServer:
#     def __init__(self) -> None:
#         self.__buffer_size = 8196
#         self.__is_running = multiprocessing.Value(ctypes.c_bool, False)
        
#         self.__listen_data_list = []
        
#         self.__client_by_fileno = collections.defaultdict(socket.socket)
#         self.__lock_by_fileno = collections.defaultdict(threading.Lock)
#         self.__send_buffer_queue_by_fileno = collections.defaultdict(queue.Queue)
#         self.__sending_buffer_by_fileno = collections.defaultdict(bytes)
#         self.__running_thread_by_tid = collections.defaultdict(threading.Thread)
#         self.__finish_thread_by_tid = collections.defaultdict(threading.Thread)
        
#         self.__listener_by_fileno = collections.defaultdict(socket.socket)
#         self.__exlistener_by_fileno = collections.defaultdict(socket.socket)
#         self.__inlistener_by_fileno = collections.defaultdict(socket.socket)
        
#         self.__exsocket_by_fileno = collections.defaultdict(socket.socket)
#         self.__insocket_by_fileno = collections.defaultdict(socket.socket)
        
#         self.__exlistener_by_infileno = collections.defaultdict(socket.socket)
#         self.__inlistener_by_exfileno = collections.defaultdict(socket.socket)
        
#         self.__insockets_by_inlistener_fileno = collections.defaultdict(dict)
#         self.__exsockets_by_exlistener_fileno = collections.defaultdict(dict)
        
#         self.__exlistener_by_inlistener_fileno = collections.defaultdict(socket.socket)
#         self.__inlistener_by_exlistener_fileno = collections.defaultdict(socket.socket)
        
#         self.__listener_eventmask = select.EPOLLIN | select.EPOLLPRI | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
#         self.__hup_eventmask = select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
#         self.__recv_eventmask = select.EPOLLIN | select.EPOLLPRI | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
#         self.__send_eventmask = select.EPOLLIN | select.EPOLLPRI | select.EPOLLOUT | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
#         self.__closer_eventmask = select.EPOLLIN | select.EPOLLPRI | select.EPOLLHUP | select.EPOLLRDHUP
        
#     # @contextmanager
#     # def __acquire_timeout(self, lock:threading.Lock, timeout:float):
#     #     result = lock.acquire(timeout=timeout)
#     #     try:
#     #         yield result
#     #     finally:
#     #         if result:
#     #             lock.release()
#     @contextmanager
#     def __acquire_blocking(self, lock:threading.Lock, blocking:bool):
#         result = lock.acquire(blocking=blocking)
#         try:
#             yield result
#         finally:
#             if result:
#                 lock.release()

#     def listen(self, listen_ip:str, listen_port:int, backlog:int) -> socket.socket:
#         listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#         listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#         recv_buf_size = listen_socket.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
#         send_buf_size = listen_socket.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
#         listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, recv_buf_size*2)
#         listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, send_buf_size*2)
#         listen_socket.setblocking(False)
        
#         listen_socket.bind((listen_ip, listen_port))
#         listen_socket.listen(backlog)
#         return listen_socket
        
#     def append_listen(self, listen_ip:str, external_port:int, internal_port:int, backlog:int = 5):
#         self.__listen_data_list.append((listen_ip, external_port, internal_port, backlog))
        
#     def start(self, count_thread:int = 1):
#         self.__is_running.value = True
#         self.__epoll = select.epoll()
#         self.__close_event, self.__close_event_listener = socket.socketpair()
#         self.__epoll.register(self.__close_event_listener, self.__closer_eventmask)
    
#         for _ in range(count_thread):
#             et = threading.Thread(target=self.__epoll_thread_function)
#             et.start()
#             self.__running_thread_by_tid[et.ident] = et
        
#         for listen_data in self.__listen_data_list:
#             listen_ip = listen_data[0]
#             external_port = listen_data[1]
#             internal_port = listen_data[2]
#             backlog = listen_data[3]
            
#             inlistener = self.listen('localhost', internal_port, backlog)
#             inlistener_fileno = inlistener.fileno()
#             print(f"Listen 'localhost':{internal_port}")
            
#             exlistener = self.__listen(listen_ip, external_port, backlog)
#             exlistener_fileno = exlistener.fileno()
#             print(f"Listen {listen_ip}:{internal_port}")
            
#             self.__inlistener_by_fileno.update({inlistener_fileno: inlistener})
#             self.__exlistener_by_fileno.update({exlistener_fileno: exlistener})
            
#             self.__lock_by_fileno.update({inlistener_fileno : threading.Lock()})
#             self.__lock_by_fileno.update({exlistener_fileno : threading.Lock()})
            
#             self.__listener_by_fileno.update({inlistener_fileno: inlistener})
#             self.__listener_by_fileno.update({exlistener_fileno: exlistener})
            
#             self.__insockets_by_inlistener_fileno.update({inlistener_fileno:{}})
#             self.__exsockets_by_exlistener_fileno.update({exlistener_fileno:{}})
            
#             self.__inlistener_by_exlistener_fileno.update({exlistener_fileno: inlistener})
#             self.__exlistener_by_inlistener_fileno.update({inlistener_fileno: exlistener})
        
#             self.__epoll.register(inlistener_fileno, self.__listener_eventmask)
#             self.__epoll.register(exlistener_fileno, self.__listener_eventmask)
        
#     def close_client(self, fileno:int):
#         try:
#             _ = self.__lock_by_fileno.pop(fileno)
#         except KeyError:
#             pass
#         client_socket:socket.socket = None
#         try:
#             self.__client_by_fileno.pop(fileno)
#         except KeyError:
#             pass
#         len_send_buffer_queue = -1
#         send_buffer_queue:queue.Queue = None
#         try:
#             send_buffer_queue = self.__send_buffer_queue_by_fileno.pop(fileno)
#             len_send_buffer_queue = len(send_buffer_queue.queue)
#         except KeyError:
#             pass
#         sending_buffer:bytes = b''
#         try:
#             sending_buffer = self.__sending_buffer_by_fileno.pop(fileno)
#         except KeyError:
#             pass
#         print(f"{datetime.now()} [{threading.get_ident()}:TID] [{fileno:3}] Try Close. send buffer remain:{len(sending_buffer)} bytes. queue remain:{len_send_buffer_queue}")
        
#         if send_buffer_queue:
#             while not send_buffer_queue.empty():
#                 _ = send_buffer_queue.get_nowait()
            
#         if client_socket:
#             try:
#                 client_socket.shutdown(socket.SHUT_RDWR)
#             except ConnectionResetError:
#                 print(f"{datetime.now()} [{threading.get_ident()}:TID] [{fileno:3}] ConnectionResetError")
                
#             except BrokenPipeError:
#                 print(f"{datetime.now()} [{threading.get_ident()}:TID] [{fileno:3}] BrokenPipeError")
                
#             except OSError as e:
#                 if e.errno == errno.ENOTCONN: # errno 107
#                     print(f"{datetime.now()} [{threading.get_ident()}:TID] [{fileno:3}] ENOTCONN")
#                 else:
#                     raise e
            
#             client_socket.close()
#         print(f"{datetime.now()} [{threading.get_ident()}:TID] [{fileno:3}] Closed.")

#     def join(self):
#         tids = list(self.__running_thread_by_tid.keys())
#         for tid in tids:
#             if tid in self.__running_thread_by_tid:
#                 self.__running_thread_by_tid[tid].join()
        
#         for tid in self.__finish_thread_by_tid:
#             self.__finish_thread_by_tid[tid].join()
#             print(f"[{tid}:TID] joined")

#     def close(self):
#         self.__is_running.value = False
#         self.__close_event.send(b'close')
        
#     def __epoll_accepting(self, detect_fileno):
#         lock = self.__lock_by_fileno.get(detect_fileno)
#         if lock:
#             with self.__acquire_blocking(lock, False) as acquired:
#                 if acquired:
#                     self.__epoll.modify(detect_fileno, self.__hup_eventmask)
#                     try:
#                         while True:
#                             client_socket, address = self.__listener_by_fileno[detect_fileno].accept()
#                             client_socket_fileno = client_socket.fileno()
#                             client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#                             client_socket.setblocking(False)
                            
#                             if detect_fileno in self.__exlistener_by_fileno:
#                                 self.__exsocket_by_fileno.update({client_socket_fileno : client_socket})
                                
#                                 inlistener = self.__inlistener_by_exlistener_fileno[detect_fileno]
#                                 self.__inlistener_by_exfileno.update({client_socket_fileno : inlistener})
                                
#                             elif detect_fileno in self.__inlistener_by_fileno:
#                                 # ipc
#                                 self.__insocket_by_fileno.update({client_socket_fileno : client_socket})
#                                 self.__insockets_by_inlistener_fileno[detect_fileno].update({client_socket_fileno : client_socket})
                                
#                                 exlistener = self.__exlistener_by_inlistener_fileno[detect_fileno]
#                                 self.__exlistener_by_infileno.update({client_socket_fileno : exlistener})
                                
#                             self.__client_by_fileno.update({client_socket_fileno : client_socket})
#                             self.__lock_by_fileno.update({client_socket_fileno : threading.Lock()})
#                             self.__send_buffer_queue_by_fileno.update({client_socket_fileno : queue.Queue()})
#                             self.__sending_buffer_by_fileno.update({client_socket_fileno : b''})
                            
#                             self.__epoll.register(client_socket, self.__recv_eventmask)    
                            
#                     except BlockingIOError as e:
#                         if e.errno == socket.EAGAIN:
#                             pass
#                         else:
#                             raise e
#                     self.__epoll.modify(detect_fileno, self.__listener_eventmask)
                
#     def __get_insocket_by_exsocket(self, exsocket_fileno:int) -> socket.socket:
#         inlistener = self.__inlistener_by_exfileno.get(exsocket_fileno)
#         insockets_dict = self.__insockets_by_inlistener_fileno.get(inlistener.fileno())
#         insockets = list(insockets_dict.values())
#         print(f"insockets : {len(insockets)}")
#         if insockets[0]:
#             return insockets[0] #temp
#         else:
#             return None
        
#     def __epollout_work(self, detect_fileno:int):
#         lock = self.__lock_by_fileno.get(detect_fileno)
#         if lock:
#             with self.__acquire_blocking(lock, False) as acquired:
#                 if acquired:
#                     self.__epoll.modify(detect_fileno, self.__hup_eventmask)
#                     send_bytes = 0
#                     try:
#                         while True:
#                             if self.__sending_buffer_by_fileno[detect_fileno] == b'':
#                                 self.__sending_buffer_by_fileno[detect_fileno] = self.__send_buffer_queue_by_fileno[detect_fileno].get_nowait()                                
#                             send_length = self.__client_by_fileno[detect_fileno].send(self.__sending_buffer_by_fileno[detect_fileno])
#                             if send_length <= 0:
#                                 break
#                             send_bytes += send_length
#                             self.__sending_buffer_by_fileno[detect_fileno] = self.__sending_buffer_by_fileno[detect_fileno][send_length:]
#                     except BlockingIOError as e:
#                         if e.errno == socket.EAGAIN:
#                             pass
#                         else:
#                             raise e
#                     except queue.Empty:
#                         pass
                    
#                     if self.__sending_buffer_by_fileno[detect_fileno] != b'':
#                         self.__epoll.modify(detect_fileno, self.__send_eventmask)
#                     elif not self.__send_buffer_queue_by_fileno[detect_fileno].empty():
#                         self.__epoll.modify(detect_fileno, self.__send_eventmask)
#                     else:
#                         self.__epoll.modify(detect_fileno, self.__recv_eventmask)
#                 # else:
#                 #     print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] send lock False ")
        
#     def __put_send(self, send_fileno:int, data:bytes = None):
#         try:
#             self.__send_buffer_queue_by_fileno[send_fileno].put_nowait(data)
#         except KeyError:
#             # removed
#             pass
#         try:
#             self.__epoll.modify(send_fileno, self.__send_eventmask)
#         except FileNotFoundError:
#             pass
#         except OSError as e:
#             if e.errno == errno.EBADF:
#                 print("e.errno == errno.EBADF self.__epoll.modify(send_fileno, self.__send_eventmask)")
#                 pass
#             elif e.errno == errno.EBADFD:
#                 print("e.errno == errno.EBADFD self.__epoll.modify(send_fileno, self.__send_eventmask)")
#                 pass
#             else:
#                 raise e
            
#     def __epollin_from_external(self, detect_fileno):
#         lock = self.__lock_by_fileno.get(detect_fileno)
#         if lock:
#             with self.__acquire_blocking(lock, False) as acquired:
#                 if acquired:
#                     self.__epoll.modify(detect_fileno, self.__hup_eventmask)
#                     exsocket = self.__exsocket_by_fileno.get(detect_fileno)
#                     recv_bytes = b''
#                     try:
#                         while True:
#                             temp_recv_bytes = exsocket.recv(self.__buffer_size)
#                             if temp_recv_bytes == None or temp_recv_bytes == -1 or temp_recv_bytes == b'':
#                                 break
#                             else:
#                                 recv_bytes += temp_recv_bytes
                        
#                     except BlockingIOError as e:
#                         if e.errno == socket.EAGAIN:
#                             pass
#                         else:
#                             raise e
                    
#                     self.__epoll.modify(detect_fileno, self.__recv_eventmask)
                    
#                     send_data = {
#                         'fileno' : detect_fileno,
#                         'data' : ""
#                     }
#                     send_str = json.dumps(send_data)
#                     send_bytes = send_str.encode()
#                     print(f"[{threading.get_ident()}:TID] {send_bytes}")
                    
#                     send_bytes = send_bytes[:-2] + recv_bytes + send_bytes[-2:]
#                     print(send_bytes)
                    
#                     insocket = self.__get_insocket_by_exsocket(detect_fileno)
#                     if insocket:
#                         self.__put_send(insocket.fileno(), send_bytes)
                        
    
#     def __epollin_from_internal(self, detect_fileno):
#         lock = self.__lock_by_fileno.get(detect_fileno)
#         if lock:
#             with self.__acquire_blocking(lock, False) as acqiured:
#                 if acqiured:
#                     insocket = self.__insocket_by_fileno.get(detect_fileno)
#                     recv_bytes = insocket.recv(self.__buffer_size)
#                     recv_data = json.loads(recv_bytes)
#                     exfileno = recv_data['fileno']
#                     exsocket = self.__exsocket_by_fileno.get(exfileno)
#                     if exsocket:
#                         self.__put_send(exsocket.fileno(), recv_bytes)
    
#     def __epoll_thread_function(self):
#         # EPOLLIN = 0x001,
#         # EPOLLPRI = 0x002,
#         # EPOLLOUT = 0x004,
#         # EPOLLRDNORM = 0x040,
#         # EPOLLRDBAND = 0x080,
#         # EPOLLWRNORM = 0x100,
#         # EPOLLWRBAND = 0x200,
#         # EPOLLMSG = 0x400,
#         # EPOLLERR = 0x008,
#         # EPOLLHUP = 0x010,
#         # EPOLLRDHUP = 0x2000,
#         # EPOLLONESHOT = (1 << 30),
#         # EPOLLET = (1 << 31)

#         try:
#             tid = threading.get_ident()
#             print(f"{datetime.now()} [{tid}:TID] Start Epoll Work")
#             while self.__is_running.value:
#                 events = self.__epoll.poll()
#                 for detect_fileno, detect_event in events:
#                     if detect_fileno == self.__close_event_listener.fileno():
#                         if tid in self.__running_thread_by_tid:
#                             self.__finish_thread_by_tid[tid] = self.__running_thread_by_tid.pop(tid)
#                         else:
#                             print(f'unknown thread {tid} - this is impossible')
#                             _tid, runnning_thread = self.__running_thread_by_tid.popitem()
#                             self.__finish_thread_by_tid[_tid] = runnning_thread
                            
#                         if 0 < len(self.__running_thread_by_tid):
#                             self.__close_event.send(b'close')
#                         else:
#                             self.__epoll.close()
#                             client_fileno_list = list(self.__client_by_fileno.keys())
#                             for fileno in client_fileno_list:
#                                 self.close_client(fileno)
                    
#                     elif detect_fileno in self.__listener_by_fileno:
#                         if detect_event & (select.EPOLLHUP | select.EPOLLRDHUP):
#                             print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] Listener HUP")
                            
#                         elif detect_event & select.EPOLLIN:
#                             self.__epoll_accepting(detect_fileno)
                        
#                         else:
#                             print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] Accept Unknown")
                    
                            
#                     elif detect_fileno in self.__client_by_fileno:
#                         try:
#                             if detect_event & (select.EPOLLHUP | select.EPOLLRDHUP):
#                                 try:
#                                     self.__epoll.unregister(detect_fileno)
#                                     self.close_client(detect_fileno)
#                                 except FileNotFoundError:
#                                     print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] FileNotFoundError")
                                    
#                             elif detect_event & select.EPOLLOUT:
#                                 self.__epollout_work(detect_fileno)
                                
#                                 # if detect_fileno in self.__exsocket_by_fileno:
#                                 #     self.__epollout_from_external(detect_fileno)
                                    
#                                 # elif detect_fileno in self.__insocket_by_fileno: 
#                                 #     self.__epollout_from_internal(detect_fileno)
                                
#                             elif detect_event & select.EPOLLIN:
#                                 if detect_fileno in self.__exsocket_by_fileno:
#                                     self.__epollin_from_external(detect_fileno)
                                
#                                 elif detect_fileno in self.__insocket_by_fileno:
#                                     self.__epollin_from_internal(detect_fileno)
                                    
#                             else:
#                                 print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] Unknown Event. {detect_event:#06x}")
#                         except BrokenPipeError:
#                             print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] BrokenPipeError")
#                         except ConnectionResetError:
#                             print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] ConnectionResetError")
#                     else:
#                         print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] Unknown Detection. {detect_event:#06x}")
                        
#         except Exception as e:
#             print(e, traceback.format_exc())
        
#         print(f"{datetime.now()} [{tid}:TID] Finish Epoll Work")
