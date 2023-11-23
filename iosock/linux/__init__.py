import socket
import select
import platform
import multiprocessing
import ctypes
import threading
import collections
import queue
import errno
import traceback
from datetime import datetime

from multiprocessing.pool import ThreadPool

from .. import abstract
from .. import exception

from contextlib import contextmanager

class Client(abstract.ClientBase):
    def __init__(self) -> None:
        print("linux client")
        
class Server(abstract.ServerBase):
    def __init__(self) -> None:
        self.__buffer_size = 8196
        self.__is_running = multiprocessing.Value(ctypes.c_bool, False)
        
        self.__socket_by_fileno = collections.defaultdict(socket.socket)
        self.__lock_by_fileno = collections.defaultdict(threading.Lock)
        self.__send_buffer_queue_by_fileno = collections.defaultdict(queue.Queue)
        self.__sending_buffer_by_fileno = collections.defaultdict(bytes)
        self.__thread_by_tid = collections.defaultdict(threading.Thread)
        
        self.__recv_queue = queue.Queue()
        self.__send_fileno_queue = queue.Queue()
        
        self.__recv_eventmask = select.EPOLLIN | select.EPOLLPRI | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
        self.__send_eventmask = select.EPOLLIN | select.EPOLLPRI | select.EPOLLOUT | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
        
    @contextmanager
    def __acquire_timeout(self, lock:threading.Lock, timeout:float):
        result = lock.acquire(timeout=timeout)
        try:
            yield result
        finally:
            if result:
                lock.release()

    def start(self, listen_ip:str, listen_port:int, count_thread:int, backlog:int = 5):
        self.__listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        self.__listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # self.__listen_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        
        recv_buf_size = self.__listen_socket.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        send_buf_size = self.__listen_socket.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
        self.__listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, recv_buf_size*2)
        self.__listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, send_buf_size*2)
        
        self.__listen_socket.setblocking(False)
        self.__listen_socket.bind((listen_ip, listen_port))
        self.__listen_socket.listen(backlog)
        
        
        listen_socket_fileno = self.__listen_socket.fileno()
        res = self.__listen_socket.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        print(f"[{listen_socket_fileno:2}] SO_RCVBUF : {res}")
        res = self.__listen_socket.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
        print(f"[{listen_socket_fileno:2}] SO_SNDBUF : {res}")
        
        res = self.__listen_socket.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)
        print(f"[{listen_socket_fileno:2}] TCP_NODELAY : {res}")
        
        self.__socket_by_fileno.update({listen_socket_fileno : self.__listen_socket})
        self.__lock_by_fileno.update({listen_socket_fileno : threading.Lock()})
        self.__send_buffer_queue_by_fileno.update({listen_socket_fileno : queue.Queue()})
        self.__sending_buffer_by_fileno.update({listen_socket_fileno : b''})
        
        self.__epoll = select.epoll()
        self.__listener_eventmask = select.EPOLLIN | select.EPOLLPRI | select.EPOLLHUP | select.EPOLLRDHUP
        self.__epoll.register(self.__listen_socket, self.__listener_eventmask)
        
        self.__is_running.value = True
        
        for _ in range(count_thread):
            et = threading.Thread(target=self.__epoll_thread_function)
            et.start()
            self.__thread_by_tid[et.ident] = et
            
        self.__close_event, self.__close_event_listener = socket.socketpair()
        self.__closer_eventmask = select.EPOLLIN | select.EPOLLPRI | select.EPOLLHUP | select.EPOLLRDHUP
        self.__epoll.register(self.__close_event_listener, self.__closer_eventmask)
            
    def close(self):
        self.__is_running.value = False
        self.__close_event.send(b'close')
        
    def close_client(self, fileno:int):
        try:
            _ = self.__lock_by_fileno.pop(fileno)
        except KeyError:
            pass
        client_socket:socket.socket = None
        try:
            self.__socket_by_fileno.pop(fileno)
        except KeyError:
            pass
        len_send_buffer_queue = -1
        send_buffer_queue:queue.Queue = None
        try:
            send_buffer_queue = self.__send_buffer_queue_by_fileno.pop(fileno)
            len_send_buffer_queue = len(send_buffer_queue.queue)
        except KeyError:
            pass
        sending_buffer:bytes = b''
        try:
            sending_buffer = self.__sending_buffer_by_fileno.pop(fileno)
        except KeyError:
            pass
        
        print(f"[{fileno}] Try Close. send buffer remain:{len(sending_buffer)} bytes. queue remain:{len_send_buffer_queue}")
        
        if send_buffer_queue:
            while not send_buffer_queue.empty():
                _ = send_buffer_queue.get_nowait()
            
        if client_socket:
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except ConnectionResetError:
                pass
            except BrokenPipeError:
                print(f"[{fileno}] BrokenPipeError")
                pass
            except OSError as e:
                if e.errno == errno.ENOTCONN: # errno 107
                    print(f"[{fileno}] ENOTCONN")
                    pass
                else:
                    raise e
            except Exception as e:
                print(e, traceback.format_exc())
            client_socket.close()
        
        print(f'[{fileno}] Closed')

    def recv(self):
        return self.__recv_queue.get()
    
    def send(self, send_fileno:int, data:bytes = None):
        self.__send_buffer_queue_by_fileno[send_fileno].put_nowait(data)
        self.__epoll.modify(send_fileno, self.__send_eventmask)
            
#####################################################################################################################
#####################################################################################################################
#####################################################################################################################
#####################################################################################################################
#####################################################################################################################
#####################################################################################################################
            
    def __epoll_accepting(self, detect_fileno:int):
        lock = self.__lock_by_fileno.get(detect_fileno)
        if lock:
            with self.__acquire_timeout(lock, 0.1) as acquired:
                if acquired:
                    try:
                        while True:
                            client_socket, address = self.__listen_socket.accept()
                            client_socket_fileno = client_socket.fileno()
                            client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                            res = client_socket.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)
                            print(f"[{client_socket_fileno:2}] TCP_NODELAY : {res}")
                            res = client_socket.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
                            print(f"[{client_socket_fileno:2}] SO_RCVBUF : {res}")
                            res = client_socket.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
                            print(f"[{client_socket_fileno:2}] SO_SNDBUF : {res}")
                            
                            client_socket.setblocking(False)
                            
                            accepting_text = f"[{client_socket_fileno:2}] accept {address[0]}:{address[1]}"
                            exist_client_socket = self.__socket_by_fileno.get(client_socket_fileno)
                            if exist_client_socket:
                                try:
                                    exist_client_socket.shutdown(socket.SHUT_RDWR)
                                    accepting_text += "exist shutdown,"
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
                        
                            self.__socket_by_fileno.update({client_socket_fileno : client_socket})
                            self.__lock_by_fileno.update({client_socket_fileno : threading.Lock()})
                            self.__send_buffer_queue_by_fileno.update({client_socket_fileno : queue.Queue()})
                            self.__sending_buffer_by_fileno.update({client_socket_fileno : b''})
                            
                            self.__epoll.register(client_socket, self.__recv_eventmask)    
                            
                            print(accepting_text)
                            
                    except BlockingIOError as e:
                        if e.errno == socket.EAGAIN:
                            pass
                        else:
                            raise e
                else:
                    self.__epoll.modify(self.__listen_socket, self.__listener_eventmask)
                        
        
            
    def __epollin_work(self, detect_fileno:int):
        lock = self.__lock_by_fileno.get(detect_fileno)
        if lock:
            with self.__acquire_timeout(lock, 0.1) as acqiured:
                client_socket = self.__socket_by_fileno.get(detect_fileno)
                if acqiured:
                    # text_print = f"{datetime.now()} [{detect_fileno:2}] [{threading.get_ident()}] recv "
                    recv_bytes = b''
                    try:
                        while True:
                            temp_recv_bytes = client_socket.recv(self.__buffer_size)
                            if temp_recv_bytes == None or temp_recv_bytes == -1 or temp_recv_bytes == b'':
                                break
                            else:
                                recv_bytes += temp_recv_bytes
                        
                    except BlockingIOError as e:
                        if e.errno == socket.EAGAIN:
                            pass
                        else:
                            raise e
                        
                    if recv_bytes:
                        self.__recv_queue.put_nowait({
                            "fileno": detect_fileno,
                            "data": recv_bytes
                        })
                else:
                    print(f"{datetime.now()} [{detect_fileno:2}] [{threading.get_ident()}] recv work timeout")
    
    def __epollout_work(self, detect_fileno:int):
        try:
            # text_print = f"{datetime.now()} [{detect_fileno:2}] "
            with self.__acquire_timeout(self.__lock_by_fileno[detect_fileno], 0.1) as acqiured:
                # text_print += f"[{threading.get_ident()}] send "
                send_bytes = 0
                if acqiured:
                    try:
                        while True:
                            if self.__sending_buffer_by_fileno[detect_fileno] == b'':
                                self.__sending_buffer_by_fileno[detect_fileno] = self.__send_buffer_queue_by_fileno[detect_fileno].get_nowait()                                
                            send_length = self.__socket_by_fileno[detect_fileno].send(self.__sending_buffer_by_fileno[detect_fileno])
                            if send_length <= 0:
                                break
                            send_bytes += send_length
                            self.__sending_buffer_by_fileno[detect_fileno] = self.__sending_buffer_by_fileno[detect_fileno][send_length:]
                    except BlockingIOError as e:
                        if e.errno == socket.EAGAIN:
                            pass
                        else:
                            raise e
                    except BrokenPipeError:
                        pass
                    except queue.Empty:
                        pass
                else:
                    # text_print += "send work timeout "
                    print("send work timeout ")
                    
                # text_print += f"{send_bytes:7} bytes. "
        
            if self.__sending_buffer_by_fileno[detect_fileno] != b'':
                self.__epoll.modify(detect_fileno, self.__send_eventmask)
                # text_print += f"retry;buffer remain:{len(self.__sending_buffer_by_fileno[detect_fileno])} queue remain:{len(self.__send_buffer_queue_by_fileno[detect_fileno].queue)}"
                
            elif not self.__send_buffer_queue_by_fileno[detect_fileno].empty():
                self.__epoll.modify(detect_fileno, self.__send_eventmask)
                # text_print += f"retry;buffer remain:{len(self.__sending_buffer_by_fileno[detect_fileno])} queue remain:{len(self.__send_buffer_queue_by_fileno[detect_fileno].queue)}"
                
            else:
                self.__epoll.modify(detect_fileno, self.__recv_eventmask)
                # text_print += f"to recv"
                
            # print(text_print)
            
        except Exception as e:
            print(e, traceback.format_exc())
    
    def __epoll_thread_function(self):
        try:
            while self.__is_running.value:
                events = self.__epoll.poll()
                for detect_fileno, detect_event in events:
                    if detect_fileno == self.__listen_socket.fileno():
                        if detect_event & (select.EPOLLHUP | select.EPOLLRDHUP):
                            # self.__listen_socket = self.__get_listener(self.__listen_ip, self.__listen_port, self.__backlog)
                            print(f"{datetime.now()} [{detect_fileno:2}] [{threading.get_ident()}] Listener HUP")
                            pass
                            
                        elif detect_event & select.EPOLLIN:
                            self.__epoll_accepting(detect_fileno)
                        
                        else:
                            print(f"{datetime.now()} [{detect_fileno:2}] [{threading.get_ident()}] accept", detect_fileno, detect_event)
                    
                    elif detect_fileno == self.__close_event_listener.fileno():
                        tid = threading.get_ident()
                        if tid in self.__thread_by_tid:
                            self.__thread_by_tid[tid]
        
        
                        self.__len_close_epoll_thread.value += 1
                        if self.__len_close_epoll_thread.value < len(self.__epoll_threads):
                            self.__close_event.send(b'close')
                        else:
                            self.__epoll.close()
                            self.__send_fileno_queue.put_nowait(None)
                            self.__recv_queue.put_nowait(None)
                            
                            client_fileno_list = list(self.__socket_by_fileno.keys())
                            for fileno in client_fileno_list:
                                self.close_client(fileno)
                        
                                
                    elif detect_event & (select.EPOLLHUP | select.EPOLLRDHUP):
                        try:
                            self.__epoll.unregister(detect_fileno)
                            self.close_client(detect_fileno)
                        except Exception as e:
                            print(detect_fileno, e, traceback.format_exc())
                    
                    elif detect_event & select.EPOLLOUT:
                        self.__epollout_work(detect_fileno)
                        
                    elif detect_event & select.EPOLLIN:
                        self.__epollin_work(detect_fileno)
                        
                    else:
                        print("unknown", detect_fileno, detect_event)
        except Exception as e:
            print(e, traceback.format_exc())
        
        print(f"{datetime.now()} [{threading.get_ident()}] Finish Epoll")
