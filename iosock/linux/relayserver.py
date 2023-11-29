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

class RelayServer:
    def __init__(self) -> None:
        self.__buffer_size = 8196
        self.__is_running = multiprocessing.Value(ctypes.c_bool, False)
        
        self.__listen_data_list = []
        
        self.__client_by_fileno = collections.defaultdict(socket.socket)
        self.__lock_by_fileno = collections.defaultdict(threading.Lock)
        self.__send_buffer_queue_by_fileno = collections.defaultdict(queue.Queue)
        self.__sending_buffer_by_fileno = collections.defaultdict(bytes)
        self.__running_thread_by_tid = collections.defaultdict(threading.Thread)
        self.__finish_thread_by_tid = collections.defaultdict(threading.Thread)
        
        self.__listener_by_fileno = collections.defaultdict(socket.socket)
        self.__exlistener_by_fileno = collections.defaultdict(socket.socket)
        self.__inlistener_by_fileno = collections.defaultdict(socket.socket)
        
        self.__exsocket_by_fileno = collections.defaultdict(socket.socket)
        self.__insocket_by_fileno = collections.defaultdict(socket.socket)
        
        self.__exlistener_by_infileno = collections.defaultdict(socket.socket)
        self.__inlistener_by_exfileno = collections.defaultdict(socket.socket)
        
        self.__insockets_by_inlistener_fileno = collections.defaultdict(dict)
        self.__exsockets_by_exlistener_fileno = collections.defaultdict(dict)
        
        self.__exlistener_by_inlistener_fileno = collections.defaultdict(socket.socket)
        self.__inlistener_by_exlistener_fileno = collections.defaultdict(socket.socket)
        
        self.__listener_eventmask = select.EPOLLIN | select.EPOLLPRI | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
        self.__hup_eventmask = select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
        self.__recv_eventmask = select.EPOLLIN | select.EPOLLPRI | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
        self.__send_eventmask = select.EPOLLIN | select.EPOLLPRI | select.EPOLLOUT | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
        self.__closer_eventmask = select.EPOLLIN | select.EPOLLPRI | select.EPOLLHUP | select.EPOLLRDHUP
        
    # @contextmanager
    # def __acquire_timeout(self, lock:threading.Lock, timeout:float):
    #     result = lock.acquire(timeout=timeout)
    #     try:
    #         yield result
    #     finally:
    #         if result:
    #             lock.release()
    @contextmanager
    def __acquire_blocking(self, lock:threading.Lock, blocking:bool):
        result = lock.acquire(blocking=blocking)
        try:
            yield result
        finally:
            if result:
                lock.release()

    def listen(self, listen_ip:str, listen_port:int, backlog:int) -> socket.socket:
        listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        recv_buf_size = listen_socket.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        send_buf_size = listen_socket.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
        listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, recv_buf_size*2)
        listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, send_buf_size*2)
        listen_socket.setblocking(False)
        
        listen_socket.bind((listen_ip, listen_port))
        listen_socket.listen(backlog)
        return listen_socket
        
    def append_listen(self, listen_ip:str, external_port:int, internal_port:int, backlog:int = 5):
        self.__listen_data_list.append((listen_ip, external_port, internal_port, backlog))
        
    def start(self, count_thread:int = 1):
        self.__is_running.value = True
        self.__epoll = select.epoll()
        self.__close_event, self.__close_event_listener = socket.socketpair()
        self.__epoll.register(self.__close_event_listener, self.__closer_eventmask)
    
        for _ in range(count_thread):
            et = threading.Thread(target=self.__epoll_thread_function)
            et.start()
            self.__running_thread_by_tid[et.ident] = et
        
        for listen_data in self.__listen_data_list:
            listen_ip = listen_data[0]
            external_port = listen_data[1]
            internal_port = listen_data[2]
            backlog = listen_data[3]
            
            inlistener = self.listen('localhost', internal_port, backlog)
            inlistener_fileno = inlistener.fileno()
            print(f"Listen 'localhost':{internal_port}")
            
            exlistener = self.__listen(listen_ip, external_port, backlog)
            exlistener_fileno = exlistener.fileno()
            print(f"Listen {listen_ip}:{internal_port}")
            
            self.__inlistener_by_fileno.update({inlistener_fileno: inlistener})
            self.__exlistener_by_fileno.update({exlistener_fileno: exlistener})
            
            self.__lock_by_fileno.update({inlistener_fileno : threading.Lock()})
            self.__lock_by_fileno.update({exlistener_fileno : threading.Lock()})
            
            self.__listener_by_fileno.update({inlistener_fileno: inlistener})
            self.__listener_by_fileno.update({exlistener_fileno: exlistener})
            
            self.__insockets_by_inlistener_fileno.update({inlistener_fileno:{}})
            self.__exsockets_by_exlistener_fileno.update({exlistener_fileno:{}})
            
            self.__inlistener_by_exlistener_fileno.update({exlistener_fileno: inlistener})
            self.__exlistener_by_inlistener_fileno.update({inlistener_fileno: exlistener})
        
            self.__epoll.register(inlistener_fileno, self.__listener_eventmask)
            self.__epoll.register(exlistener_fileno, self.__listener_eventmask)
        
    def close_client(self, fileno:int):
        try:
            _ = self.__lock_by_fileno.pop(fileno)
        except KeyError:
            pass
        client_socket:socket.socket = None
        try:
            self.__client_by_fileno.pop(fileno)
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
        print(f"{datetime.now()} [{threading.get_ident()}:TID] [{fileno:3}] Try Close. send buffer remain:{len(sending_buffer)} bytes. queue remain:{len_send_buffer_queue}")
        
        if send_buffer_queue:
            while not send_buffer_queue.empty():
                _ = send_buffer_queue.get_nowait()
            
        if client_socket:
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except ConnectionResetError:
                print(f"{datetime.now()} [{threading.get_ident()}:TID] [{fileno:3}] ConnectionResetError")
                
            except BrokenPipeError:
                print(f"{datetime.now()} [{threading.get_ident()}:TID] [{fileno:3}] BrokenPipeError")
                
            except OSError as e:
                if e.errno == errno.ENOTCONN: # errno 107
                    print(f"{datetime.now()} [{threading.get_ident()}:TID] [{fileno:3}] ENOTCONN")
                else:
                    raise e
            
            client_socket.close()
        print(f"{datetime.now()} [{threading.get_ident()}:TID] [{fileno:3}] Closed.")

    def join(self):
        tids = list(self.__running_thread_by_tid.keys())
        for tid in tids:
            if tid in self.__running_thread_by_tid:
                self.__running_thread_by_tid[tid].join()
        
        for tid in self.__finish_thread_by_tid:
            self.__finish_thread_by_tid[tid].join()
            print(f"[{tid}:TID] joined")

    def close(self):
        self.__is_running.value = False
        self.__close_event.send(b'close')
        
    def __epoll_accepting(self, detect_fileno):
        lock = self.__lock_by_fileno.get(detect_fileno)
        if lock:
            with self.__acquire_blocking(lock, False) as acquired:
                if acquired:
                    self.__epoll.modify(detect_fileno, self.__hup_eventmask)
                    try:
                        while True:
                            client_socket, address = self.__listener_by_fileno[detect_fileno].accept()
                            client_socket_fileno = client_socket.fileno()
                            client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                            client_socket.setblocking(False)
                            
                            if detect_fileno in self.__exlistener_by_fileno:
                                self.__exsocket_by_fileno.update({client_socket_fileno : client_socket})
                                
                                inlistener = self.__inlistener_by_exlistener_fileno[detect_fileno]
                                self.__inlistener_by_exfileno.update({client_socket_fileno : inlistener})
                                
                            elif detect_fileno in self.__inlistener_by_fileno:
                                # ipc
                                self.__insocket_by_fileno.update({client_socket_fileno : client_socket})
                                self.__insockets_by_inlistener_fileno[detect_fileno].update({client_socket_fileno : client_socket})
                                
                                exlistener = self.__exlistener_by_inlistener_fileno[detect_fileno]
                                self.__exlistener_by_infileno.update({client_socket_fileno : exlistener})
                                
                            self.__client_by_fileno.update({client_socket_fileno : client_socket})
                            self.__lock_by_fileno.update({client_socket_fileno : threading.Lock()})
                            self.__send_buffer_queue_by_fileno.update({client_socket_fileno : queue.Queue()})
                            self.__sending_buffer_by_fileno.update({client_socket_fileno : b''})
                            
                            self.__epoll.register(client_socket, self.__recv_eventmask)    
                            
                    except BlockingIOError as e:
                        if e.errno == socket.EAGAIN:
                            pass
                        else:
                            raise e
                    self.__epoll.modify(detect_fileno, self.__listener_eventmask)
                
    def __get_insocket_by_exsocket(self, exsocket_fileno:int) -> socket.socket:
        inlistener = self.__inlistener_by_exfileno.get(exsocket_fileno)
        insockets_dict = self.__insockets_by_inlistener_fileno.get(inlistener.fileno())
        insockets = list(insockets_dict.values())
        print(f"insockets : {len(insockets)}")
        if insockets[0]:
            return insockets[0] #temp
        else:
            return None
        
    def __epollout_work(self, detect_fileno:int):
        lock = self.__lock_by_fileno.get(detect_fileno)
        if lock:
            with self.__acquire_blocking(lock, False) as acquired:
                if acquired:
                    self.__epoll.modify(detect_fileno, self.__hup_eventmask)
                    send_bytes = 0
                    try:
                        while True:
                            if self.__sending_buffer_by_fileno[detect_fileno] == b'':
                                self.__sending_buffer_by_fileno[detect_fileno] = self.__send_buffer_queue_by_fileno[detect_fileno].get_nowait()                                
                            send_length = self.__client_by_fileno[detect_fileno].send(self.__sending_buffer_by_fileno[detect_fileno])
                            if send_length <= 0:
                                break
                            send_bytes += send_length
                            self.__sending_buffer_by_fileno[detect_fileno] = self.__sending_buffer_by_fileno[detect_fileno][send_length:]
                    except BlockingIOError as e:
                        if e.errno == socket.EAGAIN:
                            pass
                        else:
                            raise e
                    except queue.Empty:
                        pass
                    
                    if self.__sending_buffer_by_fileno[detect_fileno] != b'':
                        self.__epoll.modify(detect_fileno, self.__send_eventmask)
                    elif not self.__send_buffer_queue_by_fileno[detect_fileno].empty():
                        self.__epoll.modify(detect_fileno, self.__send_eventmask)
                    else:
                        self.__epoll.modify(detect_fileno, self.__recv_eventmask)
                # else:
                #     print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] send lock False ")
        
    def __put_send(self, send_fileno:int, data:bytes = None):
        try:
            self.__send_buffer_queue_by_fileno[send_fileno].put_nowait(data)
        except KeyError:
            # removed
            pass
        try:
            self.__epoll.modify(send_fileno, self.__send_eventmask)
        except FileNotFoundError:
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
            
    def __epollin_from_external(self, detect_fileno):
        lock = self.__lock_by_fileno.get(detect_fileno)
        if lock:
            with self.__acquire_blocking(lock, False) as acquired:
                if acquired:
                    self.__epoll.modify(detect_fileno, self.__hup_eventmask)
                    exsocket = self.__exsocket_by_fileno.get(detect_fileno)
                    recv_bytes = b''
                    try:
                        while True:
                            temp_recv_bytes = exsocket.recv(self.__buffer_size)
                            if temp_recv_bytes == None or temp_recv_bytes == -1 or temp_recv_bytes == b'':
                                break
                            else:
                                recv_bytes += temp_recv_bytes
                        
                    except BlockingIOError as e:
                        if e.errno == socket.EAGAIN:
                            pass
                        else:
                            raise e
                    
                    self.__epoll.modify(detect_fileno, self.__recv_eventmask)
                    
                    send_data = {
                        'fileno' : detect_fileno,
                        'data' : ""
                    }
                    send_str = json.dumps(send_data)
                    send_bytes = send_str.encode()
                    print(f"[{threading.get_ident()}:TID] {send_bytes}")
                    
                    send_bytes = send_bytes[:-2] + recv_bytes + send_bytes[-2:]
                    print(send_bytes)
                    
                    insocket = self.__get_insocket_by_exsocket(detect_fileno)
                    if insocket:
                        self.__put_send(insocket.fileno(), send_bytes)
                        
    
    def __epollin_from_internal(self, detect_fileno):
        lock = self.__lock_by_fileno.get(detect_fileno)
        if lock:
            with self.__acquire_blocking(lock, False) as acqiured:
                if acqiured:
                    insocket = self.__insocket_by_fileno.get(detect_fileno)
                    recv_bytes = insocket.recv(self.__buffer_size)
                    recv_data = json.loads(recv_bytes)
                    exfileno = recv_data['fileno']
                    exsocket = self.__exsocket_by_fileno.get(exfileno)
                    if exsocket:
                        self.__put_send(exsocket.fileno(), recv_bytes)
    
    def __epoll_thread_function(self):
        # EPOLLIN = 0x001,
        # EPOLLPRI = 0x002,
        # EPOLLOUT = 0x004,
        # EPOLLRDNORM = 0x040,
        # EPOLLRDBAND = 0x080,
        # EPOLLWRNORM = 0x100,
        # EPOLLWRBAND = 0x200,
        # EPOLLMSG = 0x400,
        # EPOLLERR = 0x008,
        # EPOLLHUP = 0x010,
        # EPOLLRDHUP = 0x2000,
        # EPOLLONESHOT = (1 << 30),
        # EPOLLET = (1 << 31)

        try:
            tid = threading.get_ident()
            print(f"{datetime.now()} [{tid}:TID] Start Epoll Work")
            while self.__is_running.value:
                events = self.__epoll.poll()
                for detect_fileno, detect_event in events:
                    if detect_fileno == self.__close_event_listener.fileno():
                        if tid in self.__running_thread_by_tid:
                            self.__finish_thread_by_tid[tid] = self.__running_thread_by_tid.pop(tid)
                        else:
                            print(f'unknown thread {tid} - this is impossible')
                            _tid, runnning_thread = self.__running_thread_by_tid.popitem()
                            self.__finish_thread_by_tid[_tid] = runnning_thread
                            
                        if 0 < len(self.__running_thread_by_tid):
                            self.__close_event.send(b'close')
                        else:
                            self.__epoll.close()
                            client_fileno_list = list(self.__client_by_fileno.keys())
                            for fileno in client_fileno_list:
                                self.close_client(fileno)
                    
                    elif detect_fileno in self.__listener_by_fileno:
                        if detect_event & (select.EPOLLHUP | select.EPOLLRDHUP):
                            print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] Listener HUP")
                            
                        elif detect_event & select.EPOLLIN:
                            self.__epoll_accepting(detect_fileno)
                        
                        else:
                            print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] Accept Unknown")
                    
                            
                    elif detect_fileno in self.__client_by_fileno:
                        try:
                            if detect_event & (select.EPOLLHUP | select.EPOLLRDHUP):
                                try:
                                    self.__epoll.unregister(detect_fileno)
                                    self.close_client(detect_fileno)
                                except FileNotFoundError:
                                    print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] FileNotFoundError")
                                    
                            elif detect_event & select.EPOLLOUT:
                                self.__epollout_work(detect_fileno)
                                
                                # if detect_fileno in self.__exsocket_by_fileno:
                                #     self.__epollout_from_external(detect_fileno)
                                    
                                # elif detect_fileno in self.__insocket_by_fileno: 
                                #     self.__epollout_from_internal(detect_fileno)
                                
                            elif detect_event & select.EPOLLIN:
                                if detect_fileno in self.__exsocket_by_fileno:
                                    self.__epollin_from_external(detect_fileno)
                                
                                elif detect_fileno in self.__insocket_by_fileno:
                                    self.__epollin_from_internal(detect_fileno)
                                    
                            else:
                                print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] Unknown Event. {detect_event:#06x}")
                        except BrokenPipeError:
                            print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] BrokenPipeError")
                        except ConnectionResetError:
                            print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] ConnectionResetError")
                    else:
                        print(f"{datetime.now()} [{threading.get_ident()}:TID] [{detect_fileno:3}] Unknown Detection. {detect_event:#06x}")
                        
        except Exception as e:
            print(e, traceback.format_exc())
        
        print(f"{datetime.now()} [{tid}:TID] Finish Epoll Work")
