import socket
import select
import multiprocessing
import ctypes
import threading
import collections
import queue
import errno
import traceback
from datetime import datetime

class EpollServer():
    def __init__(self) -> None:
        self.__buffer_size = 8196
        self.__is_running = multiprocessing.Value(ctypes.c_bool, False)
        self.__is_debug_mode = multiprocessing.Value(ctypes.c_bool, False)
        self.__running_threads = []
        self.__running_thread_by_tid = collections.defaultdict(threading.Thread)
        
        self.__listener_by_ip_port = collections.defaultdict(socket.socket)
        self.__listener_by_fileno = collections.defaultdict(socket.socket)
        
        self.__client_by_fileno = collections.defaultdict(socket.socket)
        self.__listener_fileno_by_client_fileno = collections.defaultdict(int)
        self.__registered_eventmask_by_fileno = collections.defaultdict(int)
        self.__send_lock_by_fileno = collections.defaultdict(threading.Lock)
        self.__recv_lock_by_fileno = collections.defaultdict(threading.Lock)
        
        self.__send_buffer_by_fileno = collections.defaultdict(bytes)
        self.__send_buffer_lock_by_fileno = collections.defaultdict(threading.Lock)
        
        self.__client_fileno_dict_by_listener_fileno = collections.defaultdict(dict)
        
        self.__recv_queue = queue.Queue()
        
        self.__epoll : select.epoll = None
        
        self.__listener_eventmask = select.EPOLLIN | select.EPOLLPRI | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
        self.__recv_eventmask = select.EPOLLIN  | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
        self.__send_recv_eventmask = select.EPOLLIN | select.EPOLLOUT | select.EPOLLHUP | select.EPOLLRDHUP
        self.__closer_eventmask = select.EPOLLIN | select.EPOLLPRI | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
    
    def listen(self, ip:str, port:int, backlog:int = 5):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # listener.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1) # Nagle's
        
        # increase buffer size
        recv_buf_size = listener.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        send_buf_size = listener.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, recv_buf_size*2)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, send_buf_size*2)
        
        listener.setblocking(False)
        listener.bind((ip, port))
        listener.listen(backlog)
        
        listener_fileno = listener.fileno()
        
        self.__listener_by_ip_port.update({f"{ip}:{port}":listener})
        self.__listener_by_fileno.update({listener_fileno : listener})
        
        self.__client_fileno_dict_by_listener_fileno.update({listener_fileno : {}})
        
        if self.__epoll and not self.__epoll.closed:
            # After 'start()'
            self.__epoll.register(listener_fileno, self.__listener_eventmask)
            self.__registered_eventmask_by_fileno.update({listener_fileno : self.__listener_eventmask})

    def unlisten(self, ip:str, port:int):
        listener = self.__listener_by_ip_port.get(f"{ip}:{port}")
        if listener:
            listener.shutdown(socket.SHUT_RDWR)
    
    def start(self, count_threads:int=1, is_debug_mode:bool = False):
        self.__is_running.value = True
        self.__is_debug_mode.value = is_debug_mode
        
        self.__epoll = select.epoll()
        self.__close_event, self.__close_event_listener = socket.socketpair()
        self.__epoll.register(self.__close_event_listener, self.__closer_eventmask)
        
        for _ in range(count_threads):
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

    def recv(self) -> dict:
        '''
        Return
        -
        dict\n
        Error or Close if return is None\n
        dict['type'] : 'accept', 'recv', 'debug', 'close_client'\n
        if dict['type'] == 'accept':
            dict['fileno'] (int) : socket fileno
        elif dict['type'] == 'recv':
            dict['fileno'] (int) : socket fileno
            dict['bytes'] (bytes) : receive bytes
        elif dict['type'] == 'debug':
            dict['message'] (str) : debug message
        elif dict['type'] == 'close_client':
            dict['fileno'] (int) : socket fileno
            
        '''
        if self.__is_running.value:
            recv_data = self.__recv_queue.get()
            if recv_data:
                return recv_data
            else:
                #for multithreading
                self.__is_running.value = False
                self.__recv_queue.put_nowait(None)
                return None
        else:
            #for multithreading
            self.__recv_queue.put_nowait(None)
            return None
    
    def send(self, socket_fileno:int, data:bytes):
        if data is not None and data != b'':
            try:
                send_buffer_lock = self.__send_buffer_lock_by_fileno.get(socket_fileno)
                if send_buffer_lock:
                    with send_buffer_lock:
                        send_buffer = self.__send_buffer_by_fileno.get(socket_fileno)
                        if send_buffer is not None:
                            self.__send_buffer_by_fileno.update({socket_fileno : send_buffer + data})
                    
                send_lock = self.__send_lock_by_fileno.get(socket_fileno)
                if send_lock:
                    with send_lock:
                        registered_eventmask = self.__registered_eventmask_by_fileno.get(socket_fileno)
                        if registered_eventmask is not None and registered_eventmask != self.__send_recv_eventmask:
                            self.__registered_eventmask_by_fileno.update({socket_fileno : self.__send_recv_eventmask})
                            self.__epoll.modify(socket_fileno, self.__send_recv_eventmask)
                
            except KeyError:
                if self.__is_debug_mode.value:
                    self.__recv_queue.put_nowait({
                        "type" : "debug",
                        "message" : f"[{socket_fileno}] send KeyError.\n{traceback.format_exc()}"
                    })
                
            except FileNotFoundError:
                if self.__is_debug_mode.value:
                    self.__recv_queue.put_nowait({
                        "type" : "debug",
                        "message" : f"[{socket_fileno}] send FileNotFoundError.\n{traceback.format_exc()}"
                    })
                    
            except PermissionError:
                if self.__is_debug_mode.value:
                    self.__recv_queue.put_nowait({
                        "type" : "debug",
                        "message" : f"[{socket_fileno}] send PermissionError.\n{traceback.format_exc()}"
                    })
                
            except OSError as e:
                if e.errno == errno.EBADF:
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
            tid_bytes = self.__close_event.recv(32)
            tid = int.from_bytes(tid_bytes, byteorder='big')
            self.__running_thread_by_tid[tid].join()
            
        self.__recv_queue.put_nowait(None)
    
    def __shutdown_listeners(self):
        fileno_list = list(self.__listener_by_fileno.keys())
        for fileno in fileno_list:
            self.__shutdown_listener(fileno)
            
    def __shutdown_listener(self, listener_fileno:int):
        listener = self.__listener_by_fileno.get(listener_fileno)
        if listener:
            listener.shutdown(socket.SHUT_RDWR)
        
    def __close_listener(self, listener_fileno:int):
        try:
            self.__epoll.unregister(listener_fileno)
        except FileNotFoundError:
            pass
        except OSError as e:
            if e.errno == errno.EBADF:
                pass   
            else:
                raise e
        listener = self.__listener_by_fileno.get(listener_fileno)
        if listener:
            listener.close()
            
    def __remove_listener(self, listener_fileno:int):
        try:
            _ = self.__listener_by_fileno.pop(listener_fileno)
        except KeyError:
            pass
        # self.__listener_by_ip_port = collections.defaultdict(socket.socket)
        
    def __unregister(self, socket_fileno:int) -> bool:
        result = False
        try:
            _ = self.__registered_eventmask_by_fileno.pop(socket_fileno)
        except KeyError:
            pass
        
        try:
            self.__epoll.unregister(socket_fileno)
            result = True
        
        except FileNotFoundError:
            result = True
            
        except OSError as e:
            if e.errno == errno.EBADF:
                result = True
            else:
                raise e
        return result
        
    def __shutdown_clients_by_listener(self, listener_fileno:int):
        client_fileno_dict = self.__client_fileno_dict_by_listener_fileno.get(listener_fileno)
        if client_fileno_dict:
            client_fileno_list = list(client_fileno_dict.keys())
            for client_fileno in client_fileno_list:
                self.shutdown_client(client_fileno)
        
    def shutdown_client(self, client_fileno:int):
        client_socket = self.__client_by_fileno.get(client_fileno)
        if client_socket:
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except ConnectionResetError:
                if self.__is_debug_mode.value:
                    self.__recv_queue.put_nowait({
                        "type" : "debug",
                        "message" : f"[{client_fileno}] shutdown_client ConnectionResetError.\n{traceback.format_exc()}"
                    })
            
            except BrokenPipeError:
                if self.__is_debug_mode.value:
                    self.__recv_queue.put_nowait({
                        "type" : "debug",
                        "message" : f"[{client_fileno}] shutdown_client BrokenPipeError.\n{traceback.format_exc()}"
                    })
            
            except OSError as e:
                if e.errno == errno.ENOTCONN: # errno 107
                    pass
                else:
                    raise e
        
    def __close_client(self, client_fileno:int):
        client_socket = self.__client_by_fileno.get(client_fileno)
        if client_socket:
            client_socket.close()
        
        self.__recv_queue.put_nowait({
            "type" : "close_client",
            "fileno" : client_fileno
        })
    
            
    def __remove_client(self, client_fileno:int):
        try: _ = self.__send_lock_by_fileno.pop(client_fileno)
        except KeyError: pass
        try: _ = self.__recv_lock_by_fileno.pop(client_fileno)
        except KeyError: pass
        try: _ = self.__client_by_fileno.pop(client_fileno)
        except KeyError: pass
        
        len_remain_send_buffer = 0
        try: 
            remain_send_buffer = self.__send_buffer_by_fileno.pop(client_fileno)
            len_remain_send_buffer = len(remain_send_buffer)
        except KeyError: pass
        
        try:
            listener_fileno = self.__listener_fileno_by_client_fileno.pop(client_fileno)
            _ = self.__client_fileno_dict_by_listener_fileno[listener_fileno].pop(client_fileno)
        except KeyError: pass
        
        if 0 < len_remain_send_buffer:
            if self.__is_debug_mode.value:
                self.__recv_queue.put_nowait({
                    "type" : "debug",
                    "message" : f"[{client_fileno}] Removed. But send buffer remain:{len_remain_send_buffer} bytes."
                })
        
    
    def __epoll_accept(self, listener_fileno:int):
        listener = self.__listener_by_fileno.get(listener_fileno)
        if listener:
            try:
                client_socket, address = listener.accept()
                client_socket_fileno = client_socket.fileno()
                client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                client_socket.setblocking(False)
                
                self.__client_by_fileno.update({client_socket_fileno : client_socket})
                self.__send_lock_by_fileno.update({client_socket_fileno : threading.Lock()})
                self.__recv_lock_by_fileno.update({client_socket_fileno : threading.Lock()})
                self.__send_buffer_lock_by_fileno.update({client_socket_fileno : threading.Lock()})
                self.__send_buffer_by_fileno.update({client_socket_fileno : b''})
                if not listener_fileno in self.__client_fileno_dict_by_listener_fileno:
                    self.__client_fileno_dict_by_listener_fileno.update({listener_fileno : {}})
                self.__client_fileno_dict_by_listener_fileno[listener_fileno][client_socket_fileno] = True
                self.__listener_fileno_by_client_fileno.update({client_socket_fileno : listener_fileno})
                
                self.__registered_eventmask_by_fileno[client_socket_fileno] = self.__recv_eventmask
                self.__epoll.register(client_socket, self.__recv_eventmask)
                
                self.__recv_queue.put_nowait({
                    "type" : "accept",
                    "fileno" : client_socket_fileno
                })
        
            except BlockingIOError as e:
                if e.errno == socket.EAGAIN:
                    pass
                else:
                    raise e
    
    def __epoll_recv(self, client_fileno:int):
        is_connect = True
        recv_lock = self.__recv_lock_by_fileno.get(client_fileno)
        if recv_lock:
            with recv_lock:
                recv_bytes = b''
                client_socket = self.__client_by_fileno.get(client_fileno)
                if client_socket:
                    is_eagain = False
                    try:
                        temp_recv_bytes = client_socket.recv(self.__buffer_size)
                        if temp_recv_bytes == None or temp_recv_bytes == -1 or temp_recv_bytes == b'':
                            if self.__is_debug_mode.value:
                                self.__recv_queue.put_nowait({
                                    "type" : "debug",
                                    "message" : f"[{client_fileno}] recv break :'{temp_recv_bytes}'"
                                })
                        
                            is_connect = False
                        else:
                            recv_bytes += temp_recv_bytes
                            
                    except ConnectionError as e:
                        pass
                    
                    except OSError as e:
                        if e.errno == socket.EAGAIN:
                            is_eagain = True
                        elif e.errno == errno.EBADF:
                            is_connect = False
                        else:
                            raise e

                    if not is_eagain and is_connect:
                        try:
                            self.__epoll.modify(client_fileno, self.__registered_eventmask_by_fileno[client_fileno])
                        except FileNotFoundError:
                            pass
                        except OSError as e:
                            if e.errno == errno.EBADF:
                                pass

                    if recv_bytes:
                        self.__recv_queue.put_nowait({
                            "type" : "recv",
                            "fileno" : client_fileno,
                            "bytes" : recv_bytes
                        })
                        
        return is_connect
    
    def __epoll_send(self, client_fileno:int):
        try:
            is_connect = True
            client_socket = self.__client_by_fileno.get(client_fileno)
            send_lock = self.__send_lock_by_fileno.get(client_fileno)
            send_buffer_lock = self.__send_buffer_lock_by_fileno.get(client_fileno)
            
            if send_lock is not None and client_socket is not None:
                with send_lock:
                    send_buffer = b''
                    if send_buffer_lock:
                        with send_buffer_lock:
                            send_buffer = self.__send_buffer_by_fileno.get(client_fileno)
                    else:
                        is_connect = False
        
                    if send_buffer is not None:
                        if send_buffer != b'':
                            try:
                                sent_length = client_socket.send(send_buffer)
                                if 0 < sent_length:
                                    if send_buffer_lock:
                                        with send_buffer_lock:
                                            self.__send_buffer_by_fileno.update({client_fileno : send_buffer[sent_length:]})
                        
                            except ConnectionError as e:
                                pass
                            
                            except BlockingIOError as e:
                                if e.errno == socket.EAGAIN:
                                    pass
                                else:
                                    raise e
                                
                            except OSError as e:
                                if e.errno == errno.EBADF:
                                    is_connect = False
                                    
                                else:
                                    raise e
                    else:
                        is_connect = False

            else:
                is_connect = False
        
                
            try:
                send_buffer_lock = self.__send_buffer_lock_by_fileno.get(client_fileno)
                if send_buffer_lock:
                    remain_buffer = b''
                    with send_buffer_lock:
                        remain_buffer = self.__send_buffer_by_fileno.get(client_fileno)
                    registered_eventmask = self.__registered_eventmask_by_fileno.get(client_fileno)
                    
                    if (remain_buffer is not None and registered_eventmask is not None) and\
                        remain_buffer == b'' and\
                        registered_eventmask != self.__recv_eventmask:
                        self.__registered_eventmask_by_fileno.update({client_fileno : self.__recv_eventmask})
                        self.__epoll.modify(client_fileno, self.__recv_eventmask)
            except OSError as e:
                if e.errno == errno.EBADF:
                    is_connect = False
                        
        except Exception as e:
            if self.__is_debug_mode.value:
                self.__recv_queue.put_nowait({
                    "type" : "debug",
                    "message" : f"[{client_fileno:3}] __epoll_send Exception:{e}\n{traceback.format_exc()}"
                })
            is_connect = False
            
        return is_connect
                
    def __epoll_thread_function(self):
        __is_running = True
        tid = threading.get_ident()
        try:
            while __is_running:
                events = self.__epoll.poll()
                for detect_fileno, detect_event in events:
                    if detect_event & select.EPOLLPRI:
                        pass
                    if detect_fileno == self.__close_event_listener.fileno():
                        self.__close_event_listener.send(tid.to_bytes(32, 'big'))
                        __is_running = False
                        
                    elif detect_fileno in self.__listener_by_fileno:
                        if detect_event & (select.EPOLLHUP | select.EPOLLRDHUP):
                            self.__shutdown_clients_by_listener(detect_fileno)
                            if self.__unregister(detect_fileno):
                                self.__close_listener(detect_fileno)
                                self.__remove_listener(detect_fileno)
                            
                        elif detect_event & select.EPOLLIN:
                            self.__epoll_accept(detect_fileno)
                        
                        else:
                            if self.__is_debug_mode.value:
                                self.__recv_queue.put_nowait({
                                    "type" : "debug",
                                    "message" : f"listener got unknown event : {detect_event:#06x}"
                                })
                        
                    elif detect_fileno in self.__client_by_fileno:
                        unregistered = False
                        if detect_event & select.EPOLLOUT:
                            if self.__epoll_send(detect_fileno) == False:
                                if self.__is_debug_mode.value:
                                    self.__recv_queue.put_nowait({
                                        "type" : "debug",
                                        "message" : f"self.__epoll_send False"
                                    })
                            
                                if self.__unregister(detect_fileno):
                                    unregistered = True
                                    self.__close_client(detect_fileno)
                                    self.__remove_client(detect_fileno)
                        
                        if detect_event & select.EPOLLIN:
                            if self.__epoll_recv(detect_fileno) == False:
                                if self.__is_debug_mode.value:
                                    self.__recv_queue.put_nowait({
                                        "type" : "debug",
                                        "message" : f"self.__epoll_recv False"
                                    })
                            
                                if self.__unregister(detect_fileno):
                                    unregistered = True
                                    self.__close_client(detect_fileno)
                                    self.__remove_client(detect_fileno)
                        
                        if detect_event & (select.EPOLLHUP | select.EPOLLRDHUP):
                            if self.__is_debug_mode.value:
                                self.__recv_queue.put_nowait({
                                    "type" : "debug",
                                    "message" : f"[{detect_fileno}] detect_event & (select.EPOLLHUP | select.EPOLLRDHUP)"
                                })
                        
                            if not unregistered:
                                if self.__unregister(detect_fileno):
                                    self.__close_client(detect_fileno)
                                    self.__remove_client(detect_fileno)
                            
                    else:
                        if self.__is_debug_mode.value:
                            self.__recv_queue.put_nowait({
                                "type" : "debug",
                                "message" : f"[{detect_fileno:3}] Unknown Fileno. {detect_event:#06x}, exist:{detect_fileno in self.__client_by_fileno}"
                            })
                    
        except Exception as e:
            if self.__is_debug_mode.value:
                self.__recv_queue.put_nowait({
                    "type" : "debug",
                    "message" : f"Exception in epoll_thread_function.\n{e}.\n{traceback.format_exc()}"
                })