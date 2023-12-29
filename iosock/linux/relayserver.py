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

class RelayServer():
    def __init__(self) -> None:
        self.__buffer_size = 8196
        self.__is_running = multiprocessing.Value(ctypes.c_bool, False)
        self.__message_queue = None
        
        self.__running_threads = []
        self.__running_thread_by_tid = collections.defaultdict(threading.Thread)
        
        self.__listener_by_ip_port = collections.defaultdict(socket.socket)
        self.__listener_by_fileno = collections.defaultdict(socket.socket)
        
        self.__socket_by_fileno = collections.defaultdict(socket.socket)
        
        self.__client_by_fileno = collections.defaultdict(socket.socket)
        self.__listener_fileno_by_client_fileno = collections.defaultdict(int)
        self.__client_fileno_dict_by_listener_fileno = collections.defaultdict(dict)
        self.__registered_eventmask_by_fileno = collections.defaultdict(int)
        
        self.__send_lock_by_fileno = collections.defaultdict(threading.Lock)
        self.__recv_lock_by_fileno = collections.defaultdict(threading.Lock)
        
        self.__send_buffer_by_fileno = collections.defaultdict(bytes)
        self.__send_buffer_lock_by_fileno = collections.defaultdict(threading.Lock)
        
        self.__relay_addr_by_listener_addr = collections.defaultdict(str)
        self.__relay_by_fileno = collections.defaultdict(socket.socket)
        self.__client_by_relay_fileno = collections.defaultdict(socket.socket)
        self.__relay_by_client_fileno = collections.defaultdict(socket.socket)
        
        self.__epoll : select.epoll = None
        
        self.__listener_eventmask = select.EPOLLIN | select.EPOLLPRI | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
        self.__recv_eventmask = select.EPOLLIN  | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
        self.__send_recv_eventmask = select.EPOLLIN | select.EPOLLOUT | select.EPOLLHUP | select.EPOLLRDHUP
        self.__closer_eventmask = select.EPOLLIN | select.EPOLLPRI | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
        
    
    def __listen(self, ip:str, port:int, backlog:int = 5):
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

    def start(self, count_threads:int=1, message_queue:queue.Queue = None ):
        self.__is_running.value = True
        self.__message_queue = message_queue
        
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
                # After 'listen()'
                self.__epoll.register(fileno, self.__listener_eventmask)
                self.__registered_eventmask_by_fileno.update({fileno : self.__listener_eventmask})

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
                if self.__message_queue:
                    self.__message_queue.put_nowait({
                        "type" : "debug",
                        "message" : f"[{socket_fileno}] send KeyError.\n{traceback.format_exc()}"
                    })
                
            except FileNotFoundError:
                if self.__message_queue:
                    self.__message_queue.put_nowait({
                        "type" : "debug",
                        "message" : f"[{socket_fileno}] send FileNotFoundError.\n{traceback.format_exc()}"
                    })
                    
            except PermissionError:
                if self.__message_queue:
                    self.__message_queue.put_nowait({
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
        if self.__message_queue:
            self.__message_queue.put_nowait(None)
        self.__shutdown_listeners()
        
        for _ in self.__running_threads:
            self.__close_event.send(b'close')
            tid_bytes = self.__close_event.recv(32)
            tid = int.from_bytes(tid_bytes, byteorder='big')
            self.__running_thread_by_tid[tid].join()
            
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
            if self.__message_queue:
                self.__message_queue.put_nowait({
                    "type" : "debug",
                    "message" : f"[{listener_fileno:3}] Listner Close()"
                })
        
    def __remove_listener(self, listener_fileno:int):
        try:
            _ = self.__listener_by_fileno.pop(listener_fileno)
        except KeyError:
            pass
    
    def __unregister(self, socket_fileno:int) -> bool:
        result = False
        try:
            _ = self.__registered_eventmask_by_fileno.pop(socket_fileno)
            self.__epoll.unregister(socket_fileno)
            result = True
            if self.__message_queue:
                self.__message_queue.put_nowait({
                    "type" : "debug",
                    "message" : f"[{socket_fileno:3}] epoll unregister."
                })

        except KeyError:
            if self.__message_queue:
                self.__message_queue.put_nowait({
                    "type" : "debug",
                    "message" : f"[{socket_fileno}] __unregister KeyError.\n{traceback.format_exc()}"
                })
            
        except FileNotFoundError:
            if self.__message_queue:
                self.__message_queue.put_nowait({
                    "type" : "debug",
                    "message" : f"[{socket_fileno}] __unregister FileNotFoundError.\n{traceback.format_exc()}"
                })
            
        except OSError as e:
            if e.errno == errno.EBADF:
                pass
            else:
                raise e
            
        except Exception as e:
            if self.__message_queue:
                self.__message_queue.put_nowait({
                    "type" : "debug",
                    "message" : f"[{socket_fileno}] send Exception: {e}.\n{traceback.format_exc()}"
                })
        
        return result
        
    def __shutdown_clients_by_listener(self, listener_fileno:int):
        client_fileno_dict = self.__client_fileno_dict_by_listener_fileno.get(listener_fileno)
        if client_fileno_dict:
            for client_fileno in client_fileno_dict:
                self.__shutdown_client(client_fileno)
        
    def __shutdown_client(self, socket_fileno:int):
        _socket = self.__socket_by_fileno.get(socket_fileno)
        if _socket:
            try:
                _socket.shutdown(socket.SHUT_RDWR)
            except ConnectionError:
                pass
            except OSError as e:
                if e.errno == errno.ENOTCONN: # errno 107
                    pass
                else:
                    raise e
            except Exception as e:
                if self.__message_queue:
                    self.__message_queue.put_nowait({
                        "type" : "debug",
                        "message" : f"[{socket_fileno}] send Exception: {e}.\n{traceback.format_exc()}"
                    })
            
    
    def __close_client(self, client_fileno:int):
        client_socket = self.__client_by_fileno.get(client_fileno)
        if client_socket:
            client_socket.close()
            if self.__message_queue:
                self.__message_queue.put_nowait({
                    "type" : "debug",
                    "message" : f"[{client_fileno:3}] Client Closed."
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
            if self.__message_queue:
                self.__message_queue.put_nowait({
                    "type" : "debug",
                    "message" : f"[{client_fileno}] Try Close. send buffer remain:{len_remain_send_buffer} bytes."
                })
        
    def __epoll_accept(self, listener_fileno:int):
        listener = self.__listener_by_fileno.get(listener_fileno)
        if listener:
            client_socket = None
            try:
                client_socket, address = listener.accept()
                if self.__message_queue:
                    self.__message_queue.put_nowait({
                        "type" : "debug",
                        "message" : f"accept {client_socket.fileno():3}:{address}"
                    })
                
                client_socket_fileno = client_socket.fileno()
                client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                client_socket.setblocking(False)
                
                self.__socket_by_fileno.update({client_socket_fileno : client_socket})
                self.__client_by_fileno.update({client_socket_fileno : client_socket})
                self.__send_lock_by_fileno.update({client_socket_fileno : threading.Lock()})
                self.__recv_lock_by_fileno.update({client_socket_fileno : threading.Lock()})
                self.__send_buffer_lock_by_fileno.update({client_socket_fileno : threading.Lock()})
                self.__send_buffer_by_fileno.update({client_socket_fileno : b''})
                if not listener_fileno in self.__client_fileno_dict_by_listener_fileno:
                    self.__client_fileno_dict_by_listener_fileno.update({listener_fileno : {}})
                self.__client_fileno_dict_by_listener_fileno[listener_fileno][client_socket_fileno] = True
                self.__listener_fileno_by_client_fileno.update({client_socket_fileno : listener_fileno})
                
                self.__registered_eventmask_by_fileno.update({client_socket_fileno : self.__recv_eventmask})
                self.__epoll.register(client_socket, self.__recv_eventmask)
            except BlockingIOError as e:
                if e.errno == socket.EAGAIN:
                    return
                else:
                    raise e
            
            if client_socket:
                sockname = listener.getsockname()
                listener_addr = sockname[0]
                listener_port = sockname[1]
                
                disconnect_client = False
                
                relay_addr = self.__relay_addr_by_listener_addr.get(f"{listener_addr}:{listener_port}")
                if relay_addr:
                    relay_addrs = relay_addr.split(':')
                    relay_ip = relay_addrs[0]
                    relay_port = int(relay_addrs[1])
                    try:
                        relay_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        relay_socket.connect((relay_ip, relay_port))
                        relay_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        relay_socket.setblocking(False)
                        relay_socket_fileno = relay_socket.fileno()
                        if self.__message_queue:
                            self.__message_queue.put_nowait({
                                "type" : "debug",
                                "message" : f"Client fileno:{client_socket.fileno()} -> Relay fileno:{relay_socket_fileno}"
                            })
                            
                        self.__registered_eventmask_by_fileno.update({relay_socket_fileno : self.__recv_eventmask})
                        self.__epoll.register(relay_socket_fileno, self.__recv_eventmask)
                        
                        self.__socket_by_fileno.update({relay_socket_fileno : relay_socket})
                        self.__relay_by_fileno.update({relay_socket_fileno : relay_socket})
                        self.__client_by_relay_fileno.update({relay_socket_fileno : client_socket})
                        self.__relay_by_client_fileno.update({client_socket_fileno : relay_socket})
                        
                        self.__send_lock_by_fileno.update({relay_socket_fileno : threading.Lock()})
                        self.__recv_lock_by_fileno.update({relay_socket_fileno : threading.Lock()})
                        self.__send_buffer_lock_by_fileno.update({relay_socket_fileno : threading.Lock()})
                        self.__send_buffer_by_fileno.update({relay_socket_fileno : b''})
                    except socket.error as e:
                        disconnect_client = True
                else:
                    disconnect_client = True
                
                if disconnect_client:
                    client_socket.send(b'NOTSERVER')
                    self.__shutdown_client(client_socket.fileno())
                    
    def relay(self, fromip:str, fromport:int, toip:str, toport:int, check_relay_function=None):
        self.__listen(fromip, fromport)
        self.check_relay_function = check_relay_function
        self.__relay_addr_by_listener_addr.update({f"{fromip}:{fromport}" : f"{toip}:{toport}"})
    
    def __epoll_recv(self, recv_socket:socket.socket) -> bytes:
        recv_bytes = b''
        recv_socket_fileno = recv_socket.fileno()
        recv_lock = self.__recv_lock_by_fileno.get(recv_socket_fileno)
        if recv_lock:
            with recv_lock:
                if recv_socket:
                    is_connect = True
                    is_eagain = False
                    try:
                        temp_recv_bytes = recv_socket.recv(self.__buffer_size)
                        if temp_recv_bytes == None or temp_recv_bytes == -1 or temp_recv_bytes == b'':
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
                            self.__epoll.modify(recv_socket_fileno, self.__registered_eventmask_by_fileno[recv_socket_fileno])
                        except FileNotFoundError:
                            pass
                        except OSError as e:
                            if e.errno == errno.EBADF:
                                pass

        return recv_bytes
    
    def __epoll_send(self, client_fileno:int):
        is_connect = True
        try:
            client_socket = self.__socket_by_fileno.get(client_fileno)
            send_lock = self.__send_lock_by_fileno.get(client_fileno)
            send_buffer_lock = self.__send_buffer_lock_by_fileno.get(client_fileno)
            
            if send_lock is not None and client_socket is not None:
                with send_lock:
                    send_buffer = b''
                    if send_buffer_lock:
                        with send_buffer_lock:
                            send_buffer = self.__send_buffer_by_fileno.get(client_fileno)
                    else:
                        if self.__message_queue:
                            self.__message_queue.put_nowait({
                                "type" : "debug",
                                "message" : f"[{client_fileno}] is_connect = False send_buffer_lock is {send_buffer_lock}"
                            })
                    
                    if send_buffer is not None:
                        if send_buffer != b'':
                            try:
                                sent_length = client_socket.send(send_buffer)
                                if 0 < sent_length:
                                    if send_buffer_lock:
                                        with send_buffer_lock:
                                            self.__send_buffer_by_fileno.update({client_fileno : send_buffer[sent_length:]})
                                    else:
                                        if self.__message_queue:
                                            self.__message_queue.put_nowait({
                                                "type" : "debug",
                                                "message" : f"[{client_fileno}] is_connect = False send_buffer_lock is {send_buffer_lock}"
                                            })
                        
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
                        if self.__message_queue:
                            self.__message_queue.put_nowait({
                                "type" : "debug",
                                "message" : f"[{client_fileno}] is_connect = False send_buffer is {send_buffer}"
                            })
                        
                        is_connect = False

            else:
                if self.__message_queue:
                    self.__message_queue.put_nowait({
                        "type" : "debug",
                        "message" : f"[{client_fileno}] is_connect = False send_lock is {send_lock} and client_socket is {client_socket}"
                    })
                is_connect = False
        
            try:
                remain_buffer = b''
                send_buffer_lock = self.__send_buffer_lock_by_fileno.get(client_fileno)
                if send_buffer_lock:
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
            if self.__message_queue:
                self.__message_queue.put_nowait({
                    "type" : "debug",
                    "message" : f"[{client_fileno}] __epoll_send Excetion:{e}.\n{traceback.format_exc()}"
                })
            
            is_connect = False
            
        return is_connect
    
    def __epoll_thread_function(self):
        __is_running = True
        tid = threading.get_ident()
        if self.__message_queue:
            self.__message_queue.put_nowait({
                "type" : "debug",
                "message" : f"Start Epoll Work"
            })
        
        try:
            while __is_running:
                events = self.__epoll.poll()
                for detect_fileno, detect_event in events:
                    if detect_event & select.EPOLLPRI:
                        if self.__message_queue:
                            self.__message_queue.put_nowait({
                                "type" : "debug",
                                "message" : f"EPOLLPRI [{detect_event:#06x} & select.EPOLLPRI]"
                            })
                        
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
                            if self.__message_queue:
                                self.__message_queue.put_nowait({
                                    "type" : "debug",
                                    "message" : f"listen event else [{detect_event:#06x}]..?"
                                })
                            
                    else:
                        if detect_event & select.EPOLLOUT:
                            if self.__epoll_send(detect_fileno) == False:
                                if self.__unregister(detect_fileno):
                                    self.__close_client(detect_fileno)
                                    self.__remove_client(detect_fileno)
                        
                        if detect_event & select.EPOLLIN:
                            from_socket:socket.socket = None
                            to_socket:socket.socket = None
                            if detect_fileno in self.__client_by_fileno:
                                from_socket = self.__client_by_fileno.get(detect_fileno)
                                to_socket = self.__relay_by_client_fileno.get(detect_fileno)
                                
                            elif detect_fileno in self.__relay_by_fileno:
                                from_socket = self.__relay_by_fileno.get(detect_fileno)
                                to_socket = self.__client_by_relay_fileno.get(detect_fileno)
                            
                            if from_socket and to_socket:
                                recv_bytes = self.__epoll_recv(from_socket)
                                if recv_bytes:
                                    if self.check_relay_function:
                                        if self.check_relay_function(from_socket.fileno(), to_socket.fileno(), recv_bytes):
                                            self.send(to_socket.fileno(), recv_bytes)
                                    else:
                                        self.send(to_socket.fileno(), recv_bytes)
                                    
                        if detect_event & (select.EPOLLHUP | select.EPOLLRDHUP):
                            to_socket_fileno:int = None
                            if detect_fileno in self.__client_by_fileno:
                                to_socket = self.__relay_by_client_fileno.get(detect_fileno)
                                if to_socket:
                                    to_socket_fileno = to_socket.fileno()
                                    
                            elif detect_fileno in self.__relay_by_fileno:
                                to_socket = self.__client_by_relay_fileno.get(detect_fileno)
                                if to_socket:
                                    to_socket_fileno = to_socket.fileno()
                                    
                            if to_socket_fileno:
                                self.__shutdown_client(to_socket_fileno)
                            
                            if self.__unregister(detect_fileno):
                                self.__close_client(detect_fileno)
                                self.__remove_client(detect_fileno)
                            
                        elif not detect_event & (select.EPOLLIN | select.EPOLLOUT):
                            if self.__message_queue:
                                self.__message_queue.put_nowait({
                                    "type" : "debug",
                                    "message" : f"Unknown Event. {detect_event:#06x}, exist:{detect_fileno in self.__client_by_fileno}"
                                })
                            
        except Exception as e:
            if self.__message_queue:
                self.__message_queue.put_nowait({
                    "type" : "debug",
                    "message" : f"{e}, {traceback.format_exc()}"
                })
            
        if self.__message_queue:
            self.__message_queue.put_nowait({
                "type" : "debug",
                "message" : f"Finish Epoll Work"
            })
            