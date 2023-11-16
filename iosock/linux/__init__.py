import socket
import select
import platform
import multiprocessing
import ctypes
import threading
import collections
from .. import abstract

class Client(abstract.ClientBase):
    def __init__(self) -> None:
        print("linux client")
        
class Server(abstract.ServerBase):
    def __init__(self) -> None:
        print("linux server")
        self.__buffer_size = 1024
        self.__is_running = multiprocessing.Value(ctypes.c_bool, False)
        self.client_by_fileno = collections.defaultdict(dict)
        
    def create_client(self, client_socket):
        return {
            "socket" : client_socket,
            "lock" : threading.Lock()
        }
        
    def listen(self, listen_ip:str, listen_port:int, is_blocking:bool = False, backlog:int = 5):
        self.__listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.__listen_socket.setblocking(is_blocking)
        self.__listen_socket.bind((listen_ip, listen_port))
        self.__listen_socket.listen(backlog)

    def start(self, listen_ip:str, listen_port:int, is_blocking:bool = False, backlog:int = 5):
        system = platform.system()
        if system == "Linux":
            self.listen(listen_ip, listen_port, is_blocking, backlog)

            self.__epoll = select.epoll()
            
            self.__is_running.value = True
            self.__epoll_thread = threading.Thread(target=self.__epoll_thread_function)
            self.__epoll_thread.start()
            
            self.__stop_epoll_sender, self.__stop_epoll_listener = socket.socketpair()
            
            closer_eventmask = select.EPOLLHUP | select.EPOLLRDHUP
            self.__epoll.register(self.__stop_epoll_listener, closer_eventmask)
            
            listener_eventmask = select.EPOLLIN | select.EPOLLPRI | select.EPOLLOUT | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
            self.__epoll.register(self.__listen_socket, listener_eventmask)
            
        elif system == "Darwin":
            print("MacOS")
            pass
        elif system == "Windows":
            print("Windows")
            pass
            
    def join(self):
        self.__epoll_thread.join()
    
    def stop(self):
        self.__is_running.value = False
        self.__stop_epoll_sender.shutdown(socket.SHUT_RDWR)
    
    def __epoll_thread_function(self):
        while self.__is_running.value:
            events = self.__epoll.poll()
            for detect_fileno, detect_event in events:
                if detect_fileno == self.__listen_socket.fileno():
                    if detect_event & (select.EPOLLHUP | select.EPOLLOUT):
                        self.__is_running.value = False
                        self.__epoll.unregister(self.__listen_socket)
                        
                    elif detect_event & (select.EPOLLIN | select.EPOLLPRI):
                        client_socket, address = self.__listen_socket.accept()
                        client_socket.setblocking(False)
                        self.client_by_fileno.update({client_socket.fileno(), self.create_client(client_socket)})
                        
                        client_eventmask = select.EPOLLIN | select.EPOLLPRI | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
                        self.__epoll.register(client_socket, client_eventmask)
                    
                    else:
                        print("accept", detect_fileno, detect_event)
                                                            
                elif detect_fileno ==  self.__stop_epoll_listener.fileno():
                    self.__epoll.unregister(self.__listen_socket)
                    self.__listen_socket.shutdown(socket.SHUT_RDWR)
                    print("close", "close_listener", detect_event & (select.EPOLLIN | select.EPOLLPRI), detect_event & (select.EPOLLHUP | select.EPOLLRDHUP))
                
                else:
                    client = self.client_by_fileno.get(detect_fileno)
                    client_socket:socket.socket = client['socket']
                    
                    if detect_event & (select.EPOLLHUP | select.EPOLLRDHUP):
                        print("close", client_socket)
                        
                    elif detect_event & (select.EPOLLIN | select.EPOLLPRI):
                        recv_bytes = client_socket.recv(self.__buffer_size)
                        print("r", type(recv_bytes), recv_bytes)
                        
                    else:
                        print("r", detect_fileno, detect_event)
                    
        self.__epoll.close()