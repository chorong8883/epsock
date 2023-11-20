import socket
import traceback
import queue
from .. import abstract

class Client(abstract.ClientBase):
    def __init__(self) -> None:
        print("darwin client")
        self.__buffer_size = 10240
        
    def connect(self, ip:str, port:int):
        self.__client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__client_socket.connect((ip, port))
        self.__client_socket.setblocking(False)
        
    def close(self):
        if self.__client_socket:
            self.__client_socket.shutdown(socket.SHUT_RDWR)
            self.__client_socket.close()
            
    def send(self, data:bytes):
        return self.__client_socket.send(data)
        
    def recv(self):
        return self.__client_socket.recv(self.__buffer_size)
        
class Server:
    def __init__(self) -> None:
        print("darwin server")
        self.__recv_queue = queue.Queue()
    
    def start(self, listen_ip:str, listen_port:int, is_blocking:bool = False, backlog:int = 5):
        pass
    
    def stop(self):
        pass
    
    def join(self):
        pass
        
    def recv(self):
        return self.__recv_queue.get()
    
    def send(self, fileno:int, data:bytes):
        pass