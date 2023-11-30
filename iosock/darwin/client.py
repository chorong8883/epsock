import socket
import errno

class DarwinClient:
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
        self.__client_socket = None
            
    def sendall(self, data:bytes):
        self.__client_socket.sendall(data)
    
    def send(self, data:bytes) -> int:
        return self.__client_socket.send(data)
        
    def recv(self, buffer_size:int = None) -> bytes:
        if buffer_size:
            return self.__client_socket.recv(buffer_size)
        else:
            return self.__client_socket.recv(self.__buffer_size)
        
    def fileno(self) -> int:
        return self.__client_socket.fileno()
    
    def setblocking(self, is_block:bool):
        self.__client_socket.setblocking(is_block)
