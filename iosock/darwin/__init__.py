import socket
from .. import abstract

class Client(abstract.ClientBase):
    def __init__(self) -> None:
        print("darwin client")
        self.__buffer_size = 1024
        
    def connect(self, ip:str, port:int):
        self.__client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__client_socket.connect((ip, port))
        
        # sre = self.__receive_socket.recv(BUFFER_SIZE)
        # print("sre", sre)
        # if sre:
        #     self.__receive_socket.setblocking(False)
        # else:
        #     self.__receive_socket.close()
        #     self.__receive_socket = None
        
        # rre = self.__send_socket.recv(BUFFER_SIZE)
        # print("rre", rre)
        # if rre:
        #     self.__send_socket.setblocking(False)
        # else:
        #     self.__send_socket.close()
        #     self.__send_socket = None
        
    def join(self):
        self.__client_socket.recv(self.__buffer_size)
            
    def close(self):
        if self.__client_socket:
            self.__client_socket.shutdown(socket.SHUT_RDWR)
            self.__client_socket.close()
            
    def send(self, data:bytes):
        self.__client_socket.send(data)
        
class Server:
    def __init__(self) -> None:
        print("darwin server")
        