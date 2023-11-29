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
        


        

