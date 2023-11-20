import socket
import select
import platform
import multiprocessing
import ctypes
import threading
import collections
import queue
import errno

from multiprocessing.pool import ThreadPool

from .. import abstract

class Client(abstract.ClientBase):
    def __init__(self) -> None:
        print("linux client")
        
class Server(abstract.ServerBase):
    def __init__(self) -> None:
        print("linux server")
        self.__buffer_size = 10240
        self.__is_running = multiprocessing.Value(ctypes.c_bool, False)
        self.client_by_fileno = collections.defaultdict(dict)
        self.__recv_queue = queue.Queue()
        
        self.__detect_epollin_fileno_queue = queue.Queue()
        self.__send_fileno_queue = queue.Queue()
        
    def create_client(self, client_socket) -> dict:
        return {
            "socket" : client_socket,
            "lock" : threading.Lock(),
            "send_buffer_queue" : queue.Queue(),
            "sending_buffer" : b''
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
            self.__recv_work_thread = threading.Thread(target=self.__recv_work)
            self.__send_work_thread = threading.Thread(target=self.__send_work)
            
            self.__epoll_thread.start()
            self.__recv_work_thread.start()
            self.__send_work_thread.start()
            
            listener_eventmask = select.EPOLLIN | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
            self.__epoll.register(self.__listen_socket, listener_eventmask)
            
        elif system == "Darwin":
            print("MacOS")
            pass
        elif system == "Windows":
            print("Windows")
            pass
            
    def join(self):
        self.__epoll_thread.join()
        self.__recv_work_thread.join()
        self.__send_work_thread.join()
    
    def stop(self):
        self.__is_running.value = False
        self.__listen_socket.shutdown(socket.SHUT_RDWR)
        
    def close_client(self, client_fileno:int):
        client_data = self.client_by_fileno.pop(client_fileno)
        lock:threading.Lock = client_data["lock"]
        lock.acquire()
        client_socket:socket.socket = client_data["socket"]
        try:
            client_socket.setblocking(True)
            client_socket.settimeout(3)
            
            try:
                while True:
                    if client_data['sending_buffer'] == b'':
                        client_data['sending_buffer'] = client_data['send_buffer_queue'].get_nowait()
                    
                    start_index = 0
                    end_index = len(client_data['sending_buffer'])
                    while start_index < end_index:
                        send_length = client_socket.send(client_data['sending_buffer'][start_index:end_index])
                        if send_length <= 0:
                            break
                        start_index += send_length
                    client_data['sending_buffer'] = b''
            except queue.Empty:
                pass
            
            client_socket.shutdown(socket.SHUT_RDWR)
        except OSError as e:
            if e.errno == errno.ENOTCONN: # errno 107
                pass
            else:
                raise e
        client_socket.close()
        
        lock.release()

#####################################################################################################################
#####################################################################################################################
#####################################################################################################################
    
    def recv(self):
        return self.__recv_queue.get()
    
    def __recv_work(self):
        while self.__is_running.value:
            detect_fileno = self.__detect_epollin_fileno_queue.get()
            if not detect_fileno:
                self.__recv_queue.put_nowait(None)
                break
            
            client = self.client_by_fileno.get(detect_fileno)
            client_lock:threading.Lock = client['lock']
            client_lock.acquire()
            
            client_socket:socket.socket = client['socket']
            
            result = b''
            try:
                while True:
                    recv_bytes = client_socket.recv(self.__buffer_size)
                    if recv_bytes:
                        result += recv_bytes
                    
            except BlockingIOError as e:
                if e.errno == socket.EAGAIN:
                    pass
                else:
                    raise e
            
            client_lock.release()
            
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
            client_lock:threading.Lock = client_data['lock']
            client_lock.acquire()
            
            sending_data = b''
            if client_data['sending_buffer'] == b'':
                try:
                    client_data['sending_buffer'] = client_data['send_buffer_queue'].get_nowait()
                    sending_data = client_data['sending_buffer']
                except queue.Empty:
                    return
            else:
                sending_data = client_data['sending_buffer']
            
            start_index = 0
            end_index = len(sending_data)
            try:
                while start_index < end_index:
                    send_length = client_data['socket'].send(sending_data[start_index:end_index])
                    if send_length <= 0:
                        break
                    start_index += send_length
            except BlockingIOError as e:
                if e.errno == socket.EAGAIN:
                    pass
                else:
                    raise e
                
            if start_index < end_index:
                self.client_by_fileno[send_fileno]['sending_buffer'] = self.client_by_fileno[send_fileno]['sending_buffer'][start_index:end_index]
                self.__send_fileno_queue.put_nowait(send_fileno)
            else:
                self.client_by_fileno[send_fileno]['sending_buffer'] = b''
                if not client_data['send_buffer_queue'].empty():
                    self.__send_fileno_queue.put_nowait(send_fileno)
                    
            client_lock.release()
            
#####################################################################################################################
#####################################################################################################################
#####################################################################################################################            
            
    def __epoll_thread_function(self):
        while self.__is_running.value:
            events = self.__epoll.poll()
            for detect_fileno, detect_event in events:
                if detect_fileno == self.__listen_socket.fileno():
                    if detect_event & (select.EPOLLHUP | select.EPOLLRDHUP):
                        self.__is_running.value = False
                        
                    elif detect_event & select.EPOLLIN:
                        client_socket, address = self.__listen_socket.accept()
                        print(f"accept {client_socket.fileno()} {address}")
                        client_socket.setblocking(False)
                        client_socket_fileno = client_socket.fileno()
                        exist_client = self.client_by_fileno.get(client_socket_fileno)
                        if exist_client:
                            exist_socket:socket.socket = exist_client["socket"]
                            try:
                                exist_socket.shutdown(socket.SHUT_RDWR)
                            except OSError as e:
                                if e.errno == errno.ENOTCONN: # errno 107
                                    pass
                                else:
                                    raise e
                        client_data = self.create_client(client_socket)
                        self.client_by_fileno.update({client_socket_fileno : client_data})
                        
                        client_eventmask = select.EPOLLIN | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
                        self.__epoll.register(client_socket, client_eventmask)
                    
                    else:
                        print("accept", detect_fileno, detect_event)

                elif detect_event & (select.EPOLLHUP | select.EPOLLRDHUP):
                    self.__epoll.unregister(detect_fileno)
                    self.close_client(detect_fileno)
                    
                elif detect_event & select.EPOLLIN:
                    self.__detect_epollin_fileno_queue.put_nowait(detect_fileno)
                    
                else:
                    print("unknown", detect_fileno, detect_event)
                    
        self.__epoll.close()
        self.__detect_epollin_fileno_queue.put_nowait(None)
        self.__send_fileno_queue.put_nowait(None)
        
        client_fileno_list = list(self.client_by_fileno.keys())
        for client_fileno in client_fileno_list:
            self.close_client(client_fileno)