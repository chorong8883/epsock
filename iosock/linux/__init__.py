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

from multiprocessing.pool import ThreadPool

from .. import abstract

from contextlib import contextmanager

@contextmanager
def acquire_timeout(lock:threading.Lock, timeout:float):
    result = lock.acquire(timeout=timeout)
    try:
        yield result
    finally:
        lock.release()

class Client(abstract.ClientBase):
    def __init__(self) -> None:
        print("linux client")
        
class Server(abstract.ServerBase):
    def __init__(self) -> None:
        self.__buffer_size = 10240
        self.__is_running = multiprocessing.Value(ctypes.c_bool, False)
        self.client_by_fileno = collections.defaultdict(dict)
        self.__recv_queue = queue.Queue()
        
        self.__detect_epollin_fileno_queue = queue.Queue()
        self.__send_fileno_queue = queue.Queue()
        
        self.temp_recv_data = collections.defaultdict(bytes)
        
    def create_client(self, client_socket) -> dict:
        return {
            "socket" : client_socket,
            "lock" : threading.Lock(),
            "send_buffer_queue" : queue.Queue(),
            "sending_buffer" : b''
        }
        
    def get_listener(self, listen_ip:str, listen_port:int, is_blocking:bool = False, backlog:int = 5) -> socket.socket:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setblocking(is_blocking)
        s.bind((listen_ip, listen_port))
        s.listen(backlog)
        return s 

    def start(self, listen_ip:str, listen_port:int, is_blocking:bool = False, backlog:int = 5):
        system = platform.system()
        if system == "Linux":
            self.__listen_socket = self.get_listener(listen_ip, listen_port, is_blocking, backlog)

            self.__epoll = select.epoll()
            listener_eventmask = select.EPOLLIN | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLET
            self.__epoll.register(self.__listen_socket, listener_eventmask)
            
            self.__is_running.value = True
            
            self.__len_close_epoll_thread = multiprocessing.Value(ctypes.c_int8, 0)
            self.__epoll_threads = []
            for _ in range(1):
                et = threading.Thread(target=self.__epoll_thread_function)
                et.start()
                self.__epoll_threads.append(et)
            
            self.close_event, self.close_event_listener = socket.socketpair()
            self.__epoll.register(self.close_event_listener, listener_eventmask)
            
            self.__recv_work_thread = threading.Thread(target=self.__recv_work)
            self.__send_work_thread = threading.Thread(target=self.__send_work)
            
            self.__recv_work_thread.start()
            self.__send_work_thread.start()
            
            
            
        elif system == "Darwin":
            print("This system is MacOS. Current process execute for Linux.")
            
        elif system == "Windows":
            print("This system is Windows. Current process execute for Linux.")
            
    def join(self):
        for et in self.__epoll_threads:
            et.join()
        
        self.close_event.close()
        
        self.__recv_work_thread.join()
        self.__send_work_thread.join()
    
    def stop(self):
        self.__is_running.value = False
        self.close_event.send(b'close')
        
    def close_client(self, client_fileno:int):
        client_data = self.client_by_fileno.pop(client_fileno)
        print(f"[{client_fileno}] Try Close. remain send buffer {len(client_data['send_buffer_queue'].queue)}")
        with client_data["lock"]:
            while not client_data['send_buffer_queue'].empty():
                _ = client_data['send_buffer_queue'].get_nowait()
        
        try:
        #     with client_data["lock"]:
        #         client_data["socket"].setblocking(True)
        #     with client_data["lock"]:
        #         client_data["socket"].settimeout(1)
        #     print(f'[{client_fileno}] client_socket settimeout')
        #     try:
        #         while True:
        #             if client_data['sending_buffer'] == b'':
        #                 print(f"[{client_fileno}] {len(client_data['send_buffer_queue'].queue)}")
        #                 client_data['sending_buffer'] = client_data['send_buffer_queue'].get_nowait()
                    
        #             start_index = 0
        #             end_index = len(client_data['sending_buffer'])
        #             while start_index < end_index:
        #                 with client_data["lock"]:
        #                     send_length = client_data["socket"].send(client_data['sending_buffer'][start_index:end_index])
        #                     if send_length <= 0:
        #                         break
        #                     start_index += send_length
        #             client_data['sending_buffer'] = b''
        #     except queue.Empty:
        #         print(f"[{client_fileno}] queue.Empty")
        #     print(f'[{client_fileno}] before client_socket.shutdown')
            with client_data["lock"]:
                client_data["socket"].shutdown(socket.SHUT_RDWR)
        # except TimeoutError:
        #     print(f"[{client_fileno}] TimeoutError")
        #     pass
        except ConnectionResetError:
            pass
        except BrokenPipeError:
            print(f"[{client_fileno}] BrokenPipeError")
            pass
        except OSError as e:
            if e.errno == errno.ENOTCONN: # errno 107
                print(f"[{client_fileno}] ENOTCONN")
                pass
            else:
                raise e
        except Exception as e:
            print(e, traceback.format_exc())
        
        with client_data["lock"]:
            client_data["socket"].close()
        
        print(f'[{client_fileno}] Closed')

#####################################################################################################################
#####################################################################################################################
#####################################################################################################################
    
    def recv(self):
        return self.__recv_queue.get()
    
    def __recv_work(self):
        while self.__is_running.value:
            detect_fileno = self.__detect_epollin_fileno_queue.get()
            if not detect_fileno:
                break
            
            client_data = self.client_by_fileno.get(detect_fileno)
            if client_data:
                result = b''
                try:
                    while True:
                        with acquire_timeout(client_data['lock'], 1) as acqiured:
                            if acqiured:
                                recv_bytes = client_data['socket'].recv(self.__buffer_size)
                                if recv_bytes:
                                    result += recv_bytes
                                else:
                                    break
                            else:
                                print("recv work timedout")
                        
                except BlockingIOError as e:
                    if e.errno == socket.EAGAIN:
                        pass
                    else:
                        raise e
                if result is not None and result != b'':
                    self.__recv_queue.put_nowait({
                        "fileno": detect_fileno,
                        "data": result
                    })
                
        self.__recv_queue.put_nowait(None)
        print("Finish Recver")
#####################################################################################################################
#####################################################################################################################
#####################################################################################################################

    def send(self, send_fileno:int, data:bytes = None):
        self.client_by_fileno[send_fileno]['send_buffer_queue'].put_nowait(data)
        self.__send_fileno_queue.put_nowait(send_fileno)
            
    def __send_work(self):
        while self.__is_running.value:
            send_fileno = self.__send_fileno_queue.get()
            if not send_fileno:
                break
        
            if self.client_by_fileno[send_fileno]:
                with acquire_timeout(self.client_by_fileno[send_fileno]['lock'], 1) as acqiured:
                    if acqiured:
                        if self.client_by_fileno[send_fileno]['sending_buffer'] == b'':
                            try:
                                self.client_by_fileno[send_fileno]['sending_buffer'] = self.client_by_fileno[send_fileno]['send_buffer_queue'].get_nowait()
                            except queue.Empty:
                                continue
                    else:
                        print("send work timedout buffering")
                
                start_index = 0
                end_index = len(self.client_by_fileno[send_fileno]['sending_buffer'])
                try:
                    while start_index < end_index:
                        with acquire_timeout(self.client_by_fileno[send_fileno]['lock'], 1) as acqiured:
                            if acqiured:
                                send_length = self.client_by_fileno[send_fileno]['socket'].send(self.client_by_fileno[send_fileno]['sending_buffer'][start_index:end_index])
                                if send_length <= 0:
                                    break
                                start_index += send_length
                            else:
                                print("send work timedout")
                        
                except BlockingIOError as e:
                    if e.errno == socket.EAGAIN:
                        pass
                    else:
                        raise e
                except BrokenPipeError:
                    # print(f"[{self.client_by_fileno[send_fileno]['socket'].fileno()}] BrokenPipeError cur:{send_fileno}")
                    pass
                    
                with self.client_by_fileno[send_fileno]['lock']:
                    if 0 <= start_index < end_index:
                        self.client_by_fileno[send_fileno]['sending_buffer'] = self.client_by_fileno[send_fileno]['sending_buffer'][start_index:end_index]
                    else:
                        self.client_by_fileno[send_fileno]['sending_buffer'] = b''
                
                if self.client_by_fileno[send_fileno]['sending_buffer'] != b'':
                    self.__send_fileno_queue.put_nowait(send_fileno)
                elif not self.client_by_fileno[send_fileno]['send_buffer_queue'].empty():
                    self.__send_fileno_queue.put_nowait(send_fileno)
    
        print("Finish Sender")
#####################################################################################################################
#####################################################################################################################
#####################################################################################################################            
            
    def __epoll_thread_function(self):
        while self.__is_running.value:
            events = self.__epoll.poll()
            for detect_fileno, detect_event in events:
                if detect_fileno == self.__listen_socket.fileno():
                    if detect_event & (select.EPOLLHUP | select.EPOLLRDHUP):
                        print(f"Detect Close Listner")
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
                
                elif detect_fileno == self.close_event_listener.fileno():
                    self.__len_close_epoll_thread.value += 1
                    if self.__len_close_epoll_thread.value < len(self.__epoll_threads):
                        self.close_event.send(b'close')
                    
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
        
        for key in self.temp_recv_data:
            print(f"[{threading.get_ident()}] [{key}] recv {len(self.temp_recv_data[key])} bytes\n{self.temp_recv_data[key]}")
            
        print("Finish Epoll")
