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


@contextmanager
def __acquire_timeout(self, lock:threading.Lock, timeout:float):
    result = lock.acquire(timeout=timeout)
    try:
        yield result
    finally:
        if result:
            lock.release()
@contextmanager
def __acquire_blocking(self, lock:threading.Lock, blocking:bool):
    result = lock.acquire(blocking=blocking)
    try:
        yield result
    finally:
        if result:
            lock.release()


class KqueueServer:
    def __init__(self) -> None:
        self.__buffer_size = 10240
        self.__is_running = multiprocessing.Value(ctypes.c_bool, False)
        self.client_by_fileno = collections.defaultdict(dict)
        self.__kevent_by_fileno = collections.defaultdict(select.kevent)
        self.__recv_queue = queue.Queue()
        
        self.__detect_epollin_fileno_queue = queue.Queue()
        self.__send_fileno_queue = queue.Queue()
    
    def start(self, listen_ip:str, listen_port:int, is_blocking:bool = False, backlog:int = 5):
        self.__listen_socket = self.get_listener(listen_ip, listen_port, is_blocking, backlog)

        self.__is_running.value = True
        
        self.__kq_thread = threading.Thread(target=self.__kq_thread_function)
        self.__recv_work_thread = threading.Thread(target=self.__recv_work)
        self.__send_work_thread = threading.Thread(target=self.__send_work)
        
        self.__kq_thread.start()
        self.__recv_work_thread.start()
        self.__send_work_thread.start()
        
        listen_socket_fileno = self.__listen_socket.fileno()
        kevent = select.kevent(listen_socket_fileno)
        self.__kevent_by_fileno[listen_socket_fileno] = kevent
        
    def stop(self):
        self.__is_running.value = False
        self.__listen_socket.shutdown(socket.SHUT_RDWR)
        
    def join(self):
        self.__kq_thread.join()
        self.__recv_work_thread.join()
        self.__send_work_thread.join()
    
    def create_client(self, client_socket):
        return {
            "socket" : client_socket,
            "lock" : threading.Lock(),
            "send_buffer_queue" : queue.Queue(),
            "sending_buffer" : b''
        }
    
    def recv(self):
        return self.__recv_queue.get()
    
    def __recv_work(self):
        while self.__is_running.value:
            detect_fileno = self.__detect_epollin_fileno_queue.get()
            if not detect_fileno:
                self.__recv_queue.put_nowait(None)
                break
            
            result = b''
            with __acquire_timeout(self.client_by_fileno[detect_fileno]['lock'], 1) as acqiured:
                try:
                    while True:
                        recv_bytes = self.client_by_fileno[detect_fileno]['socket'].recv(self.__buffer_size)
                        if recv_bytes == None or recv_bytes == -1 or recv_bytes == b'':
                            break
                        elif recv_bytes:
                            result += recv_bytes
                        else:
                            break
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
            if client_data:
                with __acquire_timeout(self.client_by_fileno[send_fileno]['lock'], 1) as acqiured:
                    if acqiured:
                        if self.client_by_fileno[send_fileno]['sending_buffer'] == b'':
                            try:
                                self.client_by_fileno[send_fileno]['sending_buffer'] = self.client_by_fileno[send_fileno]['send_buffer_queue'].get_nowait()
                                self.client_by_fileno[send_fileno]['socket'].send(b'')
                            except queue.Empty:
                                return
                        
                        start_index = 0
                        end_index = len(self.client_by_fileno[send_fileno]['sending_buffer'])
                        try:
                            while start_index < end_index:
                                send_length = self.client_by_fileno[send_fileno]['socket'].send(self.client_by_fileno[send_fileno]['sending_buffer'][start_index:end_index])
                                if send_length <= 0:
                                    break
                                start_index += send_length
                        except BlockingIOError as e:
                            if e.errno == socket.EAGAIN:
                                pass
                            else:
                                raise e
                            
                        if 0 <= start_index < end_index:
                            self.client_by_fileno[send_fileno]['sending_buffer'] = self.client_by_fileno[send_fileno]['sending_buffer'][start_index:end_index]
                        else:
                            self.client_by_fileno[send_fileno]['sending_buffer'] = b''
                            self.client_by_fileno[send_fileno]['socket'].send(b'')
                        
                        if self.client_by_fileno[send_fileno]['sending_buffer'] != b'':
                            self.__send_fileno_queue.put_nowait(send_fileno)
                        elif not self.client_by_fileno[send_fileno]['send_buffer_queue'].empty():
                            self.__send_fileno_queue.put_nowait(send_fileno)
                        
                    # else:
                        # print('send lock timeout')
                            
#####################################################################################################################
#####################################################################################################################
#####################################################################################################################            
            
    
    def get_listener(self, listen_ip:str, listen_port:int, is_blocking:bool = False, backlog:int = 5) -> socket.socket:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setblocking(is_blocking)
        s.bind((listen_ip, listen_port))
        s.listen(backlog)
        return s 

    def __kq_thread_function(self):
        kq = select.kqueue()
        while self.__is_running.value:
            kevents = self.__kevent_by_fileno.values()
            events = kq.control(list(kevents), 1000)
            for event in events:
                if event.flags & select.KQ_EV_ERROR:
                    # print("event.flags & select.KQ_EV_ERROR")
                    return
                    
                if event.ident == self.__listen_socket.fileno():
                    if event.flags & select.KQ_EV_EOF:
                        # print(f"event.ident == detect_close_fd.fileno() {event}")
                        self.__is_running.value = False
                        continue
                    else:
                        client_socket, address = self.__listen_socket.accept()
                        client_socket_fileno = client_socket.fileno()
                        # print(f"accept {client_socket_fileno} {address}")
                        client_socket.setblocking(False)
                        client = self.client_by_fileno.get(client_socket_fileno)
                        if client:
                            s:socket.socket = client["socket"]
                            try:
                                s.shutdown(socket.SHUT_RDWR)
                            except Exception as e:
                                pass
                            s.close()
                        client_data = self.create_client(client_socket)
                        self.client_by_fileno.update({client_socket_fileno : client_data})
                        
                        kevent = select.kevent(client_socket_fileno)
                        self.__kevent_by_fileno.update({client_socket_fileno:kevent})
                        
                elif event.filter == select.KQ_FILTER_READ:
                    if event.flags & select.KQ_EV_EOF:
                        # print("event.flags & select.KQ_EV_EOF")
                        self.__kevent_by_fileno.pop(event.ident)
                        client_data = self.client_by_fileno.pop(event.ident)
                        client_data['socket'].shutdown(socket.SHUT_RDWR)
                        client_data['socket'].close()
                
                    else:
                        client_data = self.client_by_fileno.pop(event.ident)
                        data = client_data['socket'].recv()
                        # print(data)
                
        kq.close()
        
        
        

