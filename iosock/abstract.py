import abc
from abc import abstractmethod

class ClientBase(metaclass=abc.ABCMeta):
    @abstractmethod
    def connect(self): pass
    
    @abstractmethod
    def close(self): pass
    
    @abstractmethod
    def send(self, data:bytes) -> int: pass
    
    @abstractmethod
    def recv(self) -> bytes: pass
    
    @abstractmethod
    def get_fileno(self) -> int: pass
    
    @abstractmethod
    def setblocking(self, is_block:bool): pass
    
class ServerBase(metaclass=abc.ABCMeta):
    @abstractmethod
    def listen(self, ip:str, port:int, backlog:int): pass
    
    @abstractmethod
    def start(self, count_thread:int): pass
        
    @abstractmethod
    def close(self): pass
    
    @abstractmethod
    def join(self): pass
    
    @abstractmethod
    def send(self, data:bytes) -> int: pass
    
    # @abstractmethod
    # def recv(self) -> bytes: pass
    