import abc
from abc import abstractmethod

class ClientBase(metaclass=abc.ABCMeta):
    @abstractmethod
    def connect(self): pass
    @abstractmethod
    def close(self): pass
    
class ServerBase(metaclass=abc.ABCMeta):
    @abstractmethod
    def start(self): pass
    @abstractmethod
    def stop(self): pass
    @abstractmethod
    def join(self): pass