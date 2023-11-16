import platform
from . import darwin
from . import linux

class Client:
    def __init__(self):
        system = platform.system()
        if system == "Linux":
            print("Client Linux")
            
        elif system == "Darwin":
            print("Client MacOS")
            self.client = darwin.Client()
            
        elif system == "Windows":
            print("Client Windows")
            
class Server:
    def __init__(self):
        system = platform.system()
        if system == "Linux":
            print("Server Linux")
            self.server = linux.Server()
            
        elif system == "Darwin":
            print("Server MacOS")
            
        elif system == "Windows":
            print("Server Windows")

    def start(self, listen_ip:str, listen_port:int, is_blocking:bool = False, backlog:int = 5):
        self.server.start(listen_ip, listen_port, is_blocking, backlog)
    def join(self):
        self.server.join()
    def stop(self):
        self.server.stop()