class Client:
    def __init__(self):
        import platform
        system = platform.system()
        if system == "Linux":
            print("Client Linux")
            
        elif system == "Darwin":
            from . import darwin
            self.client = darwin.Client()
            
        elif system == "Windows":
            print("Client Windows")
            
    def connect(self, ip:str, port:int):
        self.client.connect(ip, port)
    
    def close(self):
        self.client.close()
    
    def send(self, data:bytes):
        return self.client.send(data)
    
    def sendall(self, data:bytes):
        self.client.sendall(data)
        
    def recv(self):
        return self.client.recv()

    def get_fileno(self) -> int:
        return self.client.get_fileno()
    
    def setblocking(self, is_block:bool):
        self.client.setblocking(is_block)
            
class Server:
    def __init__(self):
        import platform
        system = platform.system()
        if system == "Linux":
            from . import linux
            self.server = linux.Server()
            
        elif system == "Darwin":
            from . import darwin
            self.server = darwin.Server()
            
        elif system == "Windows":
            print("Server Windows")

    def start(self, listen_ip:str, listen_port:int, count_thread:int = 2, backlog:int = 5):
        self.server.start(listen_ip, listen_port, count_thread, backlog)
    
    def close(self):
        self.server.close()
    
    def send(self, fileno:int, data:bytes):
        self.server.send(fileno, data)
    
    def recv(self):
        return self.server.recv()