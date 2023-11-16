from .. import abstract

class Client(abstract.ClientBase):
    def __init__(self) -> None:
        print("darwin client")
        
class Server:
    def __init__(self) -> None:
        print("darwin server")
        