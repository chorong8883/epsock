import iosock
import signal


server = iosock.Server()

def signal_handler(num_recv_signal, frame):
    recv_signal = signal.Signals(num_recv_signal)
    print(f"Get Signal: {recv_signal.name}")
    server.stop()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGABRT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

server.start('218.55.118.203', 59012)
server.join()