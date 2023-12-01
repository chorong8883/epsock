import signal
import threading

import iosock

server = iosock.RelayServer()


def signal_handler(num_recv_signal, frame):
    print(f"\nGet Signal: {signal.Signals(num_recv_signal).name}")
    server.close()
    print("Server Close.")

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGABRT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    server.relay('218.55.118.203', 59012, 'localhost', 60809)
    server.start()
    print("Server Start.")
    
    server.join()
    print("Join Server.")