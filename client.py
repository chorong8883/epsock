import signal
import time
import iosock

client = iosock.Client()

def signal_handler(num_recv_signal, frame):
    recv_signal = signal.Signals(num_recv_signal)
    print(f"Get Signal: {recv_signal.name}")
    client.close()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGABRT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

client.connect('218.55.118.203', 59012)
time.sleep(1)
client.send(b'hello')
client.join()