import iosock
client = iosock.LinuxClient()
client.connect('218.55.118.203', 59012)
recv_bytes = client.recv(1024)
print(recv_bytes)
client.shutdown()
client.close()