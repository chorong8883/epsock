# iosock
Simple I/O Socket
Working on.

```python
import iosock
server = iosock.EpollServer()
server.listen('localhost', 0000)
server.start()
recv_data = server.recv()
if recvdata['type'] == 'accept':
elif recvdata['type'] == 'debug':
server.join()
```
