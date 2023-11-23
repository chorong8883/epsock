import collections
import threading
import time

ad = collections.defaultdict(threading.Thread)
is_running = True
def test():
    while is_running:
        time.sleep(1)
        print("run")
adt = threading.Thread(target=test)
adt.start()

ad[adt.ident] = adt

print(len(ad))

time.sleep(3)

is_running = False

ad[adt.ident].join()
ad.popitem()

print(len(ad))