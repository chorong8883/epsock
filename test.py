import threading
import time

lock1 = threading.Lock()
lock2 = threading.Lock()

def f1():
    lock1.acquire()
    print(1)
    print(1)
    print(1)
    print(1)
    lock1.release()

def f2():
    lock2.acquire()
    print(2)
    print(2)
    print(2)
    print(2)
    lock1.acquire()
    print(3)
    lock1.release()
    time.sleep(1)
    print(2)
    lock2.release()

t1 = threading.Thread(target=f1)
t2 = threading.Thread(target=f2)


t1.start()
t2.start()

t1.join()
t2.join()