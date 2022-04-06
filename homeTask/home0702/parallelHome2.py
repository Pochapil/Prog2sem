from threading import Thread, Lock
import random


# def random_write(lock):
#     for i in range(10):
#         lock.acquire()
#         x = random.randint(0, 100)
#         print(x)
#         if x % 2 == 0:
#             lock.release()
#
#
# def write_if_even(lock):
#     lock.acquire()
#     print("--is even number")
#     lock.release()
#
#
# L = Lock()
#
# p1 = Thread(target=random_write(L))
# p2 = Thread(target=write_if_even(L))
#
# p1.start()
# p2.start()
#
# p1.join()
# p2.join()


import random
import threading


def random_write(event_for_set, lock):
    for i in range(10):
        lock.acquire()
        x = random.randint(0, 100)
        print(x)
        if x % 2 == 0:
            event_for_set.set()
        lock.release()



def write_if_even(event_for_wait, lock):
    event_for_wait.wait()
    lock.acquire()
    print("--is even number")
    event_for_wait.clear()
    lock.release()


L = Lock()

e1 = threading.Event()

t1 = threading.Thread(target=random_write(e1, L))
t2 = threading.Thread(target=write_if_even(e1,L))

t1.start()
t2.start()

t1.join()
t1.join()




