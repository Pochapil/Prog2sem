import random
import threading
import queue

q = queue.Queue()


def random_write():
    for i in range(10):
        x = random.randint(0, 100)
        q.put(x)


def write_if_even():
    while True:
        x = q.get()
        print(x, end="")
        if x % 2 == 0:
            print("--is even number", end="")
        print()
        q.task_done()


t1 = threading.Thread(target=random_write)
t2 = threading.Thread(target=write_if_even, daemon=True)

t1.start()
t2.start()

q.join()

t1.join()
t1.join()
