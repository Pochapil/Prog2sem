import random
import threading

end_flag = False


def random_write(event_for_wait, event_for_set):
    global end_flag
    for i in range(10):
        event_for_wait.wait()
        x = random.randint(0, 100)
        print(x, end="")
        if x % 2 == 0:
            event_for_wait.clear()
            event_for_set.set()
        else:
            print()
    end_flag = True


def write_if_even(event_for_wait, event_for_set):
    while not end_flag:
        event_for_wait.wait()
        event_for_wait.clear()
        print("--is even number")
        event_for_set.set()


e1 = threading.Event()
e2 = threading.Event()

t1 = threading.Thread(target=random_write, args=(e1, e2))
t2 = threading.Thread(target=write_if_even, args=(e2, e1))

t1.start()
t2.start()

e1.set()

t1.join()
t1.join()
