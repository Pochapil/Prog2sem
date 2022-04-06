
from multiprocessing import Pool
import time


def f(file_name):
    file = open(file_name, 'r')


if __name__ == '__main__':
    with Pool(processes=4) as pool:  # start 4 worker processes
        print(pool.map(f, range(10)))  # prints "[0, 1, 4,..., 81]"

        result = pool.map_async(time.sleep, (10,))
        print(result.get(timeout=1))  # raises multiprocessing.TimeoutError




f = open("first.txt", 'r')
f1 = open("first.txt", 'r')


