import random
from itertools import repeat

import numpy as np
import multiprocessing as mp

# params
number_size_bytes = 4  # количество бит на 1 число
read_size_number = 10  # количество чисел в 1 файле
read_size_bytes = number_size_bytes * read_size_number
number_of_files = 10

source_file_name = "source.bin"


def sort_parts(source_file_name, read_size_number, i, queue, lock):
    lock.acquire()
    print(i)
    array_to_sort = np.fromfile(source_file_name, dtype=np.int32, count=read_size_number)
    i.value += 1
    print(array_to_sort)
    lock.release()
    write_file_name = "%s%d.bin" % (source_file_name[:-4], i.value)
    # write_file_name = "" + source_file_name[:-4] + i + ".bin"
    array_to_sort.sort()
    array_to_sort.tofile(write_file_name)
    queue.put(write_file_name)
    print("%s was sorted" % write_file_name)


# for i in range(number_of_files):
#     file_name = "%s%d.bin" % (source_file_name[:-4], i)


if __name__ == '__main__':
    # создание текста для чтения
    number_list = []
    for i in range(100):
        number = random.randint(0, 100)
        number_list.append(number)
    array = np.array(number_list, dtype=np.int32)
    array.tofile(source_file_name)

    # test
    print("----------------------begin to sort-------------------------")
    manager = mp.Manager()
    i = manager.Value('i', 0)
    queue = manager.Queue()
    lock = manager.Lock()
    print("cpu_count = %d" % mp.cpu_count())
    source_file = open(source_file_name, 'rb')
    total_number = source_file.seek(0, 2) // number_size_bytes # сколько всего чисел в файле
    with mp.Pool(mp.cpu_count()) as pool:  # start 4 worker processes
        # pool.starmap(sort_parts, [[source_file_name, read_size_number, i, queue, lock]], chunksize=5)
        pool.starmap(sort_parts, list(repeat([source_file_name, read_size_number, i, queue, lock], 10)), chunksize=5)

    print("----------------------end sorting---------------------------")

    while not queue.empty():
        str = queue.get()
        print(str)
        print(np.fromfile(str, dtype=np.int32, count=read_size_number))
        print()
