import random
from itertools import repeat

import numpy as np
import multiprocessing as mp

# params
number_size_bytes = 4  # количество байт на 1 число
read_size_number = 1000  # количество чисел в 1 файле
number_of_files = 10  # количество файлов
source_file_name = "source.bin"


def sort_parts(source_file_name, quotient, remainder, i, queue, lock):
    with open(source_file_name, 'rb') as source_file:
        lock.acquire()
        # при считывании перемещаем каретку на правильное количество байт
        seek_to_position = i.value * read_size_number * number_size_bytes
        # print(i, seek_to_position)
        source_file.seek(seek_to_position)  # при считывании перемещаем каретку на правильное количество байт
        if i.value < quotient:
            array_to_sort = np.fromfile(source_file, dtype=np.int32, count=read_size_number)
        else:
            array_to_sort = np.fromfile(source_file, dtype=np.int32, count=remainder)
        # print(array_to_sort)
        lock.release()
        write_file_name = "%s%d.bin" % (source_file_name[:-4], i.value)
        i.value += 1
        # write_file_name = "" + source_file_name[:-4] + i + ".bin"
        array_to_sort.sort()
        array_to_sort.tofile(write_file_name)
        queue.put(write_file_name)
        print("%s was sorted" % write_file_name)
    source_file.close()


def merge_parts(queue, lock):
    while True:
        lock.acquire()
        if not queue.empty():
            first_file_name = queue.get()
        else:
            lock.release()
            break
        if not queue.empty():
            second_file_name = queue.get()
        else:
            queue.put(first_file_name)
            lock.release()
            break
        lock.release()
        array_a = np.fromfile(first_file_name, dtype=np.int32)
        array_b = np.fromfile(second_file_name, dtype=np.int32)
        array_result = []
        count_a = 0
        count_b = 0
        for i in range(len(array_a) + len(array_b)):
            if count_b == len(array_b) - 1:
                while count_a < len(array_a):
                    array_result.append(array_a[count_a])
                    count_a += 1
                break
            if count_a == len(array_a) - 1:
                while count_b < len(array_b):
                    array_result.append(array_b[count_b])
                    count_b += 1
                break
            if array_a[count_a] < array_b[count_b]:
                array_result.append(array_a[count_a])
                count_a += 1
            else:
                array_result.append(array_b[count_b])
                count_b += 1
        array_result = np.array(array_result)
        result_file_name = "%s%s.bin" % (first_file_name[:-4], second_file_name[:-4].replace("source", ""))
        array_result.tofile(result_file_name)
        print("have finished merging files in %s" % result_file_name)
        queue.put(result_file_name)
        queue.task_done()
        queue.task_done()


# for i in range(number_of_files):
#     file_name = "%s%d.bin" % (source_file_name[:-4], i)


if __name__ == '__main__':
    # создание текста для чтения
    generate_flag = False
    if generate_flag:
        with open(source_file_name, 'wb') as f:
            for i in range(number_of_files):
                buff = np.random.randint(2 ** 31 - 1, size=read_size_number, dtype=np.int32)
                buff.tofile(f)
        print("end of generating")

    # test
    print("----------------------begin to sort-------------------------")
    manager = mp.Manager()
    i = manager.Value('i', 0)
    queue = manager.Queue()
    lock = manager.Lock()
    # print("cpu_count = %d" % mp.cpu_count())
    source_file = open(source_file_name, 'rb')
    total_number = source_file.seek(0, 2) // number_size_bytes  # сколько всего чисел в файле
    quotient = total_number // read_size_number
    remainder = total_number % read_size_number
    source_file.close()
    # with get_context("spawn").Pool() as pool:
    with mp.Pool(mp.cpu_count()) as pool:  # start 4 worker processes
        # pool.starmap(sort_parts, [[source_file_name, read_size_number, i, queue, lock]], chunksize=5)
        pool.starmap(sort_parts, list(
            repeat([source_file_name, quotient, remainder, i, queue, lock], quotient + (1 if remainder > 0 else 0))),
                     chunksize=6)
    print("-----------------------end sorting---------------------------")

    print("----------------------begin to merge-------------------------")
    with mp.Pool(mp.cpu_count()) as pool:  # start 4 worker processes
        pool.starmap(merge_parts, [[queue, lock]])
    print("-----------------------end merging---------------------------")

    while not queue.empty():
        str = queue.get()
        print("result file is: %s" % str)
        # print(np.fromfile(str, dtype=np.int32))
        print()
