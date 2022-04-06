from itertools import repeat
import time
import numpy as np
import multiprocessing as mp
import os

# params
number_size_bytes = 4  # количество байт на 1 число
read_size_number = 100  # количество чисел в 1 файле
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
        result_file_name = "%s%s.bin" % (first_file_name[:-4], second_file_name[:-4].replace("source", ""))
        with open(first_file_name, 'rb') as first_file:
            with open(second_file_name, 'rb') as second_file:
                with open(result_file_name, 'wb') as result_file:
                    first = np.fromfile(first_file, count=1, dtype=np.int32)
                    second = np.fromfile(second_file, count=1, dtype=np.int32)
                    while True:
                        if first < second:
                            first.tofile(result_file)
                            first = np.fromfile(first_file, count=1, dtype=np.int32)
                            # print("first was added")
                            if first.size < 1:
                                # print("first file end")
                                while second.size > 0:
                                    second.tofile(result_file)
                                    second = np.fromfile(second_file, count=1, dtype=np.int32)
                                break
                        else:
                            second.tofile(result_file)
                            second = np.fromfile(second_file, count=1, dtype=np.int32)
                            # print("second was added")
                            if second.size < 1:
                                while first.size > 0:
                                    first.tofile(result_file)
                                    first = np.fromfile(first_file, count=1, dtype=np.int32)
                                # print("second file end")
                                break
                print("have finished merging files in %s" % result_file_name)
                result_file.close()
            second_file.close()
        first_file.close()
        queue.put(result_file_name)
        queue.task_done()
        queue.task_done()


# for i in range(number_of_files):
#     file_name = "%s%d.bin" % (source_file_name[:-4], i)


if __name__ == '__main__':
    # создание текста для чтения
    generate_flag = True
    if generate_flag:
        with open(source_file_name, 'wb') as f:
            for i in range(number_of_files):
                buff = np.random.randint(2 ** 31 - 1, size=read_size_number, dtype=np.int32)
                buff.tofile(f)
            buff = np.random.randint(2 ** 31 - 1, size=2, dtype=np.int32)
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


    def count_merge_times(n):
        if n > 1:
            return n // 2 + count_merge_times(n // 2)
        else:
            return 1


    # print(count_merge_times(quotient + (1 if remainder > 0 else 0)))
    t0 = time.time()
    print("----------------------begin to merge-------------------------")
    with mp.Pool(mp.cpu_count()) as pool:  # start 4 worker processes
        # pool.starmap(merge_parts, [[queue, lock]], chunksize=2)
        pool.starmap(merge_parts,
                     list(repeat([queue, lock], count_merge_times(quotient + (1 if remainder > 0 else 0)))),
                     chunksize=2)
    print("-----------------------end merging---------------------------")
    t1 = time.time()
    print("run time = %d" % (t1 - t0))
    str = queue.get()
    print("result file is: %s" % str)
    print("source:")
    print(np.fromfile(source_file_name, dtype=np.int32))
    print("sorted:")
    print(np.fromfile(str, dtype=np.int32))
    print(os.stat(str).st_size)
    print(os.stat(source_file_name).st_size)
