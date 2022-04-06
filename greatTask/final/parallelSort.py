from itertools import repeat
import time
import numpy as np
import multiprocessing as mp
import os

# params
number_size_bytes = 4  # количество байт на 1 число
read_size_number = 10_000  # количество чисел в оперативной памяти
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
        # print("%s was sorted" % write_file_name)
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
        with open(first_file_name, 'rb') as first_file:
            with open(second_file_name, 'rb') as second_file:
                result_file_name = "%s%s.bin" % (first_file_name[:-4], second_file_name[:-4].replace("source", ""))
                with open(result_file_name, 'wb') as result_file:
                    first_file_size = os.stat(first_file_name).st_size
                    second_file_size = os.stat(second_file_name).st_size
                    # print(second_file_size)
                    j = 0
                    while j * read_size_number * number_size_bytes < max(first_file_size, second_file_size):
                        first = np.fromfile(first_file, count=read_size_number, dtype=np.int32)
                        second = np.fromfile(second_file, count=read_size_number, dtype=np.int32)
                        count_first = 0
                        count_second = 0
                        while True:
                            if first.size > 0 and (count_second == second.size or second.size == 0):
                                while count_first < first.size:
                                    first[count_first].tofile(result_file)
                                    count_first += 1
                                    # print("first file name = %s end" % first_file_name)
                                break
                            if second.size > 0 and (count_first == first.size or first.size == 0):
                                while count_second < second.size:
                                    second[count_second].tofile(result_file)
                                    count_second += 1
                                break
                            if first[count_first] < second[count_second]:
                                first[count_first].tofile(result_file)
                                count_first += 1
                            else:
                                second[count_second].tofile(result_file)
                                count_second += 1
                        j += 1
                # print("have finished merging files in %s" % result_file_name)
                result_file.close()
            second_file.close()
        first_file.close()
        queue.put(result_file_name)
        queue.task_done()
        queue.task_done()


# for i in range(number_of_files):
#     file_name = "%s%d.bin" % (source_file_name[:-4], i)


if __name__ == '__main__':

    # создание большого файла
    generate_flag = False
    number_of_files = 10  # количество файлов
    if generate_flag:
        with open(source_file_name, 'wb') as f:
            for i in range(number_of_files):
                buff = np.random.randint(2 ** 31 - 1, size=read_size_number, dtype=np.int32)
                # buff = np.random.randint(100, size=read_size_number, dtype=np.int32)
                buff.tofile(f)
            #buff = np.random.randint(2 ** 31 - 1, size=2, dtype=np.int32)
            #buff.tofile(f)
        print("end of generating")

    # разделение и сортировка
    print("----------------------begin to sort-------------------------")
    manager = mp.Manager()
    i = manager.Value('i', 0)
    queue = manager.Queue()
    lock = manager.Lock()
    source_file = open(source_file_name, 'rb')
    total_number = source_file.seek(0, 2) // number_size_bytes  # сколько всего чисел в файле
    quotient = total_number // read_size_number
    remainder = total_number % read_size_number
    source_file.close()
    with mp.Pool(mp.cpu_count()) as pool:  # start 4 worker processes
        pool.starmap(sort_parts, list(
            repeat([source_file_name, quotient, remainder, i, queue, lock], quotient + (1 if remainder > 0 else 0))),
                     chunksize=6)
    print("-----------------------end sorting---------------------------")


    # сколько раз нужно будет сливать файлы
    def count_merge_times(n):
        if n > 1:
            return n // 2 + count_merge_times(n // 2)
        else:
            return 1


    # t0 = time.time()
    # слияние файлов
    print("----------------------begin to merge-------------------------")
    with mp.Pool(mp.cpu_count()) as pool:  # start 4 worker processes
        # pool.starmap(merge_parts, [[queue, lock]], chunksize=2) #почему-то работает только 1 процесс
        pool.starmap(merge_parts,
                     list(repeat([queue, lock], count_merge_times(quotient + (1 if remainder > 0 else 0)))),
                     chunksize=2)
    print("-----------------------end merging---------------------------")

    # t1 = time.time()
    # print("run time = %d" % (t1 - t0))

    str = queue.get()
    print("result file is: %s" % str)
    # print("source:")
    # print(np.fromfile(source_file_name, dtype=np.int32))
    # print("sorted:")
    # print(np.fromfile(str, dtype=np.int32))
    # print(os.stat(str).st_size)
    # print(os.stat(source_file_name).st_size)
    # print("source0:")
    # print(np.fromfile("source0.bin", dtype=np.int32))