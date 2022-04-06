
import numpy as np
import os

number_size_bytes = 4  # количество байт на 1 число
read_size_number = 10  # количество чисел в 1 файле
number_of_files = 10  # количество файлов

first_file_name = "source1.bin"
second_file_name = "source2345.bin"
result_file_name = "result.bin"

with open(first_file_name, 'rb') as first_file:
    with open(second_file_name, 'rb') as second_file:
        #result_file_name = "%s%s.bin" % (first_file_name[:-4], second_file_name[:-4].replace("source", ""))
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
                for i in range(first.size + second.size):

                    if first.size > 0 and (count_second == second.size - 1 or second.size == 0):
                        while count_first < first.size:
                            first[count_first].tofile(result_file)
                            count_first += 1
                            # print("first file name = %s end" % first_file_name)
                        break
                    if second.size > 0 and (count_first == first.size - 1 or first.size == 0):
                        while count_second < second.size:
                            second[count_second].tofile(result_file)
                            count_second += 1
                        break
                    if (first.size > 0 and second.size > 0):
                        if first[count_first] < second[count_second]:
                            first[count_first].tofile(result_file)
                            count_first += 1
                        else:
                            second[count_second].tofile(result_file)
                            count_second += 1
                j += 1
        print("have finished merging files in %s" % result_file_name)
        result_file.close()
    second_file.close()
first_file.close()