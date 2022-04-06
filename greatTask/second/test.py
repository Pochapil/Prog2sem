import numpy as np
import os

first_file_name = "source1.bin"
second_file_name = "source2.bin"
result_file_name = "result.bin"



first_file_size = os.stat(first_file_name).st_size
print(first_file_size)

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