import numpy as np

file = open("array.bin", "wb")
num = [2, 4, 6, 8, 10]
array = bytearray(num)
file.write(array)
file.close()

file = open("array.bin", "rb")
# number = list(file.read(4))
s = (file.read(4))
# print(number)
file.close()


import os

def getSize(filename):
    st = os.stat(filename)
    return st.st_size
print(np.dtype(int))
l = np.int32

print(np.frombuffer(s, dtype=np.int32))


f = open( ... )

f.seek(last_pos)

line = f.readline()  # no 's' at the end of `readline()`

last_pos = f.tell()

f.close()
just remember, last_pos is not a line number in your file, it's a byte offset from the beginning of the file -- there's no point in incrementing/decrementing it.





with open(file_path, 'rb') as file_obj:
    file_obj.seek(seek_to_position)
    data_ro = np.frombuffer(file_obj.read(total_num_bytes), dtype=your_dtype_here)
    data_rw = data_ro.copy() #without copy(), the result is read-only




import numpy as np

binary_file = "sample_binary.bin"
with open(binary_file, 'rb') as f:
    f.seek(40)
    my_array = np.fromfile(f, dtype=np.uint8)

