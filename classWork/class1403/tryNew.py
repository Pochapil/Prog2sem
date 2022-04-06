import numpy as np

binary_file = "source.bin"
with open(binary_file, 'rb') as f:
    f.seek(16)
    my_array = np.fromfile(f, dtype=np.int32)

print(my_array)