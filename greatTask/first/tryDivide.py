from multiprocessing import Pool

# params
number_size_bytes = 32  # количество бит на 1 число
read_size_number = 2  # количество чисел в 1 файле
read_size_bytes = number_size_bytes * read_size_number
number_of_files = 10

source_file_name = "source.bin"

source_file = open(source_file_name, 'wb')
for i in range(10):
    source_file.write(i.to_bytes(length=number_size_bytes, byteorder='big'))
source_file.close()

source_file = open(source_file_name, 'rb')
files_to_sort = []
for i in range(number_of_files):
    write_file_name = "%s%d.bin" % (source_file_name[:-4], i)
    files_to_sort.append(write_file_name)
    print(write_file_name)
    write_file = open(write_file_name, 'wb')
    write_file.write(source_file.read(read_size_bytes))
    write_file.close()
source_file.close()

# test
for i in range(number_of_files):
    read_file_name = "%s%d.bin" % (source_file_name[:-4], i)
    print(read_file_name)
    read_file = open(read_file_name, 'rb')
    for j in range(read_size_number):
        a1 = read_file.read(number_size_bytes)
        print(int.from_bytes(a1, byteorder='big'))
    print()
    read_file.close()


# todo sort in files

def f(file_name):
    file = open(file_name, 'rb')
    arr = []
    while True:
        number = file.read(number_size_bytes)
        if number == b"":
            break
        arr.append(int.from_bytes(number, byteorder='big'))
    arr.sort()
    file.close()
    file = open(file_name, 'wb')
    for i in range(len(arr)):
        file.write(arr[i].to_bytes(length=number_size_bytes, byteorder='big'))
    file.close()
    print(file_name + " was sorted")


# for i in range(number_of_files):
#     file_name = "%s%d.bin" % (source_file_name[:-4], i)


if __name__ == '__main__':
    print("----------------------begin to sort-------------------------")
    with Pool(processes=4) as pool:  # start 4 worker processes
        pool.map(f, files_to_sort, chunksize=1)
