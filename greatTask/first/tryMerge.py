number_size_bytes = 32


def merge(first_file_name, second_file_name, result_file_name):
    first_file = open(first_file_name, 'rb')  # 'myfile5.bin'
    second_file = open(second_file_name, 'rb')
    result_file = open(result_file_name, 'wb')
    a = first_file.read(number_size_bytes)
    b = second_file.read(number_size_bytes)
    while True:  # b"" - пустая строка
        if a == b"":
            while b != b"":
                result_file.write(b)
                b = second_file.read(number_size_bytes)
            break
        if b == b"":
            while a != b"":
                result_file.write(a)
                a = first_file.read(number_size_bytes)
            break
        result_file.write(a if a < b else b)
        if a < b:
            a = first_file.read(number_size_bytes)
        else:
            b = second_file.read(number_size_bytes)
    first_file.close()
    second_file.close()
    result_file.close()


arr1 = [1, 4, 3, 2, 21, 16, 29, 19]
arr2 = [100, 2, 23, 18, 8, 9, 11]

arr1.sort()
arr2.sort()

first_file_name, second_file_name, result_file_name = "first.bin", "second.bin", "result.bin"
f = open(first_file_name, 'wb')
for el in arr1:
    f.write(el.to_bytes(length=number_size_bytes, byteorder='big'))
f.close()

f = open(second_file_name, 'wb')
for el in arr2:
    f.write(el.to_bytes(length=number_size_bytes, byteorder='big'))
f.close()

merge("first.bin", "second.bin", "result.bin")
f = open(result_file_name, 'rb')
while True:
    number = f.read(number_size_bytes)
    if number == b"":
        break
    print(int.from_bytes(number, byteorder='big'))
