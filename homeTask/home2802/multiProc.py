from multiprocessing import Pool


def f(file_name):
    file = open(file_name, 'r')
    str1 = file.readline()
    arr = []
    for x in str1.split():
        arr.append(int(x))
    arr.sort()
    file.close()
    file = open(file_name[:-4] + "Sorted.txt", 'w')
    str2 = ""
    for el in arr:
        str2 = str2 + str(el) + " "
    file.write(str2)
    file.close()
    print(file_name + " was sorted")

if __name__ == '__main__':
    with Pool(processes=4) as pool:  # start 4 worker processes
        pool.map(f, [("first.txt"), ("second.txt"), ("third.txt"), ("fourth.txt")], chunksize=1)
