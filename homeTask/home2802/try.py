from multiprocessing import Pool
import time
import pandas as pd

file_name = "first.txt"
file = open(file_name, 'r')
str1 = file.readline()
arr = []
for x in str1.split():
    arr.append(int(x))
for el in arr:
    print(el)
arr.sort()
print(arr)
file.close()
file = open(file_name[:-4] + "Sorted.txt", 'w')

str2 = ""
for el in arr:
    str2 = str2 + str(el) + " "
file.write(str2)
file.close()
