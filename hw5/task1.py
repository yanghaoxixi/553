from pyspark import SparkContext
import sys
import time 
import json
import csv
import binascii
import random


def predict(name):
    if name == '':
        return 0
    else:
        name_int = int(binascii.hexlify(name.encode('utf8')),16)
        name_hash = [((a[i] * name_int + bias[i])%p) % m for i in range(n)]
        for signature in name_hash:
            if signature not in existing_list:
                return 0
        return 1


start = time.time()



inputFile_first = 'business_first.json'
inputFile_second = 'business_second.json'
outputFile = 'task1output.csv'
""" inputFile_first = sys.argv[1]
inputFile_second = sys.argv[2]
outputFile = sys.argv[3] """


sc = SparkContext()
sc.setLogLevel('ERROR')


# read data
first_data = sc.textFile(inputFile_first).map(lambda k: json.loads(k)).map(lambda k: k['city'])
# city list
city_list = first_data.distinct().filter(lambda k: k != "")
# hash to interger
city_list_int = city_list.map(lambda k: int(binascii.hexlify(k.encode('utf8')),16)).collect()

# create 20 hash function, use big prime as p 
n = 10
m = 10000
p = 520381
random.seed(10)
a = random.sample(range(1,p),n)
bias = random.sample(range(1,p),n)
# hash to 1000 bit array
hashed_city_list_int = city_list_int.flatMap(lambda k: [((a[i] * k + bias[i])%p) % m for i in range(n)])
# get existing list
existing_list = sorted(hashed_city_list_int.distinct().collect())



# predict
second_data = sc.textFile(inputFile_second).map(lambda k: json.loads(k)).map(lambda k: k['city'])
new_city_list_int = second_data.map(lambda k: predict(k)).collect()

# output 
with open(outputFile, 'w+', newline="") as opf:
    for item in new_city_list_int[:-1]:
        opf.writelines(str(item) + ' ')
    opf.writelines(str(new_city_list_int[-1]) + ' ')


print("Duration: %d s." % (time.time() - start))
aaa = 1