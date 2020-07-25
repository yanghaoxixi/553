from pyspark import SparkContext
import sys
import time
import datetime 
import json
import csv
import binascii
import random
from pyspark.streaming import StreamingContext

def FlajoletMartin(data):
    # process origin data

    # timestamp
    time1 = time.time()
    time_now = datetime.datetime.fromtimestamp(time1).strftime('%Y-%m-%d %H:%M:%S')

    # ground truth
    grounTruthNum = data.distinct().count()
    

    # estimate
    # create 100 hash function, use big prime as p 
    n = 100
    p = 52038
    m = 250
    random.seed(10)
    a = random.sample(range(1,p),n)
    bias = random.sample(range(1,p),n)

    # hash + map to binary and fix length
    city_list_int = data.filter(lambda k: k!='').map(lambda k: int(binascii.hexlify(k.encode('utf8')),16))
    hashed_city_list_int = city_list_int.map(lambda k: [(int(a[i] * k + bias[i]) % p) % m for i in range(n)]) \
        .map(lambda k: [format(i, 'b') for i in k]).collect()
    # find R
    Rlist = []
    for i in range(n):
        R = -1
        for line in hashed_city_list_int:
            if len(line[i].rstrip('0')) != 0:
                nof0 = len(line[i])-len(line[i].rstrip('0'))
                if nof0 > R:
                    R = nof0
        if R >= 0:
            Rlist.append(2**R)
        else:
            Rlist.append(1)

    # calculate avg, partition into 3 groups, and get median
    avg_list = []
    numofgroup = 50
    #median_index = int((numofgroup - 1)/2)
    groupsize = int(n/numofgroup)
    for j in range(numofgroup):
        avg_list.append(sum(Rlist[groupsize*j:groupsize*j+groupsize])/groupsize)
    if numofgroup%2 == 1:
        median_index = int((numofgroup - 1)/2)
        median = int(sorted(avg_list)[median_index])
    else:
        median_index = int(numofgroup/2)
        median = int((sorted(avg_list)[median_index] + sorted(avg_list)[median_index - 1])/2)
    
    # output
    with open(outputFile,'a') as opf:
        opf.write(str(time_now) + "," + str(grounTruthNum) + "," + str(median) + "\n")





""" port = sys.argv[1]
outputFile = sys.argv[2] """

port = '9999'
outputFile = "task2output.csv"
port = int(port)

# set spark streaming
sc = SparkContext()
sc.setLogLevel("OFF")
ssc = StreamingContext(sc, 5)

# create output file
with open(outputFile,'w') as opf:
    opf.write("Time,Ground Truth,Estimation\n")

# get data
input_data = ssc.socketTextStream("localhost",port)

# window
stream_window = input_data.window(30,10).map(lambda k: json.loads(k)).map(lambda k: k['city']).foreachRDD(FlajoletMartin)

# run
ssc.start()
ssc.awaitTermination()