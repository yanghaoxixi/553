from pyspark import SparkContext
from graphframes import GraphFrame
from pyspark.sql import SparkSession
import time
import sys
import os
import itertools

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell")

start = time.time()

""" threshold = '7'
inputFile = 'ub_sample_data.csv'
outputFile = 'task1_output.txt' """
threshold = sys.argv[1]
inputFile = sys.argv[2]
outputFile = sys.argv[3]


sc = SparkContext()
sparkSession = SparkSession(sc)

# got data
input_data = sc.textFile(inputFile)
attri = input_data.take(1)[0]
#attri = 'user_id,business_id'
# remove header
data = input_data.filter(lambda row: row != attri).map(lambda k: k.split(',')) \
    .map(lambda k: (k[0],k[1])).groupByKey().filter(lambda k: len(list(k[1]))>=int(threshold)) \
        .mapValues(lambda k: sorted(k)).collectAsMap()
users_list = list(data.keys())
pairs_list = list(itertools.combinations(users_list,2))

#got edge rdd and vertex rdd
edge_list = sc.parallelize(pairs_list).filter(lambda k: len(set(data[k[0]])&set(data[k[1]]))>=int(threshold)) \
    .flatMap(lambda k: (k,(k[1],k[0])))
vertex_list = edge_list.flatMap(lambda k: k).distinct().map(lambda k: (k,))

# got graph
graph = GraphFrame(vertex_list.toDF(['id']),edge_list.toDF(['src','dst']))
communitiy = graph.labelPropagation(maxIter=5)
# go back to rdd
result1 = communitiy.rdd.collect()
result = sc.parallelize(result1).map(lambda k: (k[1],k[0])).groupByKey().mapValues(lambda k: sorted(list(k))) \
    .map(lambda k: k[1]).sortBy(lambda k: (len(k),k)).collect()

# output 
with open(outputFile, 'w+') as opf:
    for line in result:
        for content in line[:-1]:
            opf.writelines("'" + content + "', ")
        opf.writelines("'" + line[-1] + "'\n")



print("Duration: %d s." % (time.time() - start))
a = 1