import json
import sys
from operator import add
from pyspark import SparkContext


reviewFile = sys.argv[1]
outputFile = sys.argv[2]
pat_type = sys.argv[3]
np = sys.argv[4]
n = sys.argv[5]


result = dict()
sc = SparkContext()

review = sc.textFile(reviewFile).map(lambda item: json.loads(item)).map(lambda item: (item['business_id'],1))
# set np as # of partitions, and partitionby their first character of key.
if pat_type == 'customized':
    review = review.partitionBy(int(np),lambda key: ord(key[0]))
result['n_partitions'] = review.getNumPartitions()
result['n_items'] = review.glom().map(lambda row: len(row)).collect()
result['result'] = review.reduceByKey(add).filter(lambda item: item[1] > int(n)).collect()
# output
with open(outputFile, 'w+') as opf:
    json.dump(result, opf)

#a = 1