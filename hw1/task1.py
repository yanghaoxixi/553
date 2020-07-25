from pyspark import SparkContext
import sys
import json
from operator import add


inputFile = sys.argv[1]
outputFile = sys.argv[2]
stopWordFile = sys.argv[3]
y = sys.argv[4]
m = sys.argv[5]
n = sys.argv[6]

# excluded words and punctuations
stopword = set()
with open(stopWordFile,'r') as swfile:
    for word in swfile:
        stopword.add(word.strip('\n'))
punctuation = ["(", "[", ",", ".", "!", "?", ":", ";", "]", ")"]

# input contents
sc = SparkContext()
input_json = sc.textFile(inputFile)
input_d = input_json.map(lambda item: json.loads(item))
#content = input_d.collect()

# output saved
result = dict()

# task A
result["A"] = input_d.count()

# task B
result["B"] = input_d.filter(lambda item: item['date'][0:4] == y).count()

# task C
result["C"] = input_d.map(lambda item: item['user_id']).distinct().count()

# task D
result["D"] = input_d.map(lambda item: (item['user_id'],1)).reduceByKey(add).map(lambda k: ((-k[1],k[0]),1)). \
    sortByKey().map(lambda k: [k[0][1],-k[0][0]]).take(int(m))

# task E
result["E"] = input_d.map(lambda item: item['text']).flatMap(lambda item: item.lower().split()).map(lambda item: item.strip('([,.!?:;])')). \
    filter(lambda item: item not in stopword and item != '' and item is not None).map(lambda item: (item,1)).reduceByKey(add). \
        map(lambda k: ((-k[1],k[0]),1)). \
        sortByKey().map(lambda k: k[0][1]).take(int(n))

print(result)

# write output
with open(outputFile,'w+') as opf:
    json.dump(result, opf)

#a = 1