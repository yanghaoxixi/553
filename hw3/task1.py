from pyspark import SparkContext
import sys
import time
import json
import random
from itertools import combinations


start = time.time()

""" inputFile = "train_review.json"
outputFile = "task1.res" """
inputFile = sys.argv[1]
outputFile = sys.argv[2]

sc = SparkContext()

# raw data- (user_id, business_id), example: ('OLR4DvqFxCKLOEHqfAxpqQ', 'zK7sltLeRRioqYwgLiWUIA'), len=14026
input_data = sc.textFile(inputFile).map(lambda row: json.loads(row)).map(lambda item: (item["business_id"],item["user_id"]))

# sorted distinct user list(len=9169) and business list(len=5379)
user_list = input_data.map(lambda item: item[1]).distinct().sortBy(lambda item: item).collect()
business_list = input_data.map(lambda item: item[0]).distinct().sortBy(lambda item: item).collect()

# give index to users and businesses based on their order in character
user_dict = {}
for index, item in enumerate(user_list):
    user_dict[item] = index
business_dict = {}
for index, item in enumerate(business_list):
    business_dict[item] = index

# list of tuples(business_id, [user_id1,user_id2,user_id3,........])
table = input_data.groupByKey().map(lambda k: (business_dict[k[0]],[user_dict[item] for item in k[1]]))
table_dict = table.collectAsMap()
# create 100 hash function, use big prime as p 
n = 50
m = len(user_list)
p = 520381
random.seed(10)
a = random.sample(range(1,p),n)
bias = random.sample(range(1,p),n)

# map (businessid,[user1,user2,...]) to (businessid, list of hashed [user1,user2,...]) to (business_id, [signatures list])
# example: (5306, [828, 318, 866, 269, 1218, 3908, 227, 1495, 4376, ...])
signature = table.mapValues(lambda k: [[((a[i] * j + bias[i]) % p) % m for j in k] for i in range(n)]).mapValues(lambda k: [min(a) for a in k])

# LSH - n=50, r = 1, b = 50
threshold = 0.05
r = 1
b = int(n/r)
# get pair of ((#band,signature),[business_id that have that key])
bands = signature.mapValues(lambda k: [tuple((i,tuple(k[r*i:r*i+r]))) for i in range(b)]).flatMap(lambda k: [(k[1][i],k[0]) for i in range(b)]) \
    .groupByKey()
# filter the keys that have more than 1 business, and take out candidate pairs
candidate = bands.map(lambda k: tuple(k[1])).filter(lambda k: len(k)>1).flatMap(lambda k: [tuple(sorted(cp)) for cp in combinations(k,2)]).distinct().collect()

# verification
# firstly generate reverse index dictionary for businesses
business_dict_re = {}
for index, item in enumerate(business_list):
    business_dict_re[index] = item
# iterate candidate list to get result pair
result = []
for pair in candidate:
    s1 = set(table_dict[pair[0]])
    s2 = set(table_dict[pair[1]])
    Jsimilarity = len(s1&s2)/len((s1|s2))
    if Jsimilarity >= threshold:
        result.append([business_dict_re[pair[0]],business_dict_re[pair[1]],Jsimilarity])

#Accuracy = len(result)/59435
#print(Accuracy)
# write file
with open(outputFile,'w+') as opf:
    for pair in result:
        opf.writelines(json.dumps({"b1":pair[0],"b2":pair[1],"sim":pair[2]}) + '\n')



print("Duration: %d s." % (time.time() - start))
#aaa = 1