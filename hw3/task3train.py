from pyspark import SparkContext
import sys
import time
import json
import collections
from itertools import combinations
import random


def listpair2dict(listpair):
    rdict = dict()
    for pair in listpair:
        rdict[pair[0]] = pair[1]
    return rdict
def pearsonSim(pair):
    dict1 = matrix_dict[pair[0]]
    dict2 = matrix_dict[pair[1]]
    # got co-rated list
    corated_list = list(set(dict1.keys()) & set(dict2.keys()))
    # calculate avg for i and j
    star1 = []
    star2 = []
    for item in corated_list:
        star1.append(dict1[item])
        star2.append(dict2[item])
    avg1 = sum(star1)/len(star1)
    avg2 = sum(star2)/len(star2)
    # calculate cprrelation
    up = 0
    for i in range(len(corated_list)):
        up += (star1[i] - avg1) * (star2[i] - avg2)
    if up == 0:
        return -1
    else:
        left = 0
        right = 0
        for i in range(len(corated_list)):
            left += (star1[i] - avg1) ** 2
            right += (star2[i] - avg2) ** 2
        down = (left * right) ** 0.5
        if down == 0:
            return -1
        else:
            return up/down
def numOfCoRated(k,z):
    if len(set(matrix_dict[k[0]].keys()) & set(matrix_dict[k[1]].keys()))>=z:
        return True
    else:
        return False
def JaccardSim(k):
    set1 = set(table_dict[k[0]])
    set2 = set(table_dict[k[1]])
    if len(set1&set2) / len(set1|set2) >= 0.01:
        return True
    else:
        return False



start = time.time()

""" inputFile = "train_review.json"
outputFile = "task3user.model"
cf_type = "user_based" """
inputFile = sys.argv[1]
outputFile = sys.argv[2]
cf_type = sys.argv[3]

sc = SparkContext()

# raw data
""" input_data = sc.textFile(inputFile).map(lambda line: json.loads(line)).map(lambda k: ((k['user_id'],k['business_id']),k["stars"])) \
    .groupByKey().mapValues(lambda k: list(k)[0]).map(lambda k: (k[0][0],k[0][1],k[1])) """
input_data = sc.textFile(inputFile).map(lambda line: json.loads(line)).map(lambda k: (k['user_id'],k['business_id'],k["stars"]))    

# sorted distinct user list(len=9169) and business list(len=5379)
user_list = input_data.map(lambda item: item[0]).distinct().sortBy(lambda item: item).collect()
business_list = input_data.map(lambda item: item[1]).distinct().sortBy(lambda item: item).collect()

# give index to users and businesses based on their order in character
user_dict = {}
for index, item in enumerate(user_list):
    user_dict[item] = index
business_dict = {}
for index, item in enumerate(business_list):
    business_dict[item] = index
user_dict_re = {}
for key, value in user_dict.items():
    user_dict_re[value] = key
business_dict_re = {}
for key, value in business_dict.items():
    business_dict_re[value] = key
# index data: [(3662, 5306, 5.0), (255, 3610, 5.0), (5646, 4343, 3.0), ...] (uid,bid,stars)
input_data = input_data.map(lambda k: (user_dict[k[0]],business_dict[k[1]],k[2]))

# implement CF
# item based, group by item, filter > 3 users(or cant have more than 3 corated user)
if cf_type == 'item_based':
    business_group = input_data.map(lambda k: (k[1],(k[0],k[2]))).groupByKey().filter(lambda k: len(list(k[1])) >= 3) \
        .map(lambda k: (k[0],list(k[1]))).mapValues(lambda k: listpair2dict(k))
    # save business-user-star info, a dict of bus: {user: star, user: star,......}
    # {0: {836: 5.0, 918: 5.0, 1607: 4.0, 1807: 4.0, 2715: 5.0, 6921: 1.0}, ...}
    matrix_dict = business_group.collectAsMap()
    # get pair
    business_candidate = business_group.map(lambda k: k[0]).collect()
    buspair = list(combinations(business_candidate,2))
    ### !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!change back to 3 after test!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    business_pair = sc.parallelize(buspair).filter(lambda k: len(set(matrix_dict[k[0]].keys()) & set(matrix_dict[k[1]].keys()))>=3) \
        .map(lambda k: (k,pearsonSim(k))).filter(lambda k: k[1] > 0).collect()
    # output model file
    with open(outputFile,'w+') as opf:
        for pair in business_pair:
            opf.writelines(json.dumps({"b1":business_dict_re[pair[0][0]],"b2":business_dict_re[pair[0][1]],"sim":pair[1]}) + '\n')
elif cf_type == 'user_based':
    # create 30 hash function, use big prime as p 
    n = 50
    m = len(user_list)
    p = 520381
    random.seed(10)
    a = random.sample(range(1,p),n)
    bias = random.sample(range(1,p),n)

    # list of tuples(user_id, [business_id1,business_id2,business_id3,........])
    table = input_data.map(lambda k: (k[0],k[1])).groupByKey().mapValues(lambda k: list(k)).filter(lambda k: len(k[1]) >= 3)
    table_dict = table.collectAsMap()
    #signature
    signature = table.mapValues(lambda k: [[((a[i] * j + bias[i]) % p) % m for j in k] for i in range(n)]).mapValues(lambda k: [min(a) for a in k])
    # LSH - n=30, r = 1, b = 30
    threshold = 0.01
    r = 1
    b = int(n/r)
    # get pair of ((#band,signature),[business_id that have that key])
    bands = signature.mapValues(lambda k: [tuple((i,tuple(k[r*i:r*i+r]))) for i in range(b)]).flatMap(lambda k: [(k[1][i],k[0]) for i in range(b)]) \
        .groupByKey()
    # filter the keys that have more than 1 business, and take out candidate pairs
    candidate = bands.map(lambda k: tuple(k[1])).filter(lambda k: len(k)>1).flatMap(lambda k: [tuple(sorted(cp)) for cp in combinations(k,2)]).distinct()

    """ # iterate candidate list to get result pair
    result = []
    for pair in candidate:
        s1 = set(table_dict[pair[0]])
        s2 = set(table_dict[pair[1]])
        Jsimilarity = len(s1&s2)/len((s1|s2))
        if Jsimilarity >= threshold:
            result.append(tuple((pair[0],pair[1]))) """
    # got pair above
    user_group = input_data.map(lambda k: (k[0],(k[1],k[2]))).groupByKey().filter(lambda k: len(list(k[1])) >= 3).map(lambda k: (k[0],list(k[1]))) \
        .mapValues(lambda k: listpair2dict(k))
    matrix_dict = user_group.collectAsMap()
    user_pair = candidate.filter(lambda k: JaccardSim(k)).filter(lambda k: numOfCoRated(k,3)).map(lambda k: (k,pearsonSim(k))).filter(lambda k: k[1] > 0).collect()
    # output model file
    with open(outputFile,'w+') as opf:
        for pair in user_pair:
            opf.writelines(json.dumps({"u1":user_dict_re[pair[0][0]],"u2":user_dict_re[pair[0][1]],"sim":pair[1]}) + '\n')


    """ user_group = input_data.map(lambda k: (k[0],(k[1],k[2]))).groupByKey().filter(lambda k: len(list(k[1])) >= 3).map(lambda k: (k[0],list(k[1]))) \
        .mapValues(lambda k: listpair2dict(k))
    matrix_dict = user_group.collectAsMap()
    # get pair
    user_candidate = user_group.map(lambda k: k[0]).collect()
    upair = list(combinations(user_candidate,2))
    ### !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!change back to 3 after test!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    user_pair = sc.parallelize(upair).filter(lambda k: numOfCoRated(k,3) and JaccardSim(k)) \
        .map(lambda k: (k,pearsonSim(k))).filter(lambda k: k[1] > 0).collect()
    # output model file
    with open(outputFile,'w+') as opf:
        for pair in user_pair:
            opf.writelines(json.dumps({"u1":user_dict_re[pair[0][0]],"u2":user_dict_re[pair[0][1]],"sim":pair[1]}) + '\n') """







print("Duration: %d s." % (time.time() - start))
#aaa = 1