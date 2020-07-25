from pyspark import SparkContext
import sys
import time
import collections
import json
import os

def listpair2dict(listpair):
    rdict = dict()
    for pair in listpair:
        rdict[pair[0]] = pair[1]
    return rdict

def predict(pair):
    user = pair[0]
    business = pair[1]
    ratelist = list(train1[user].items())
    # got [(run,win)]
    group = []
    for bs_pair in ratelist:
        group.append(tuple((bs_pair[1], model_dict.get(tuple(sorted((bs_pair[0], business))), 0))))
    useful = sorted(group, key=lambda k: k[1], reverse=True)[:5]    
    # calculate p
    up = 0
    down = 0
    for pair in useful:
        up += pair[0] * pair[1]
        down += abs(pair[1])
    if up == 0:
        return bus_avg.get(business_dict_re.get(business, "UNK"),3.823989)
    elif down == 0:
        return bus_avg.get(business_dict_re.get(business, "UNK"),3.823989)
    return up/down
    
def predict2(pair):
    user = pair[0]
    business = pair[1]
    ratelist = list(train1[business].items())
    # got [(rui,wau)]
    group = []
    for bs_pair in ratelist:
        avgr = user_avg.get(user_dict_re.get(bs_pair[0], "UNK"), 3.823989)
        group.append(tuple((bs_pair[1], avgr, model_dict.get(tuple(sorted((bs_pair[0], user))), 0))))
        
    useful = group   
    # calculate p
    up = 0
    down = 0
    for pair in useful:
        up += (pair[0] - pair[1]) * pair[2]
        down += abs(pair[2])
    if up == 0:
        return user_avg.get(user_dict_re.get(user, "UNK"),3.823989)
    elif down == 0:
        return user_avg.get(user_dict_re.get(user, "UNK"),3.823989)
    return (up/down) + user_avg.get(user_dict_re.get(user, "UNK"),3.823989)





start = time.time()

""" trainFile = "train_review.json"
testFile = "test_review.json"
modelFile = "task3user.model"
outputFile = "task3user.predict"
cf_type = "user_based" """

trainFile = sys.argv[1]
testFile = sys.argv[2]
modelFile = sys.argv[3]
outputFile = sys.argv[4]
cf_type = sys.argv[5]

folderName = os.path.dirname(os.path.abspath(trainFile))
""" url = os.path.abspath(trainFile)
(folderName,fileName) = os.path.split(url) """
userAvgFile = folderName + '/user_avg.json'
busAvgFile = folderName + '/business_avg.json'

sc = SparkContext()

train_data = sc.textFile(trainFile).map(lambda line: json.loads(line)).map(lambda k: ((k['user_id'],k['business_id']),k["stars"])) \
    .groupByKey().mapValues(lambda k: list(k)[0]).map(lambda k: (k[0][0],k[0][1],k[1]))
# sorted distinct user list(len=9169) and business list(len=5379)
user_list = train_data.map(lambda item: item[0]).distinct().sortBy(lambda item: item).collect()
business_list = train_data.map(lambda item: item[1]).distinct().sortBy(lambda item: item).collect()

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

if cf_type == 'item_based':
    # model dict --  (bus1,bus2): sim
    model_dict = sc.textFile(modelFile).map(lambda k: json.loads(k)).map(lambda k: (tuple(sorted((business_dict[k['b1']],business_dict[k['b2']]))),k['sim'])) \
        .collectAsMap()
    modelkeys = list(model_dict.keys())
    # test data --- pairs [(uid,bid),(uid,bid),.........]
    test_data = sc.textFile(testFile).map(lambda row: json.loads(row)).map(lambda k: (user_dict.get(k['user_id'],-1),business_dict.get(k['business_id'],-1))) \
        .filter(lambda k: k[0]>=0 and k[1]>=0)
    # avg info for business
    bus_avg = sc.textFile(busAvgFile).map(lambda row: json.loads(row)).collect()[0]
    # train data dict{user:{busin:score, busin:score, ............}}
    """ train1 = train_data.map(lambda k: (k[0],(k[1],k[2]))).groupByKey().map(lambda k: (user_dict[k[0]],list(k[1]))) \
        .mapValues(lambda k: [(business_dict[p[0]],p[1]) for p in k]) """
    train1 = train_data.map(lambda k: (user_dict[k[0]],business_dict[k[1]],k[2])).map(lambda k: (k[0],(k[1],k[2]))).groupByKey() \
        .map(lambda k: (k[0],list(k[1]))).mapValues(lambda k: listpair2dict(k)).collectAsMap()

    #prediction
    prediction = test_data.map(lambda k: (k,predict(k))).collect()
    #predict((20625,1716))
    # output model file
    with open(outputFile,'w+') as opf:
        for pair in prediction:
            opf.writelines(json.dumps({"user_id":user_dict_re[pair[0][0]],"business_id":business_dict_re[pair[0][1]],"stars":pair[1]}) + '\n')
elif cf_type == 'user_based':
    # model dict --  (user1,user2): sim
    model_dict = sc.textFile(modelFile).map(lambda k: json.loads(k)).map(lambda k: (tuple(sorted((user_dict[k['u1']],user_dict[k['u2']]))),k['sim'])) \
        .collectAsMap()
    modelkeys = list(model_dict.keys())
    # test data --- pairs [(uid,bid),(uid,bid),.........]
    test_data = sc.textFile(testFile).map(lambda row: json.loads(row)).map(lambda k: (user_dict.get(k['user_id'],-1),business_dict.get(k['business_id'],-1))) \
        .filter(lambda k: k[0]>=0 and k[1]>=0)
    # train data dict{busin:{user:score, user:score, ............}}
    train1 = train_data.map(lambda k: (user_dict[k[0]],business_dict[k[1]],k[2])).map(lambda k: (k[1],(k[0],k[2]))).groupByKey() \
        .map(lambda k: (k[0],list(k[1]))).mapValues(lambda k: listpair2dict(k)).collectAsMap()
    # avg info for business
    user_avg = sc.textFile(userAvgFile).map(lambda row: json.loads(row)).collect()[0]
    
    #prediction
    prediction = test_data.map(lambda k: (k,predict2(k))).collect()
    #predict2((20625,1716))
    # output model file
    with open(outputFile,'w+') as opf:
        for pair in prediction:
            opf.writelines(json.dumps({"user_id":user_dict_re[pair[0][0]],"business_id":business_dict_re[pair[0][1]],"stars":pair[1]}) + '\n')

print("Duration: %d s." % (time.time() - start))
#aa = 1