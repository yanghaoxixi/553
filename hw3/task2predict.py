from pyspark import SparkContext
import sys
import time
import json


def cosim(pair):
    user = user_profiles.get(str(pair[0]),'')
    bus = business_profiles.get(str(pair[1]),'')
    if user != '' and bus != '':
        up = len((set(user))&(set(bus)))
        down = (len(set(user))) ** 0.5 * (len(set(bus))) ** 0.5
        cos = up/down
        return tuple((pair,cos))
    else:
        return tuple((pair,0))

start = time.time()

""" testFile = 'test_review.json'
modelFile = 'task2.model'
outputFile = 'task2.predict' """
testFile = sys.argv[1]
modelFile = sys.argv[2]
outputFile = sys.argv[3]

sc = SparkContext()

model = sc.textFile(modelFile).map(lambda k: json.loads(k)).collect()
user_dict = model[0]['user_dict']
business_dict = model[1]['business_dict']
user_profiles1 = model[2]['user_profiles']
business_profiles1 = model[3]['business_profiles']
user_profiles = dict()
for item in user_profiles1:
    user_profiles.update(item)
business_profiles = dict()
for item in business_profiles1:
    business_profiles.update(item)

# prediction
prediction = sc.textFile(testFile).map(lambda row: json.loads(row)).map(lambda k: (user_dict.get(k['user_id'],-1),business_dict.get(k['business_id'],-1)))\
    .filter(lambda k: k[0]>=0 and k[1]>=0).map(lambda k: cosim(k)).filter(lambda k: k[1]>=0.01).collect()

user_dict_re = {}
for key, value in user_dict.items():
    user_dict_re[value] = key
business_dict_re = {}
for key, value in business_dict.items():
    business_dict_re[value] = key

# export
with open(outputFile,'w+') as opf:
    for pair in prediction:
        opf.writelines(json.dumps({"user_id":user_dict_re[pair[0][0]],"business_id":business_dict_re[pair[0][1]],"sim":pair[1]}) + '\n')



print("Duration: %d s." % (time.time() - start))
#aaa = 1