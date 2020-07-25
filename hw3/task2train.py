from pyspark import SparkContext
import sys
import time
import json
import string
from operator import add
import math


def gotWordList(textlist):
    word_list = []
    delel_list = '\n'+string.digits+string.punctuation
    for text in textlist:
        text = text.replace("'",'').translate(str.maketrans(delel_list,'                                           ')).lower().split()
        word_list += list(filter(lambda word: word not in stopwords and len(word) > 1, text))
    return word_list
def countWord_TF(wlist):
    counter = dict()
    for word in wlist:
        counter[word] = counter.setdefault(word,0) + 1
    Maxfrequency = counter[max(counter,key=counter.get)]
    counter = dict(filter(lambda k: k[1] > 3, counter.items()))  # filter, take >3
    for item in counter:
        counter[item] /= Maxfrequency 
    return list(counter.items())
def newlist(buslist):
    for bus in buslist:
        b = []
        if business_word200.get(bus,'') != '':
            b.extend(business_word200.get(bus,[]))
    return b

start = time.time()

""" inputFile = "train_review.json"
outputFile = "task2.model"
stopwordFile = "stopwords" """
inputFile = sys.argv[1]
outputFile = sys.argv[2]
stopwordFile = sys.argv[3]

# get stopwords
with open(stopwordFile,'r') as sw:
    stopwords = set()
    for word in sw:
        stopwords.add(word.strip('\n'))

sc = SparkContext()
# read data
input_data = sc.textFile(inputFile).map(lambda line: json.loads(line))

# sorted distinct user list(len=9169) and business list(len=5379)
user_list = input_data.map(lambda item: item["user_id"]).distinct().sortBy(lambda item: item).collect()
business_list = input_data.map(lambda item: item["business_id"]).distinct().sortBy(lambda item: item).collect()

# give index to users and businesses based on their order in character
user_dict = {}
for index, item in enumerate(user_list):
    user_dict[item] = index
business_dict = {}
for index, item in enumerate(business_list):
    business_dict[item] = index

# concatenating all the review texts and transform into word list. Example (5306,["fries","hello","dad"......])
business_text_data = input_data.map(lambda k: (business_dict[k['business_id']],k['text'])).groupByKey().mapValues(lambda v: gotWordList(list(v)))
# count word, calculate TF. Example (5306, [('second', 0.125), ('time', 0.375), ('ive', 0.25), ('first', 0.25), ('whatever', 0.125), ('actually', 0.25)....])
business_TF = business_text_data.mapValues(lambda k: countWord_TF(k))
# calculate IDF [((0, 'good'), 2.6961547473552443),...................]
business_IDF = business_TF.flatMap(lambda k: [(pair[0],k[0]) for pair in k[1]]).groupByKey()\
    .map(lambda k: (k[0],list(set(k[1])),len(list(set(k[1]))))).flatMap(lambda k: [((business,k[0]),math.log((len(business_dict)/k[2]),2)) for business in k[1]])
# calculate TFIDF
business_TFIDF = business_TF.flatMap(lambda k: [((k[0],pair[0]),pair[1]) for pair in k[1]]).leftOuterJoin(business_IDF) \
    .map(lambda k: (k[0][0],(k[0][1],k[1][0]*k[1][1])))
# take top 200 as profiles
business_word200 = business_TFIDF.groupByKey().mapValues(lambda k: sorted(list(k),key=lambda i: i[1], reverse=True)[:200])
business_word_rank = business_word200.map(lambda k: (k[0],[word[0] for word in k[1]]))
# word dictionary
word_list = []
for line in business_word_rank.collect():
    for word in line[1]:
        if word not in word_list:
            word_list.append(word)
word_dict = {}
for index, item in enumerate(word_list):
    word_dict[item] = index
# real business profiles
business_profiles = business_word_rank.map(lambda k:{k[0]:[word_dict[word] for word in k[1]]}).collect()
# user profiles
business_word200 = business_word200.collectAsMap()
user_profiles = input_data.map(lambda k: (user_dict[k['user_id']],business_dict[k['business_id']])).groupByKey() \
    .mapValues(lambda k: newlist(list(k))).mapValues(lambda k: sorted(list(k),key=lambda i: i[1], reverse=True)[:200]).map(lambda k: (k[0],[word[0] for word in k[1]])) \
        .filter(lambda k: k[1] != []).map(lambda k:{k[0]:[word_dict[word] for word in k[1]]}).collect()
    #.mapValues(lambda k: list(k)).mapValues(lambda k: [business_word200.get(bus,[]) for bus in k]).collect()

# output model file
with open(outputFile,'w+') as opf:
    opf.writelines(json.dumps({'user_dict':user_dict}) + '\n')
    opf.writelines(json.dumps({'business_dict':business_dict}) + '\n')
    opf.writelines(json.dumps({'user_profiles':user_profiles}) + '\n')
    opf.writelines(json.dumps({'business_profiles':business_profiles}) + '\n')



# flatMap(lambda k: [tuple(((k[0],word),1)) for word in k[1]]).reduceByKey(add).map(lambda k: (k[0][0],(k[0][1],k[1]))).groupByKey()


print("Duration: %d s." % (time.time() - start))
#aaa = 1