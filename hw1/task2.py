import sys
import json
from pyspark import SparkContext


reviewFile = 'review.json'
businessFile = 'business.json'
outputFile = 'nospar_output'
ifSpark = 'no_spark'
n = '20'




""" reviewFile = sys.argv[1]
businessFile = sys.argv[2]
outputFile = sys.argv[3]
ifSpark = sys.argv[4]
n = sys.argv[5] """

result = dict()

if ifSpark == 'spark':
    # input content
    sc = SparkContext()

    # review: (id,star), business:(id,categories)
    review = sc.textFile(reviewFile).map(lambda item: json.loads(item)).map(lambda item: (item['business_id'],item['stars'])).map(lambda item: (item[0],float(item[1])))
    business = sc.textFile(businessFile).map(lambda item: json.loads(item)).map(lambda item: (item['business_id'],item['categories']))

    # review: (id,(sum_star,count)), business:(id,categories list)
    review = review.groupByKey().map(lambda item: (item[0],(sum(item[1]),len(item[1]))))
    business = business.filter(lambda item: item[0] is not None and item[0] != '' and item[1] is not None and item[1] != ''). \
        mapValues(lambda item: [a.strip() for a in item.split(',')])
        #.mapValues(lambda item: item.strip()).collect()

    # joinTable: (id,(cate list,(sum,count)))
    joinTable = business.leftOuterJoin(review)

    # computing avg star for each categories and sort
    joinTable = joinTable.filter(lambda k: k[1][1] is not None).map(lambda item: item[1]).flatMap(lambda item: [(cate, item[1]) for cate in item[0]]).\
        reduceByKey(lambda x, y: (x[0]+y[0],x[1]+y[1])).mapValues(lambda item: item[0]/item[1]).map(lambda k: ((-k[1],k[0]),1)). \
            sortByKey().map(lambda k: [k[0][1],-k[0][0]])
    result['result'] = joinTable.take(int(n))
if ifSpark == 'no_spark':
    # review list: list of [id, star]
    review_list = []
    with open(reviewFile, 'r',encoding='utf8') as rf:
        line = rf.readlines()
        for row in line:
            jrow = json.loads(row)
            review_list.append([jrow['business_id'],jrow['stars']])
    # business dictionary: {id:cate list}
    business_dict = dict()
    with open(businessFile, 'r',encoding='utf8') as rf:
        line = rf.readlines()
        for row in line:
            jrow = json.loads(row)
            categorieslist = jrow['categories']
            if categorieslist is not None:
                categorieslist = [cate.strip() for cate in categorieslist.split(',')]
                business_dict[jrow['business_id']] = categorieslist
    # got cate:(sum,count) 
    score_dict = dict()
    for record in review_list:
        bsnid = record[0]
        star = float(record[1])
        if bsnid in business_dict.keys():
            catel = business_dict[bsnid]
            for ctgy in catel:
                if ctgy in score_dict.keys():
                    score_dict[ctgy] = (score_dict[ctgy][0]+star, score_dict[ctgy][1]+1)
                else:
                    score_dict[ctgy] = (star,1)
    # compute avg
    for key in score_dict:
        score_dict[key] = score_dict[key][0]/score_dict[key][1]
    # sort
    sorted_score = sorted(score_dict.items(), key=lambda kv: [-kv[1], kv[0]])[:int(n)]
    for i in range(len(sorted_score)):
        sorted_score[i] = list(sorted_score[i])
    result['result'] = sorted_score

# write output
with open(outputFile,'w+') as opf:
    json.dump(result, opf)



#a = 1
