import sys
from pyspark import SparkContext
import time
import copy
import collections
from itertools import combinations
from operator import add

def candidateFilter(partition, support, datasize,bucketForBitmap=9999):
    ResultList = list()
    partitionData = list(partition)
    p = len(partitionData)/datasize
    supportPartition = support * p      #local threshold
    """ print(partitionData)
    print(supportPartition) """
    # Apriori pass 1 - find singleton:
    counter = collections.defaultdict(list)
    for transection in partitionData:
        for singleton in transection:       # couter for each singleton
            counter[singleton].append(1)
    # Apriori pass 1 - select items over support
    singlentonList = list()
    for item in counter:
        if len(counter[item]) >= supportPartition:
            singlentonList.append(item)
    singlentonList = sorted(singlentonList)        # real singleton
    ResultList += [(a,) for a in singlentonList]
    #print(singlentonList)
    # Apriori pass 2 - select frequent item-pair according to singleton
    counter = collections.defaultdict(list)
    for transection in partitionData:
        transection = sorted(set(transection)&set(singlentonList))
        if len(transection) >= 2:
            for pair in combinations(transection,2):
                counter[pair].append(1)
    pairList = list()
    for item in counter:
        if len(counter[item]) >= supportPartition:
            pairList.append(item)
    pairList = sorted(pairList)        # real pair
    ResultList += pairList    
    # Apriori pass k - for finding itemset k 
    kk = pairList # candidate of last size
    setsize = 2   # current size
    # update high level candadate list
    possibleCandidate = list()  # for next size of candidate list
    for i in range(len(kk)):
        for j in range(i+1,len(kk)):
            psb = tuple(sorted(set(kk[i])|set(kk[j])))
            if len(psb) != setsize + 1: break
            elif psb not in possibleCandidate:
                subsets = list()
                for pair in combinations(psb, setsize):
                    subsets.append(pair)
                if set(subsets).issubset(set(kk)):
                    possibleCandidate.append(psb)       
    kk = possibleCandidate   # candidate list of size 3
    setsize += 1
    #print(str(setsize) + '   kk updated')
    while kk != []:                     # until no possible candidate
        counter = collections.defaultdict(list)                         # count for each candidate
        for transection in partitionData:  
            for items in kk:
                if set(items).issubset(set(transection)):
                    counter[items].append(1)
                    
        pairList = list()                                           # determine if frequent
        for item in counter:
            if len(counter[item]) >= supportPartition:
                pairList.append(item)
        
        #pairList = sorted(pairList)        # real itemset
        ResultList += pairList
        kk = pairList
        #print(str(setsize) + '   pairList updated')
        if kk != []:
            possibleCandidate = list()  # for next size of candidate list
            for i in range(len(kk)):
                for j in range(i+1,len(kk)):
                    psb = tuple(sorted(set(kk[i])|set(kk[j])))
                    if len(psb) != setsize + 1: break
                    elif psb not in possibleCandidate:
                        subsets = list()
                        for pair in combinations(psb, setsize):
                            subsets.append(pair)
                        if set(subsets).issubset(set(kk)):
                            possibleCandidate.append(psb) 
            kk = possibleCandidate   # update  candidate list of size k+1
        setsize += 1
        #print(str(setsize) + '   kk updated')

    # way 2
    """flag = 1
                for pair in combinations(psb, setsize):
                    if pair not in kk:
                        flag = 0
                        break
                if flag == 0:break        
                else:possibleCandidate.append(psb) """
    yield ResultList
#---------------------------------------------------------------------------------------------------
def frequentFilter(partition, candidateList):
    result = list()
    partitionData = copy.deepcopy(list(partition))
    counter = collections.defaultdict(list)
    for candidate in candidateList:
        for transection in partitionData:
            if set(candidate).issubset(set(transection)):
                counter[candidate].append(1)
    for item in counter:
        result.append((item,len(counter[item])))

    yield result

startTime = time.time()   # star time

filterThreshold = sys.argv[1] 
support = sys.argv[2]
inputFile = sys.argv[3]
outputFile = sys.argv[4]
""" filterThreshold = '70'  
support = '50'
inputFile = 'user_business.csv'
outputFile = 'output_task2.txt' """


# got data
sc = SparkContext()
#sc.setLogLevel('ERROR')
#sc.setLogLevel('DEBUG')
dataOri = sc.textFile(inputFile)#.zipWithIndex().filter(_._2>=1).keys
attri = dataOri.take(1)[0]
data = dataOri.filter(lambda row: row != attri)

# got basket dataset
mbm = data.map(lambda item: (item.split(',')[0],item.split(',')[1])).groupByKey().map(lambda key: (key[0], sorted(set(key[1]))))
# mission 2: find out qualified users
mbm_f = mbm.filter(lambda item: len(item[1]) > int(filterThreshold)).map(lambda item: item[1])
#aaa = mbm.collect()
datasize = mbm_f.count()
# mission 3: SON
# SON phase 1 MAP
mbm_MAP1 = mbm_f.mapPartitions(lambda partition: candidateFilter(partition, int(support), datasize))
# SON phase 1 Reduce
mbm_REDUCE1 = mbm_MAP1.flatMap(lambda item: item).distinct().collect()
# sort and get Candidates list
mbm_REDUCE1.sort(key=lambda t: (len(t),t))
candidate = copy.deepcopy(mbm_REDUCE1)

# SON phase 2 MAP   (Got key-value pair (C,v), C is a candidate frequent itemset and v is the support for that itemset)
mbm_MAP2 = mbm_f.mapPartitions(lambda partition: frequentFilter(partition, candidate))
# SON phase 2 Reduce
mbm_REDUCE2 = mbm_MAP2.flatMap(lambda item: item).reduceByKey(add).filter(lambda item: item[1] >= int(support)).map(lambda item: item[0]).collect()
# sort and get Frequent list
mbm_REDUCE2.sort(key=lambda t: (len(t),t))
frequent = copy.deepcopy(mbm_REDUCE2)


# output
with open(outputFile, 'w+') as opf:
    flag = 1    # now length
    content = 'Candidate:\n'
    for item in candidate:
        if len(item) == 1:
            content += "('"+item[0]+"'),"
        elif len(item) != flag:      # length change position
            content = content[:-1] + '\n\n'
            flag += 1
            content += (str(item)+',')
        else:
            content += (str(item)+',')
    content = content[:-1] + '\n\n'
    content += 'Frequent Itemsets:\n'
    flag = 1
    for item in frequent:
        if len(item) == 1:
            content += "('"+item[0]+"'),"
        elif len(item) != flag:      # length change position
            content = content[:-1] + '\n\n'
            flag += 1
            content += (str(item)+',')
        else:
            content += (str(item)+',')
    content = content[:-1]

    opf.write(content)

print("Duration: %d " % (time.time() - startTime))
#a = 1