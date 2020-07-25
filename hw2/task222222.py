import sys
from pyspark import SparkContext,SparkConf
from operator import add
import json
from itertools import combinations
from functools import reduce
import re

import time
start = time.time()

def printf(p):
    print(list(p))

def PCY(x,support,partition):
    def frequent_single_item(b,threshold):
        single_item = []
        frequent_item = {}

        for each in b:
            for i in combinations(each,1):
                if i in frequent_item:
                    frequent_item[i] = frequent_item[i]+1
                else:
                    frequent_item[i] = 1

        for j in frequent_item:
            if frequent_item[j]>=threshold:
                single_item.append(tuple(j))
        return single_item

    def select(a,b,threshold):
        #a is candidate
        #b is rowdata
        pair_dict ={}
        pair_list =[]
        for j in b:
            for i in a:
                if set(list(i)).issubset(j):
                    i = tuple(sorted(i))
                    if i in pair_dict:
                        pair_dict[i] = pair_dict[i] + 1
                    else:
                        pair_dict[i] = 1

        for i in pair_dict:
            if pair_dict[i] >= threshold:
                pair_list.append(i)
        return pair_list
    # def select(a,b,threshold):
    #     #a is candidate
    #     #b is rowdata
    #     pair_dict ={}
    #     pair_list =[]
    #     ano_list = []
    #     for i in a:
    #         for j in b:
    #             if set(list(i)).issubset(j):
    #                 break
    #         ano_list.append(i)
    #     a = ano_list
    #     print("第一阶段")
    #     print(len(a))
    #     for i in a:
    #         i = frozenset(i)
    #         for j in b:
    #             if i in pair_list:
    #                 break
    #             if set(list(i)).issubset(j):
    #                 i = tuple(sorted(i))
    #                 if i in pair_dict:
    #                     pair_dict[i] = pair_dict[i] + 1
    #                 else:
    #                     pair_dict[i] = 1
    #
    #                 if pair_dict[i] >= threshold:
    #                     pair_list.append(i)
    #         continue
    #     return pair_list


    def get_more_two(pair,data,s):
        set_number = 3
        old_data = pair
        new_list= []
        while len(old_data)>0:
            tp_list = []
            for i in old_data:
                for j in old_data:
                    new = tuple(sorted(list(set(i)|set(j))))
                    if len(new) == set_number:
                        tp_list.append(new)
            tp_list = list(set(list(tp_list)))
            ser_f = select(tp_list,data,s)
            new_list.extend(ser_f)
            old_data = ser_f
            set_number += 1
        return new_list

    def get_f_pair(d,threshold):
        bitmap ={}
        total_frequent = []
        for k in d:
            for i in combinations(list(sorted(k)), 2):
                if i in bitmap:
                    bitmap[i] = bitmap[i] + 1
                else:
                    bitmap[i] = 1

        for j in bitmap:
            if bitmap[j] >= threshold:
                total_frequent.append(j)
        return total_frequent



    s = support/partition
    total_list = []
    single_f_item = frequent_single_item(x,s)
    print(len(single_f_item))
    # print(single_f_item)
    #change to normal number
    new_single_list = []
    for i in single_f_item:
        new_single_list.append(i[0])
    # all_pair = list(combinations(new_single_list,2))
    # pair_f_item = select(all_pair,x,s)
    pair_f_item = get_f_pair(x,s)
    more_two = get_more_two(pair_f_item,x,s)
    total_list.extend(single_f_item)
    total_list.extend(pair_f_item)
    total_list.extend(more_two)
    print(pair_f_item)

    return total_list

def total_candidate2(e,candidate):
    total_list = []
    for i in candidate:
        for j in e:
            if set(list(i)).issubset(set(j)):
                total_list.append(i)
    return total_list

def trans_data(final1,final2):
    file1 = {}
    file2 = {}
    for i in final1:
        lenth = len(i)
        if lenth in file1:
            file1[lenth].append(tuple(sorted(i)))
        else:
            file1[lenth] = [tuple(sorted(i))]
    for i in file1:
        file1[i] = sorted(file1[i])

    for i in final2:
        lenth = len(i)
        if lenth in file2:
            file2[lenth].append(tuple(sorted(i)))
        else:
            file2[lenth] = [tuple(sorted(i))]

    for i in file2:
        file2[i] = sorted(file2[i])


    data = {}
    data2 = {}
    result  = ""
    for lenth in file1:
        for i in file1[lenth]:
            if lenth == 1:
                if lenth in data:
                    data[lenth] = str(data[lenth])+","+str(i).replace(",","")
                else:
                    data[lenth] = str(i).replace(",","")
            else:
                if lenth in data:
                    data[lenth] = str(data[lenth]) + "," +str(i)
                else:
                    data[lenth] = str(i)
    for j in data:
        if j == 1:
            result = "Candidates:\n" + data[j] + "\n\n"
        else:
            result = result + data[j] + "\n\n"

    for lenth in file2:
        for i in file2[lenth]:
            if lenth == 1:
                if lenth in data2:
                    data2[lenth] = str(data2[lenth])+","+str(i).replace(",","")
                else:
                    data2[lenth] = str(i).replace(",","")
            else:
                if lenth in data2:
                    data2[lenth] = str(data2[lenth]) + "," +str(i)
                else:
                    data2[lenth] = str(i)
    for j in data2:
        if j == 1:
            result =result + "Frequent Itemsets:\n" + data2[j] + "\n\n"
        else:
            result = result + data2[j] + "\n\n"
    result=result[0:-2]

    with open(output_file_path,"w") as output_file:
        output_file.write(result)
        output_file.close()


filter_threshold = 70
support = 50
input_file_path = "user_business.csv"
output_file_path = "output_file1.txt"

# filter_threshold = int(sys.argv[1])
# support = int(sys.argv[2])
# input_file_path = sys.argv[3]
# output_file_path = sys.argv[4]


sc = SparkContext(appName="inf553")
file_raw = sc.textFile(input_file_path)
head = file_raw.first()
file = file_raw.filter(lambda s: s!=head)
divide_file = file.map(lambda s: s.split(",")).map(lambda s: [s[0], s[1]]).repartition(2)
unique_id_file = divide_file.groupByKey().map(lambda s: [s[0],list(s[1])]).filter(lambda s: len(s[1])>=filter_threshold)
total_pair = unique_id_file.map(lambda s: sorted(s[1])).persist()
count_number = total_pair.getNumPartitions()
# phase1
frequent_candidate = total_pair.mapPartitions(lambda s: PCY(list(s),support,count_number)).distinct().collect()
print(len(frequent_candidate))
# phase2
phase2_total_candidate = total_pair.mapPartitions(lambda s: total_candidate2(list(s),frequent_candidate)).collect()\
    #.map(lambda s: (s,1)).reduceByKey(add).filter(lambda s: s[1]>=support).map(lambda s: s[0]).collect()
trans_data(frequent_candidate,phase2_total_candidate)
end = time.time()
print ("Duration: %s Seconds"%(end-start))