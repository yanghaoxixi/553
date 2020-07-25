from pyspark import SparkContext
import sys
import time
import os
import json
import csv
import random
from collections import defaultdict
import copy
from itertools import combinations

def MergeToDS(point, stat):
    shortest_md = 9999999999
    for cluster in stat.keys():
        N1 = stat[cluster][0]
        ctoid1 = [SUM/N1 for SUM in stat[cluster][1]]
        sdev1 = [(stat[cluster][2][i]/N1)-(ctoid1[i]**2) for i in range(dimension)]
        md1 = computeMahaDistance(point, ctoid1, sdev1)
        if md1 < shortest_md:
            shortest_md = md1
            belongs_to = cluster
    
    return belongs_to


def GroupUpDicts(dict1,dict2):
    dict_result = dict()
    i = 0
    for value in dict1.values():
        dict_result[i] = value
        i += 1
    for value in dict2.values():
        dict_result[i] = value
        i += 1
    return dict_result

def MergeCS(points1,stats1):
    points = copy.deepcopy(points1)
    stats = copy.deepcopy(stats1)
    key_list = list(points.keys())
    flag = 0
    for pair in combinations(key_list,2):
        key1 = pair[0]
        key2 = pair[1]
        N1 = stats[key1][0]
        N2 = stats[key2][0]
        ctoid1 = [SUM/N1 for SUM in stats[key1][1]]
        ctoid2 = [SUM/N2 for SUM in stats[key2][1]]
        sdev1 = [(stats[key1][2][i]/N1)-(ctoid1[i]**2) for i in range(dimension)]
        sdev2 = [(stats[key2][2][i]/N2)-(ctoid2[i]**2) for i in range(dimension)]
        md1 = computeMahaDistance(ctoid1, ctoid2, sdev2)
        md2 = computeMahaDistance(ctoid2, ctoid1, sdev1)
        if md1 < alpha * (dimension ** 0.5) and md2 < alpha * (dimension ** 0.5):
            points[key1] = points[key1] + points[key2]
            stats[key1][0] = stats[key1][0] + stats[key2][0]
            stats[key1][1] = [stats[key1][1][i] + stats[key2][1][i] for i in range(dimension)]
            stats[key1][2] = [stats[key1][2][i] + stats[key2][2][i] for i in range(dimension)]
            del points[key2]
            del stats[key2]
            flag = 1
            points, stats = MergeCS(points,stats)
        if flag == 1:
            break


    return points, stats


def computeMahaDistance(point_list, ctoid_list, sdev):
    sumsum = 0
    for i in range(dimension):
        if sdev[i] > 0:
            sumsum += (((point_list[i]-ctoid_list[i]) ** 2)/sdev[i])
        elif abs(point_list[i] - ctoid_list[i]) > 1e-5:
            sumsum += 9999999
    Mdistance = sumsum ** 0.5
    
    return Mdistance



def computeDistance(list1,list2):
    dimension = len(list1)
    dist = 0
    for dim in range(dimension):
        dist += (list1[dim] - list2[dim]) ** 2
    dist = dist ** 0.5

    return dist


def Kmeans_initialize(data, k):
    num_points = len(data.keys())
    num_sample = int(num_points * 0.1)
    centroid_dict = dict()              # record centroid infomation: {0:[1,2,3,4,5,6,7...], 1:[1,2,3,4,5,6,....]}
    random.seed(12)
    c1 = list(data.keys())[random.randint(0,num_sample)]
    centroid_dict[0] = data[c1]         # pick first centroid
    for i in range(1,k):                # pick 2~k th centroids
        shortest_length = -1
        for point in list(data.keys())[:num_sample+1]:
            distance1 = 9999999999999
            for centroid in centroid_dict.keys():
                distance = computeDistance(data[point],centroid_dict[centroid])
                if distance < distance1:
                    distance1 = distance
            if distance1 > shortest_length:
                shortest_length = distance1
                next_centroid = data[point]
        centroid_dict[i] = next_centroid
    return centroid_dict

def Kmeans_implement(data, centroid_dict, taking_outliner=False, taking_CS=False):
    #n_clu = len(centroid_dict.keys())
    #centroid_dict = copy.deepcopy(centroid_dict_p)
    if taking_outliner:
        stop_threshold = 50
        outliner = list()
    else:
        outliner = list()
        cluster_statistic = defaultdict(list)
        stop_threshold = 1

    bad_energy = 0
    epoch = 0
    while True:
        cluster_result = defaultdict(list)
        # assign all points to nearest cluster
        for point in data.keys():
            shortest_length = 999999999
            belongs_to = -1
            for centroid in centroid_dict.keys():
                distance = computeDistance(data[point],centroid_dict[centroid])
                if distance < shortest_length:
                    belongs_to = centroid
                    shortest_length = distance
            cluster_result[belongs_to].append(point)
        # update centroid infomation
        old_centroids = copy.deepcopy(centroid_dict)
        for centroid in centroid_dict.keys():
            points_in_cluster = cluster_result[centroid]
            if len(points_in_cluster) == 0:
                del cluster_result[centroid]
            else:
                centroid_dict[centroid] = [sum([data[point][i] for point in points_in_cluster])/len(points_in_cluster) for i in range(dimension)]   # SUM/N
        # determine if centroids move
        sumsum = 0
        for centroid in centroid_dict.keys():
            sumsum += computeDistance(centroid_dict[centroid],old_centroids[centroid])
        epoch += 1
        # if no more new outliner in 2 loop
        if taking_outliner:
            outliner_temp = list()
            for centroid, point_list in cluster_result.items():
                if len(point_list) <= 10:
                    for oler in point_list:
                        outliner_temp.append(oler)
            if outliner_temp != outliner:
                outliner = outliner_temp
                bad_energy = 0
            else:
                bad_energy += 1
    
        # if centroids move short
        if sumsum < stop_threshold or bad_energy >= 2 or epoch >= 30:
            break
        


    if taking_outliner:
        # save outliner to RS
        """ for olt in outliner:
            RS.append(data[olt]) """
        return outliner
    elif not taking_CS:
        flag = 0
        for centroid in centroid_dict.keys():
            points_in_cluster = cluster_result[centroid]
            cluster_statistic[centroid] = [len(points_in_cluster), 
                [sum([data[point][i] for point in points_in_cluster]) for i in range(dimension)], 
                [sum([data[point][i] ** 2 for point in points_in_cluster]) for i in range(dimension)]]

        for line in cluster_result.values():
            if len(line) <= 10:
                flag = 1
                for item in line:
                    outliner.append(item)

    if taking_CS:
        dele = []
        for clu, lst in cluster_result.items():
            if len(lst) == 1:
                outliner.append(lst[0])
                dele.append(clu)
            else:
                points_in_cluster = cluster_result[clu]
                cluster_statistic[clu] = [len(points_in_cluster), 
                    [sum([data[point][i] for point in points_in_cluster]) for i in range(dimension)], 
                    [sum([data[point][i] ** 2 for point in points_in_cluster]) for i in range(dimension)]]
        for item in dele:
            del cluster_result[item]
        flag = 2





    if flag == 0:
        return cluster_result, cluster_statistic, True
    if flag == 1:
        return outliner,{}, False
    if flag == 2:
        R_return = defaultdict(list)
        for pid in outliner:
            R_return[pid] = data[pid]
        return cluster_result, cluster_statistic, R_return



def assignWhich_S(data, S_stat): # for point k, compute Mahalanobis distance to determine which DS it should go
    result_list = []
    for point_key, point in data.items():

        shortest_distance = 99999999
        for cluster_key, cluster_stat in S_stat.items():
            N = cluster_stat[0]
            ctoid = [SUM/N for SUM in cluster_stat[1]]
            sdev = [(cluster_stat[2][i]/N)-(ctoid[i]**2) for i in range(dimension)]
            # compute Mahalanobis distance of point and cluster
            Mdistance = computeMahaDistance(point, ctoid, sdev)

            if Mdistance < shortest_distance:
                shortest_distance = Mdistance
                belongs_to = cluster_key
        
        # determine if need to join DS cluster
        if shortest_distance < alpha * (dimension ** 0.5):
            result_list.append(tuple((point_key,belongs_to)))
        else:
            result_list.append(tuple((point_key,-1)))
    return result_list


#os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3.6'
#os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3.6'

start = time.time()

""" document_num = "4"
inputFolder = './data/test' + document_num
n_cluster_str = '8'
outputFile_cluster = 'output_cluster' + document_num + '.json'
outputFile_inter = 'output_intermediate' + document_num + '.csv' """

inputFolder = sys.argv[1]
n_cluster_str = sys.argv[2]
outputFile_cluster = sys.argv[3]
outputFile_inter = sys.argv[4]

n_cluster = int(n_cluster_str)

sc = SparkContext()
#sc.setLogLevel("ERROR")

# create intermediate file
with open(outputFile_inter,'w') as opf:
    opf.write("round_id,nof_cluster_discard,nof_point_discard,nof_cluster_compression,nof_point_compression,nof_point_retained\n")


# got input file in folder
round_id = 1  # if the first file
alpha = 2


for file_input in sorted(os.listdir(inputFolder)):
    inputFile = inputFolder + '/' + file_input
    
    input_data_rdd = sc.textFile(inputFile).map(lambda k: (k.split(',')[0],k.split(',')[1:])) \
        .map(lambda k: (int(k[0]),[float(i) for i in k[1]]))
    """ input_data_dict = input_data_rdd.collectAsMap()
    dimension = len(list(input_data_dict.values())[0]) """

    # processing data
    if round_id == 1:
        input_data_dict = input_data_rdd.collectAsMap()
        dimension = len(list(input_data_dict.values())[0])


        # initialize 3k centroid for kmeans using kmeans++
        centroid_dict_ini = Kmeans_initialize(input_data_dict, 2 * n_cluster)
        # using kmeans to get 3k cluster for detecting outliner
        outliners_list = Kmeans_implement(input_data_dict, centroid_dict_ini, taking_outliner=True) 

        good = False
        while not good:
            input_data_no_outliner_dict = input_data_rdd.filter(lambda k: k[0] not in outliners_list).collectAsMap()
            # Real Kmeans for clustering
            centroid_dict_ini = Kmeans_initialize(input_data_no_outliner_dict, n_cluster)
            DS_point, DS_stat, good = Kmeans_implement(input_data_no_outliner_dict, centroid_dict_ini)
            if not good:                                    # if still have outlier in this data set, update outlier and do step 4 again 
                for item in DS_point:
                    outliners_list.append(item)
        
        # step 5, distinguish CS and RS
        if len(outliners_list) <= 2 * n_cluster:
            RS = defaultdict(list)
            for p in outliners_list:
                RS[p] = input_data_dict[p]
            CS_point = defaultdict(list)
            CS_stat = defaultdict(list)
        else:
            input_data_outliner_dict = input_data_rdd.filter(lambda k: k[0] in outliners_list).collectAsMap()
            centroid_dict_ini = Kmeans_initialize(input_data_outliner_dict, 2 * n_cluster)
            CS_point, CS_stat, RS = Kmeans_implement(input_data_outliner_dict, centroid_dict_ini, taking_CS=True)
        
        # write intermediate file
        nof_point_discard1 = sum([len(cl) for cl in DS_point.values()])
        nof_point_compression1 = sum([len(cl) for cl in CS_point.values()])

        with open(outputFile_inter,'a') as opf:
            opf.write(str(round_id)+','+str(len(DS_point.keys()))+','+str(nof_point_discard1)+','+str(len(CS_point.keys()))+','+str(nof_point_compression1)+','+str(len(RS.keys()))+'\n')

        round_id += 1
    else:       # for the rest n-1 documents
        input_data_dict = input_data_rdd.collectAsMap()
        assign_list_DS = assignWhich_S(input_data_dict, DS_stat)

        # assign to DS, then we got bigger DS from new file and outliners, which not belongs to any DS from new file, maybe CS or RS
        outliners_list = []
        for pair in assign_list_DS: # pair = (point_id, cluster_id)
            if pair[1] >= 0:
                DS_point[pair[1]].append(pair[0])
                DS_stat[pair[1]][0] += 1
                DS_stat[pair[1]][1] = [DS_stat[pair[1]][1][i] + input_data_dict[pair[0]][i] for i in range(dimension)]
                DS_stat[pair[1]][2] = [(DS_stat[pair[1]][2][i] + (input_data_dict[pair[0]][i] ** 2)) for i in range(dimension)]
            else:
                outliners_list.append(pair[0])

        # step 8, assign to CS
        if len(list(CS_stat.keys())) > 0:
            input_data_outliner_dict = input_data_rdd.filter(lambda k: k[0] in outliners_list).collectAsMap()
            assign_list_CS = assignWhich_S(input_data_outliner_dict, CS_stat)

            for pair in assign_list_CS: # pair = (point_id, cluster_id)
                if pair[1] >= 0:
                    CS_point[pair[1]].append(pair[0])
                    CS_stat[pair[1]][0] += 1
                    CS_stat[pair[1]][1] = [CS_stat[pair[1]][1][i] + input_data_outliner_dict[pair[0]][i] for i in range(dimension)]
                    CS_stat[pair[1]][2] = [(CS_stat[pair[1]][2][i] + (input_data_outliner_dict[pair[0]][i] ** 2)) for i in range(dimension)]
                else:
                    RS[pair[0]] = input_data_outliner_dict[pair[0]]
        
        else:   # have not CS yet
            for pid in outliners_list:
                RS[pid] = input_data_dict[pair[0]]

        # then we have only RS left, Step 10, Run Kmeans to all RS we have. to got new CS
        if len(RS.keys()) > 2 * n_cluster:
            centroid_dict_ini = Kmeans_initialize(RS, 2 * n_cluster)
            CS_point_new, CS_stat_new, RS = Kmeans_implement(RS, centroid_dict_ini, taking_CS=True)
            # group up CS and CS_new
            CS_point = GroupUpDicts(CS_point,CS_point_new)
            CS_stat = GroupUpDicts(CS_stat,CS_stat_new)

        # Step 11, merge close CS's
        if len(CS_stat.keys()) >= 2:
            CS_point, CS_stat = MergeCS(CS_point,CS_stat)
        
        # Step 12 output
        nof_point_discard1 = sum([len(cl) for cl in DS_point.values()])
        nof_point_compression1 = sum([len(cl) for cl in CS_point.values()])

        with open(outputFile_inter,'a') as opf:
            opf.write(str(round_id)+','+str(len(DS_point.keys()))+','+str(nof_point_discard1)+','+str(len(CS_point.keys()))+','+str(nof_point_compression1)+','+str(len(RS.keys()))+'\n')
        
    
        #print(round_id)
        #print("Duration: %d s." % (time.time() - start))

        round_id += 1


# merge CS and RS into DS
if len(list(CS_stat.keys())) > 0:   
    for keyi in CS_stat.keys():
        N = CS_stat[keyi][0]
        centroid_CS = [SUM/N for SUM in CS_stat[keyi][1]]
        taget_cluster = MergeToDS(centroid_CS, DS_stat)
        DS_point[taget_cluster] += CS_point[keyi]
if len(list(RS.keys())) > 0:   
    for keyi, valuei in RS.items():
        taget_cluster = MergeToDS(valuei, DS_stat)
        DS_point[taget_cluster].append(keyi)

# write result
result = dict()
for key, value in DS_point.items():
    for point_id in value:
        result[point_id] = key

with open(outputFile_cluster, 'w+') as opf:
    opf.writelines(json.dumps(result))





print("Duration: %d s." % (time.time() - start))
#aaaaa = 1