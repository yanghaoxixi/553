from pyspark import SparkContext
import time
import sys
import os
import itertools
import copy

def betweennessCal():
    result = dict()
    # loop for all 222 vertexes as root
    for node in vertex_list:
        # -----------create tree with node as root----------------------
        # tree save parents and layer of all nodes
        tree = dict()
        tree[node] = [] # on level 0, no parents
        # store nodes that have been gone
        flag = [node]
        lvlnow = [node] # now level
        lvlnext = [[node],[]]
        while lvlnow != []:
            posistion = lvlnow[0]
            lvlnow.remove(posistion)
            for child in edge_dict[posistion]:
                if child not in flag:
                    flag.append(child)
                    tree[child] = [posistion]
                    lvlnext[-1].append(child)
                elif child in lvlnext[-1]:
                    tree[child].append(posistion)
            if lvlnow == [] and lvlnext[-1] != []:
                lvlnow = copy.deepcopy(lvlnext[-1])
                lvlnext.append([])
        lvlnext = lvlnext[:-1]
        # now get [parent1, parent2, parent3, .....] for all nodes and list lvlnext for their level
        # -----------calculate credit starting at node----------------------
        #####--------calculate label, number of shortest path -------------
        label_dict = dict()
        for lvl in range(len(lvlnext)):
            for cn in lvlnext[lvl]:
                if len(tree[cn]) == 0:
                     label_dict[cn] = 1
                else:
                    label_dict[cn] = sum([label_dict[pn] for pn in tree[cn]])
        #####-------calculate credit for each edge-------------------------
        score_dict = dict()
        for cnn in vertex_list:
            score_dict[cnn] = 1
        for lvl in range(len(lvlnext))[::-1]:
            for c in lvlnext[lvl]:
                if len(tree[c]) > 0:
                    for p in tree[c]:
                        v = float(score_dict[c] * (label_dict[p]/label_dict[c]))
                        if tuple(sorted((c,p))) not in result.keys():
                            result[tuple(sorted((c,p)))] = v
                        else:
                            result[tuple(sorted((c,p)))] += v
                        score_dict[p] += v
    #-----------calculate for all node above---------------------
    # get true betweenness
    for item in result:
        result[item] /= 2
    result_list = sorted(result.items(), key=lambda k: (-k[1],k[0][0]))

    return result_list

def bestCommunity():
    edge_dict_ori = copy.deepcopy(edge_dict)
    btw = copy.deepcopy(btw_list)
    m = int(len(edge_list)/2)
    modularity_best = -999999999999
    community_list_best = []
    #modularity_list = [] #!!!!!!!!!!!!!!
    #i = 0 #!!!!!!!!!!!!!!
    bad_energy = 0
    while len(btw) > 1: # if modularity keep decreasing in 50 loops, stop
        """ if btw[0][1] == btw[1][1]:
            print("same happened!!!!! at i= " + str(i)) """
        
        # got highest betweenness pair, remove this edge in edge_dict
        highest_value = btw[0][1]

        for hb_candidate in btw:
            if hb_candidate[1] == highest_value:
                highest = hb_candidate[0]
                edge_dict[highest[0]].remove(highest[1])
                edge_dict[highest[1]].remove(highest[0])
            else: break

        
        # got communities list now
        community_list = []
        start_point = vertex_list[0]
        lvlnow = [start_point] # now level
        viewed = []
        lvlnext = [[start_point],[]]
        nextgroup = [start_point]
        while len(viewed) != len(vertex_list):
            viewed.append(lvlnow[0])
            while lvlnow != []:
                posistion = lvlnow[0]
                lvlnow.remove(posistion)
                for cns in edge_dict[posistion]:
                    if cns not in viewed:
                        viewed.append(cns)
                        lvlnext[-1].append(cns)
                        nextgroup.append(cns)
                if lvlnow == [] and lvlnext[-1] != []:
                    lvlnow = copy.deepcopy(lvlnext[-1])
                    lvlnext.append([])
            community_list.append(sorted(set(nextgroup)))       # update community_list
            left_points = list(set(vertex_list).difference(set(viewed)))
            if len(left_points) > 0:
                start_point = left_points[0]
                lvlnow = [start_point]
                lvlnext = [[start_point],[]]
                nextgroup = [start_point]
        
        """ # calculate modularity based on list of communities (update)
        MDTY_GS = 0
        for cmnt in community_list:
            for ni in cmnt:
                ki = len(edge_dict[ni])
                for nj in cmnt:
                    kj = len(edge_dict[nj])
                    if (ni,nj) in edge_list:
                        MDTY_GS += float(1-((ki * kj)/(2 * m)))
                    else:
                        MDTY_GS += float(0-((ki * kj)/(2 * m))) """

        # calculate modularity based on list of communities (no update)
        MDTY_GS = 0
        for cmnt in community_list:
            for ni in cmnt:
                ki = len(edge_dict_ori[ni])
                for nj in cmnt:
                    kj = len(edge_dict_ori[nj])
                    if (ni,nj) in edge_list:
                        MDTY_GS += float(1-((ki * kj)/(2 * m)))
                    else:
                        MDTY_GS += float(0-((ki * kj)/(2 * m)))
        
        # normalization modularity
        MDTY_GS /= (2*m)

        # update best result 
        #modularity_list.append(MDTY_GS) #!!!!!!!!!!!!!!
        #i = i+1 #!!!!!!!!!!!!!!
        #print(i) #!!!!!!!!!!!!!!
        if MDTY_GS >= modularity_best:
            community_list_best = community_list
            modularity_best = MDTY_GS
            bad_energy = 0
        else:
            bad_energy += 1
        # update betweenness
        btw = betweennessCal()
        

    # sort result
    result_community_list = sorted(community_list_best, key=lambda cluster: (len(cluster),cluster[0]))

    return result_community_list  #,modularity_list  #!!!!!!!!!!!!!!








start = time.time()

""" threshold = '7'
inputFile = 'ub_sample_data.csv'
outputFile_between = 'task2_between.txt'
outputFile_community = 'task2_community.txt' """
threshold = sys.argv[1]
inputFile = sys.argv[2]
outputFile_between = sys.argv[3]
outputFile_community = sys.argv[4]



sc = SparkContext()
sc.setLogLevel('ERROR')

# got data
input_data = sc.textFile(inputFile)
attri = input_data.take(1)[0]
#attri = 'user_id,business_id'
# remove header
data = input_data.filter(lambda row: row != attri).map(lambda k: k.split(',')) \
    .map(lambda k: (k[0],k[1])).groupByKey().filter(lambda k: len(list(k[1]))>=int(threshold)) \
        .mapValues(lambda k: sorted(k)).collectAsMap()
users_list = list(data.keys())
pairs_list = list(itertools.combinations(users_list,2))

#vertex list ['0FVcoJko1kfZCrJRfssfIA', 'qtOCfMTrozmUSHWIcohc6Q', ...]
# edge dict  {'0FMte0z-repSVWSJ_BaQTg': ['0FVcoJko1kfZCrJRfssfIA'], '0FVcoJko1kfZCrJRfssfIA': ['0FMte0z-repSVWSJ_BaQTg', '1KQi8Ymatd4ySAd4fhSfaw', ...}
edge_list = sc.parallelize(pairs_list).filter(lambda k: len(set(data[k[0]])&set(data[k[1]]))>=int(threshold)) \
    .flatMap(lambda k: (k,(k[1],k[0])))
vertex_list = edge_list.flatMap(lambda k: k).distinct().collect()
edge_dict = edge_list.groupByKey().mapValues(lambda k: sorted(k)).collectAsMap()
edge_list = edge_list.collect()

# calculate betweenness
btw_list = betweennessCal()

# output betweenness
with open(outputFile_between, 'w+') as opf:
    for item in btw_list:
        opf.writelines(str(item[0]) + ', ' + str(item[1]) + '\n')

# calculate Modularity to get best community
best_community_list_result = bestCommunity()

# output 
with open(outputFile_community, 'w+') as opf:
    for line in best_community_list_result:
        for content in line[:-1]:
            opf.writelines("'" + content + "', ")
        opf.writelines("'" + line[-1] + "'\n")





print("Duration: %d s." % (time.time() - start))
#aaa = 1