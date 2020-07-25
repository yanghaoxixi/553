from itertools import combinations
import collections
from pyspark import SparkContext


sc = SparkContext()

a = [1,2,3,4,5,6,7]
aa = sc.parallelize(a,2)


b = aa.map(lambda a: (a,1)).collect()


print(a)
