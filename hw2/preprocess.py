from pyspark import SparkContext
import csv
import json
import sys



reviewFile = sys.argv[1]
businessFile = sys.argv[2]
outputFile = sys.argv[3]
""" reviewFile = 'review.json'
businessFile = 'business.json'
outputFile = 'user-business.csv' """


sc = SparkContext()

reviewraw = sc.textFile(reviewFile).map(lambda item: json.loads(item))
businessraw = sc.textFile(businessFile).map(lambda item: json.loads(item))


#businessNV = businessraw.map(lambda item: (item['business_id'],item['state'])).filter(lambda item: item[1] == 'NV').map(lambda item: item[0]).collect()
#review = reviewraw.map(lambda item: (item['user_id'],item['business_id'])).filter(lambda item: item[1] in businessNV).collect()
dataOri = sc.textFile('small2.csv')
attri = dataOri.first()
data = dataOri.filter(lambda row: row != attri)
review = data.map(lambda item: (item.split(',')[0],item.split(',')[1])).collect()


with open(outputFile, 'w+', newline='') as opf:
    writer = csv.writer(opf, quoting=csv.QUOTE_NONE)
    writer.writerow(["user_id", "business_id"])
    for row in review:
        writer.writerow(row)





#a = 1