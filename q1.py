from pyspark import SparkContext
from pyspark import SparkConf
from operator import add

# List the business_id of the Top 10 mostly-reviewed businesses based on number of users that have rated these businesses.

conf = SparkConf().setMaster("local").setAppName("sample")
sc = SparkContext(conf=conf)

# data is in the same folder of this script
# load data


# review = sc.textFile("review.csv").map(lambda line: line.split("::"))
# output = review.collect()


# calculate number of reviews
# review_by_business = review.map(lambda x: (x[2], 1))\
#     .reduceByKey(add)
#
# top_10 = review_by_business.top(10, key=lambda x: x[1])
# print(top_10)

friends = sc.textFile("soc-LiveJournal1Adj.txt").map(lambda line: line.split("\t"))

friends.map(lambda x: (x[0], x[1].split(",")))

friends = friends.map(lambda x: (x[0], x[1].split(",")))
print(friends.take(5))


# pairs = friends.map()
#
def func(line):
    newlist = []
    if line[1]!= '':

        for i in line[1]:
            if(i < line[0]):
                newlist.append((i+","+line[0],line[1]))
            else:
                newlist.append((line[0]+","+i,line[1]))
        return newlist
    return

pairs = friends.flatMap(func)

common = pairs.reduceByKey(lambda x,y:list(set(x).intersection(y)))

for i in common.collect():
    if i[0][0] != ",":
        print(i[0]+"\t",len(i[1]))



