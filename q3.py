from pyspark import SparkContext
from pyspark import SparkConf
from operator import add

conf = SparkConf().setMaster("local").setAppName("sample")
sc = SparkContext(conf=conf)

friends = sc.textFile("soc-LiveJournal1Adj.txt").map(lambda line: line.split("\t"))
user = sc.textFile("userdata.txt").map(lambda line: line.split(",", 1))
# print(user.take(2))





# print(friends.take(5))
#get the bussiness id with stanford
business = sc.textFile("business.csv").map(lambda x: x.split("::"))
buss = business.collect()
print(buss[0])
if "Hill" in buss[0][1]:
    print("yes")

def func(line):
    if "Stanford" in line[1]:
        return line



businstan = business.map(func).filter(lambda x: x != None)


print(businstan.take(2))

review = sc.textFile("review.csv").map(lambda x: x.split("::")).map(lambda x: [x[2],x])
print(review.take(2))


joined = businstan.leftOuterJoin(review)
print("joined",joined.take(2))
output = joined.collect()
for i in output:
    print(i[1][1][0]+"\t"+i[1][1][3])




