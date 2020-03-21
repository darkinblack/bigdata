from pyspark import SparkContext
from pyspark import SparkConf
from operator import add

conf = SparkConf().setMaster("local").setAppName("sample")
sc = SparkContext(conf=conf)




business = sc.textFile("business.csv").map(lambda x: x.split("::")).map(lambda x: (x[0],[x[1],x[2]])).reduceByKey(lambda x,y:x)
buss = business.collect()
# print(buss[0])


review = sc.textFile("review.csv").map(lambda x: x.split("::")).map(lambda x:(x[2],float(x[3])))
reviewcount = review.map(lambda x:(x[0],1))
totalpoint = review.reduceByKey(add)
count = reviewcount.reduceByKey(add)

combine = totalpoint.join(count).map(lambda x: (x[0],round(x[1][0]/x[1][1],1))).sortBy(lambda x: x[1],False)
top10 = sc.parallelize(combine.take(10))
# print(top10.take(5))

joined = top10.leftOuterJoin(business).collect()
# print(joined)

for i in joined:
    print(i[0]+"\t"+i[1][1][0]+"\t"+i[1][1][1]+"\t"+str(i[1][0]))







