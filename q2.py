from pyspark import SparkContext
from pyspark import SparkConf
from operator import add

conf = SparkConf().setMaster("local").setAppName("sample")
sc = SparkContext(conf=conf)

friends = sc.textFile("soc-LiveJournal1Adj.txt").map(lambda line: line.split("\t"))
user = sc.textFile("userdata.txt").map(lambda line: line.split(",", 1))
# print(user.take(2))


friends.map(lambda x: (x[0], x[1].split(",")))

friends = friends.map(lambda x: (x[0], x[1].split(",")))


# print(friends.take(5))

def func(line):
    newlist = []
    if line[1] != '':

        for i in line[1]:
            if (i < line[0]):
                newlist.append((i + "," + line[0], line[1]))
            else:
                newlist.append((line[0] + "," + i, line[1]))
        return newlist
    return


pairs = friends.flatMap(func)

common = pairs.reduceByKey(lambda x, y: list(set(x).intersection(y)))
common = common.map(lambda x: (x[0], len(x[1])))

sortedcommon = common.sortBy(lambda x: x[1], False)

top10 = sc.parallelize(sortedcommon.take(10)).map(lambda x: (x[0].split(",")[0], x[0].split(",")[1], x[1]))
fliptop10 = sc.parallelize(sortedcommon.take(10)).map(lambda x: (x[0].split(",")[1], x[0].split(",")[0], x[1]))

sortedcommonlist = sortedcommon.collect()

print("top10", top10.collect())


joined = top10.leftOuterJoin(user)
joinedlist: object = joined.collect()


flipjoined = fliptop10.leftOuterJoin(user)
fliptop10list = flipjoined.collect()
print("flip",fliptop10list)
print("joined", joined.take(3))
print("test", joined.collect()[0][1][1].split(",")[0])
for i in range(0, 10):
    print(str(top10.collect()[i][2]) + "\t" + joinedlist[i][1][1].split(",")[0]
          + "\t" + joinedlist[i][1][1].split(",")[1] + "\t" + joinedlist[i][1][1].split(",")[2]
          + "\t" + fliptop10list[i][1][1].split(",")[0] + "\t" + fliptop10list[i][1][1].split(",")[1]
          + "\t" + fliptop10list[i][1][1].split(",")[2])

