from pyspark import SparkContext,SparkConf
import collections

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)

def parseLine(line):
    fields = line.split(',');
    age = int(fields[2])
    numFriends = int(fields[3])
    return(age, numFriends)

lines = sc.textFile('/Users/archana/Documents/Projects/Spark/Data/fakefriends.csv')
rdd = lines.map(parseLine)
mapToOne = rdd.mapValues(lambda x:(x,1))
totalByAge = mapToOne.reduceByKey(lambda x, y: (x[0]+y[0] , x[1]+y[1]))
avgByAge = totalByAge.mapValues(lambda x: x[0]/x[1])
results = avgByAge.collect()

for result in results:
    print(result)