from pyspark import SparkConf,SparkContext
import collections



conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

rdd = sc.textFile("Data/ml-100k/u.data")

ratings = rdd.map(lambda x: x.split()[2])
result = ratings.countByValue();
#print(type(result))

sortedResults = collections.OrderedDict(sorted(result.items()))

for key,value in sortedResults.items():
    print(key,value)