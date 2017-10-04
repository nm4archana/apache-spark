from pyspark import SparkConf,SparkContext

"""
@author: Archana Masilamani

Description:
Spark script to add up the total amount spent for each unique customer ID, sorted based on amount spent

"""

conf = SparkConf().setMaster("local").setAppName("PopularMovie")
sc = SparkContext(conf=conf)

rdd = sc.textFile("Data/ml-100k/u.data")

def parseLine(line):
    splittedLine = line.split()
    movieID = splittedLine[1];
    return int(movieID)

spliitedMovie = rdd.map(parseLine)

movieCountDict =  spliitedMovie.countByValue()

popularMovie  =  max(movieCountDict,key = movieCountDict.get)

print("Movie ID: ",popularMovie ," No. Of Times Watched", movieCountDict[popularMovie])
