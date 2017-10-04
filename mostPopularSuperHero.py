from pyspark import SparkContext,SparkConf

"""
@author: Archana Masilamani

Description:
Spark script to find the Most popular super hero in a Social Graph

"""

conf = SparkConf().setMaster("local").setAppName("MostPopularSuper")
sc = SparkContext(conf=conf)

def rev(text):
    x,y = text
    text = y,x
    return text

def countOfOccurance(line):
    splittedLine = line.split()
    occCount = len(splittedLine) - 1;
    return splittedLine[0],occCount;

def parserNames(line):
    fields = line.split("\"")
    return int(fields[0]),fields[1]

rdd = sc.textFile("Data/Marvel-Graph.txt")

occuranceCnt = rdd.map(countOfOccurance)

occuranceCntAll = occuranceCnt.reduceByKey(lambda x,y:x+y)

occuranceCntAllRev = occuranceCntAll.map(rev)

namesrdd = sc.textFile("Data/Marvel-Names.txt")

namesList = namesrdd.map(parserNames)

mostPopular = occuranceCntAllRev.max();

mostPopularName = namesList.lookup(int(mostPopular[1]))[0]

print("The most popular Super Hero is ",mostPopularName, "with co-appearances",mostPopular[0])