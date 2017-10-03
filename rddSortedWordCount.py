from pyspark import SparkContext,SparkConf
import re

"""
@author: Archana Masilamani

Description:
Spark script for Sorted Word Count (RDD - Scalable) - Not using countByValue()

"""


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

def rev(text):
    x,y = text
    text = y,x
    return text

rdd = sc.textFile("Data/Book.txt")

words = rdd.flatMap(normalizeWords)

wordCount = words.map(lambda x:(x,1))

groupWordCount = wordCount.reduceByKey(lambda x,y : x+y)
wordCountrev = groupWordCount.map(rev)

#Sort By count
sortedWordCount = wordCountrev.sortByKey()

for key,val in sortedWordCount.collect():
    cleanWord = val.encode('ascii', 'ignore')
    if cleanWord:
        print(key,val)




