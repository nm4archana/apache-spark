from pyspark import SparkContext,SparkConf
import collections
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

rdd = sc.textFile("Data/Book.txt")

words = rdd.flatMap(normalizeWords)
wordsCount = words.countByValue()

'''
for key, value in wordsCount.items():
    cleanWord = key.encode('ascii', 'ignore')
    if cleanWord:
        print(key, value)
'''

sortedResults = collections.OrderedDict(sorted(wordsCount.items()))

for key,value in sortedResults.items():
    cleanWord = key.encode('ascii', 'ignore')
    if cleanWord:
        print(key,value)

