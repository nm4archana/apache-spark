from pyspark import SparkConf,SparkContext
import collections

"""
@author: Archana Masilamani

Description:
Spark script to add up the total amount spent for each unique customer ID
"""

def pareseLine(line):
    splittedLine = line.split(',')
    customerId = int(splittedLine[0])
    price = float(splittedLine[2])
    return customerId,price


conf = SparkConf().setMaster("local").setAppName("TotalAmntSpent")
sc = SparkContext(conf=conf)

rdd = sc.textFile("Data/customer-orders.csv")

splitData = rdd.map(pareseLine)

totalExpense = splitData.reduceByKey(lambda x, y: x+y)

for key,val in totalExpense.collect():
    print("Customer ID: " ,key, " Total Price: {:.2f}".format(val))


