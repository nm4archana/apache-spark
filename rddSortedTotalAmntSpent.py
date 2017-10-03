from pyspark import SparkConf,SparkContext

"""
@author: Archana Masilamani

Description:
Spark script to add up the total amount spent for each unique customer ID, sorted based on amount spent
"""

def pareseLine(line):
    splittedLine = line.split(',')
    customerId = int(splittedLine[0])
    price = float(splittedLine[2])
    return customerId,price

def rev(text):
    x,y = text
    text = y,x
    return text

conf = SparkConf().setMaster("local").setAppName("TotalAmntSpent")
sc = SparkContext(conf=conf)

rdd = sc.textFile("Data/customer-orders.csv")

splitData = rdd.map(pareseLine)

totalExpense = splitData.reduceByKey(lambda x, y: x+y)

totalExpenserev = totalExpense.map(rev)

totalExpenseSort = totalExpenserev.sortByKey()

for key,val in totalExpenseSort.collect():
    print("Customer ID: " ,val, " Total Price: {:.2f}".format(key))


