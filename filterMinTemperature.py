from pyspark import SparkContext,SparkConf
import collections

conf = SparkConf().setMaster("local").setAppName("Minimum Temperature")
sc = SparkContext(conf = conf)
rdd = sc.textFile("/Users/archana/Documents/Projects/Spark/1800.csv")

'''
parsedLines = rdd.map(lambda x: x.split(','))

minTemps = parsedLines.filter(lambda x: 'TMIN' in x[2])
results = minTemps.collect()

for result in results:
    print(result)
'''

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3])*0.1*(9.0/5.0)+32.0
    return(stationID,entryType,temperature)

lines = rdd.map(parseLine)

#minTemps = lines.filter(lambda x: 'TMIN' in x[1])
minTemps = lines.filter(lambda x: 'TMAX' in x[1])

print("**************",type(minTemps))
'''
lines = minTemps.collect()

print(lines)

for result in lines:
    print(result)
'''

stationTemps = minTemps.map(lambda x:(x[0],x[2]))

#minTemps = stationTemps.reduceByKey(lambda x,y: min(x,y))

minTemps = stationTemps.reduceByKey(lambda x,y: max(x,y))

minTemps = minTemps.collect()
print(type(minTemps))