from pyspark import SparkContext,SparkConf

"""
@author: Archana Masilamani

Description:
Spark script to find the Movie name using Broadcast in spark

"""

conf = SparkConf().setMaster("local").setAppName("Popular Movie")
sc = SparkContext(conf=conf)

def loadMovieNames():
    movieNames = {}
    with open("Data/ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

    print()

def rev(text):
    x,y = text
    text = y,x
    return text

def getMovie(text):
    count, movie = text;
    text = nameDict.value[movie],count
    return text


rdd = sc.textFile("Data/ml-100k/u.data")
movieName = rdd.map( lambda x : (int(x.split()[1]),1))

movieCount = movieName.reduceByKey(lambda x,y : x+y);

movieCountRev = movieCount.map(rev);

sortedMovies = movieCountRev.sortByKey();

nameDict = sc.broadcast(loadMovieNames())

sortedMoviesWithNames = sortedMovies.map(getMovie)

results = sortedMoviesWithNames.collect()

for key,val in results:
    print(key,val)

