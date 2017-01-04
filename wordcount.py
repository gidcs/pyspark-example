# get parameter
import sys
# spark
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    inputJson = sys.argv[1]
    outputDir = sys.argv[2]

    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("WordCount")
    sc = SparkContext(conf=conf)

    # read in text file and split each document into words
    text_file = sc.textFile(inputJson)
    words = text_file.flatMap(lambda line: line.split(" "))

    # count the occurrence of each word
    counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    result = counts.collect()

    for (word, count) in result:
        print("%s: %i" % (word, count))

    counts.saveAsTextFile(outputDir)