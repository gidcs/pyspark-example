# get parameter
import sys
# spark
from pyspark import SparkContext, SparkConf
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt

if __name__ == "__main__":

    inputFile = sys.argv[1]
    # outputDir = sys.argv[2]

    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("K-means")
    sc = SparkContext(conf=conf)

    # Load and parse the data`
    data = sc.textFile(inputFile)
    parsedData = data.map(lambda line: array([float(x) for x in line.split(' ')]))

    # Build the model (cluster the data)
    clusters = KMeans.train(parsedData, 6, maxIterations=10,
            runs=10, initializationMode="random")

    # Evaluate clustering by computing Within Set Sum of Squared Errors
    def error(point):
        center = clusters.centers[clusters.predict(point)]
        return sqrt(sum([x**2 for x in (point - center)]))

    WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
    print("Within Set Sum of Squared Error = " + str(WSSSE))

    # Save and load model
    clusters.save(sc, "myModelPath")

    for x in clusters.clusterCenters:
        print(x)
    # sameModel = KMeansModel.load(sc, "myModelPath")

    # show result
