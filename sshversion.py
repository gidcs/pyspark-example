# get parameter
import sys
# spark
from pyspark import SparkContext, SparkConf
# json object
import json

def parse_json(line):
    data = json.loads(line)
    server_protocol = {}
    if 'data' in data:
        if 'ssh' in data['data']:
            if 'server_protocol' in data['data']['ssh']:
                server_protocol = data['data']['ssh']['server_protocol']
    ret = ""
    if 'raw_banner' in server_protocol:
        raw_banner = server_protocol['raw_banner'].rstrip().encode('utf-8')
        if '220 ' in raw_banner:
            ret = "FTP"
        elif 'HTTP/' in raw_banner:
            ret = "HTTP"
        else:
            ret = raw_banner
    else:
        ret = "invalid"
    
    return [ret]

if __name__ == "__main__":

    inputJson = sys.argv[1]
    outputDir = sys.argv[2]

    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("SSHVersion")
    sc = SparkContext(conf=conf)

    # read in text file and split each document into words
    text_file = sc.textFile(inputJson)
    words = text_file.flatMap(parse_json)

    # count the occurrence of each word
    counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    result = counts.collect()

    # for (word, count) in result:
    #    print("%s: %i" % (word, count))

    counts.saveAsTextFile(outputDir)
