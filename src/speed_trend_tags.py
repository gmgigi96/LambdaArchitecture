# spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 src/speed_layer/trend_tags.py localhost:9092 posts

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from sys import argv, exit
import re
import xml.etree.ElementTree as ET
from db import DB
#from pprint import pprint
#from uuid import uuid1


if len(argv) != 3:
    print(f"Usage: {argv[0]} <host source> <port source>")
    exit(-1)


sc = SparkContext(appName='TrendTagsStreaming')
ssc = StreamingContext(sc, 2)

host = argv[1]
port = int(argv[2])


def extract_tags(xml_string):
    node = ET.fromstring(xml_string)
    return re.sub("[<>]", " ", node.get("Tags")).split()

db = DB('localhost', 27017)

kvs = ssc.socketTextStream(host, port)

trend_tags = kvs \
    .filter(lambda line: ET.fromstring(line).get("PostTypeId") == '1') \
    .flatMap(extract_tags) \
    .map(lambda e: (e, 1)) \
    .reduceByKey(lambda a, b: a+b) \
    .map(lambda t: {'tag': t[0], 'count': t[1]})



def save_db(time, rdd):
    print(f"[X] {time}: Saving into db...")
    db.update_trend_tags(rdd.collect())


trend_tags.foreachRDD(save_db)


ssc.start()
ssc.awaitTermination()
