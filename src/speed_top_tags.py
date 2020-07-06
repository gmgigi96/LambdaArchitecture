from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession

from sys import argv, exit
import re
import xml.etree.ElementTree as ET
from db import DB
from collections import namedtuple
#from pprint import pprint


if len(argv) != 4:
    print(f"Usage: {argv[0]} <host source> <port source> <mongo ip>")
    exit(-1)

host, port, mongo_ip = argv[1:]
port = int(port)

ss = SparkSession.builder.appName("prova").getOrCreate()
ssc = StreamingContext(ss.sparkContext, 2)


def extract_tags(xml_string):
    node = ET.fromstring(xml_string)
    return re.sub("[<>]", " ", node.get("Tags")).split()


db = DB(mongo_ip, 27017)

kvs = ssc.socketTextStream(host, port)


ANSWER_POST_TYPE = '2'
QUESTION_POST_TYPE = '1'

Answer = namedtuple('Answer', ['parent', 'score', 'owner'])


def xml_to_answer(xml_string):
    row = ET.fromstring(xml_string)
    owner = row.get('OwnerUserId')
    return Answer(int(row.get('ParentId')), int(row.get('Score')), int(owner if owner else -1))


def xml_to_question(xml_string):
    row = ET.fromstring(xml_string)
    return (row.get('Id'), re.sub("[<>]", " ", row.get("Tags")).split())


def extract_tag(t):
    tags, ans = t[1]
    return [(tag, ans) for tag in tags]


db = DB('localhost', 27017)

questions = ss.read.format("mongo").option("uri", f"mongodb://{mongo_ip}").option(
    "database", "stackoverflow").option("collection", "questions").load().rdd.map(lambda row: (row.id, row.tags))


answers = kvs.filter(lambda l: ET.fromstring(l).get('PostTypeId') == ANSWER_POST_TYPE) \
    .map(xml_to_answer) \
    .filter(lambda a: a.owner != -1) \
    .map(lambda a: (a.parent, a))


top_tags = answers.transform(lambda rdd: rdd.join(questions)) \
                    .flatMap(extract_tag) \
                    .map(lambda t: ((t[0], t[1].owner), (t[1].score, 1))) \
                    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
                    .map(lambda a: (a[0][1], (a[0][0], a[1][0], a[1][1]))) \
                    .groupByKey() \
                    .map(lambda t: (t[0], sorted(t[1], reverse=True, key=lambda x: x[1]/x[2])[:3])) \
                    .map(lambda t: {'owner_id': t[0], 'tags': list(map(lambda x: {'tag': x[0], 'total_score': x[1], 'count': x[2]}, t[1]))})


def save_db(time, rdd):
    print(f"[X] {time}: Saving into db...")
    #pprint(rdd.collect())
    db.update_top_tags(rdd.collect())


top_tags.foreachRDD(save_db)


ssc.start()
ssc.awaitTermination()
