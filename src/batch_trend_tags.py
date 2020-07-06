from pyspark import SparkContext
from sys import argv, exit
import re
import xml.etree.ElementTree as ET
#from pprint import pprint
from db import DB

if len(argv) < 2:
    print(f"Usage: {argv[0]} <posts-xml-file>")
    exit(-1)

POSTS_XML = argv[1]


def extract_tags(xml_string):
    node = ET.fromstring(xml_string)
    return re.sub("[<>]", " ", node.get("Tags")).split()

def filter_question(line):
    try:
        return ET.fromstring(line).get("PostTypeId") == '1'
    except:
        return False


db = DB('localhost', 27017)

sc = SparkContext()
trend_tags = sc.textFile(POSTS_XML) \
    .filter(filter_question) \
    .flatMap(extract_tags) \
    .map(lambda e: (e, 1)) \
    .reduceByKey(lambda a, b: a+b) \
    .map(lambda t: {'tag': t[0], 'count': t[1]}) \
    .collect()

db.insert_trend_tags(trend_tags)
