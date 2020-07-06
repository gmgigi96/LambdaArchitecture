from pyspark import SparkContext
from sys import argv, exit
from collections import namedtuple
import re
import xml.etree.ElementTree as ET
from db import DB
#from pprint import pprint

if len(argv) < 2:
    print(f"Usage: {argv[0]} <posts-xml-file>")
    exit(-1)

POSTS_XML = argv[1]
ANSWER_POST_TYPE = '2'
QUESTION_POST_TYPE = '1'

Answer = namedtuple('Answer', ['parent', 'score', 'owner'])
Question = namedtuple('Question', ['id', 'tags'])


def xml_to_answer(xml_string):
    row = ET.fromstring(xml_string)
    owner = row.get('OwnerUserId')
    return Answer(int(row.get('ParentId')), int(row.get('Score')), int(owner if owner else -1))


def xml_to_question(xml_string):
    row = ET.fromstring(xml_string)
    return Question(int(row.get('Id')), re.sub("[<>]", " ", row.get("Tags")).split())


def extract_tag(t):
    tags, ans = t[1]
    return [(tag, ans) for tag in tags]


db = DB('localhost', 27017)

sc = SparkContext()
posts = sc.textFile(POSTS_XML).cache()

def filter_question(l):
    try:
        return ET.fromstring(l).get('PostTypeId') == QUESTION_POST_TYPE
    except:
        return False

def filter_answer(l):
    try:
        return ET.fromstring(l).get('PostTypeId') == ANSWER_POST_TYPE
    except:
        return False

questions = posts.filter(filter_question) \
                 .map(xml_to_question).cache()

answers = posts.filter(filter_answer) \
               .map(xml_to_answer) \
               .filter(lambda a: a.owner != -1) \
               .map(lambda a: (a.parent, a))

top_tags = questions.join(answers) \
                    .flatMap(extract_tag) \
                    .map(lambda t: ((t[0], t[1].owner), (t[1].score, 1))) \
                    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
                    .map(lambda a: (a[0][1], (a[0][0], a[1][0], a[1][1]))) \
                    .groupByKey() \
                    .map(lambda t: (t[0], sorted(t[1], reverse=True, key=lambda x: x[1]/x[2])[:3])) \
                    .map(lambda t: {'owner_id': t[0], 'tags': list(map(lambda x: {'tag': x[0], 'total_score': x[1], 'count': x[2]}, t[1]))}) \
                    .collect()

#pprint(top_tags)

db.insert_top_tags(top_tags)

questions = questions.map(lambda q: q._asdict()).collect()
db.insert_questions(questions)
