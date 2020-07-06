from pymongo import MongoClient


class DB:

    def __init__(self, host, port):
        self.client = MongoClient(host=host, port=port)
        self.db = self.client.stackoverflow

    def insert_top_tags(self, ttlist):
        self.db.top_tags.delete_many({})
        self.db.top_tags.insert_many(ttlist)

    def update_top_tags(self, ttlist):
        for elem in ttlist:
            for tag in elem['tags']:
                self.db.top_tags.update({'owner_id': elem['owner_id'], 'tags': {
                                        '$elemMatch': {'tag': tag['tag']}}}, {'$inc': {'tags.$.count': tag['count'], 'tags.$.total_score': tag['total_score']}})

    def insert_trend_tags(self, ttlist):
        self.db.trend_tags.delete_many({})
        self.db.trend_tags.insert_many(ttlist)

    def update_trend_tags(self, ttlist):
        for tag in ttlist:
            self.db.trend_tags.update({'tag': tag['tag']}, {
                '$inc': {'count': tag['count']}})

    def insert_questions(self, qlist):
        self.db.questions.delete_many({})
        self.db.questions.insert_many(qlist)

