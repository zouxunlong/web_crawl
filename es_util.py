from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import os

ES_CONNECTION_STRING = "http://localhost:9200"
es = Elasticsearch(ES_CONNECTION_STRING).options(ignore_status=400)


def put_news_article(input_path="/home/xuancong/web_crawl/data"):
    for rootdir, dirs, files in os.walk(input_path):

# def generate_actions():
#     for item in db_worksheet['vi-en.train'].find().sort('_id', 1):
#         yield item

# response=bulk(client=es, index="worksheet_vi-en.train", actions=generate_actions())

# doc = {
#     'author': 'author_name',
#     'text': 'Interesting content...',
#     'timestamp': datetime.now(),
# }
# res = es.index(index="python_es01", id=1, document=doc)
# print(res['result'])


# res = es.get(index="python_es01", id=1)
# print(res['_source'])


# res =es.indices.create(index='python_es02')
# print(res)


# resp = es.search(index="bt_data", query={"match_all": {}}, track_total_hits=True, size=100, sort=  {"LaBSE": {"order": "asc"}}, search_after=[-0.0396])
# print("Got %d Hits:" % resp['hits']['total']['value'])
# for hit in resp['hits']['hits']:
#     print("(text)s".format(hit["_source"]))


# print(response, flush=True)
