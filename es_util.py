import json
import re
from simhash import Simhash
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import os

ES_CONNECTION_STRING = "http://localhost:9200"
es = Elasticsearch(ES_CONNECTION_STRING).options(ignore_status=400)


class Reg_Exp:
    pattern_punctuation = r"""[!?,.:;"#$£€%&'()+-/<≤=≠≥>@[\]^_{|}，。、—‘’“”：；【】￥…《》？！（）]"""
    pattern_url = r"[(http(s)?):\/\/(www\.)?a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#?&//=]*)"
    pattern_email = r"[\w\-\.]+@([\w\-]+\.)+[\w\-]{2,4}"
    pattern_arabic = r"[\u0600-\u06FF]"
    pattern_chinese = r"[\u4e00-\u9fff]"
    pattern_tamil = r"[\u0B80-\u0BFF]"
    pattern_thai = r"[\u0E00-\u0E7F]"
    pattern_russian = r"[\u0400-\u04FF]"
    pattern_korean = r"[\uac00-\ud7a3]"
    pattern_japanese = r"[\u3040-\u30ff\u31f0-\u31ff]"
    pattern_vietnamese = r"[àáãạảăắằẳẵặâấầẩẫậèéẹẻẽêềếểễệđìíĩỉịòóõọỏôốồổỗộơớờởỡợùúũụủưứừửữựỳỵỷỹýÀÁÃẠẢĂẮẰẲẴẶÂẤẦẨẪẬÈÉẸẺẼÊỀẾỂỄỆĐÌÍĨỈỊÒÓÕỌỎÔỐỒỔỖỘƠỚỜỞỠỢÙÚŨỤỦƯỨỪỬỮỰỲỴỶỸÝ]"
    pattern_emoji = r'[\U0001F1E0-\U0001F1FF\U0001F300-\U0001F64F\U0001F680-\U0001FAFF\U00002702-\U000027B0]'


def unwanted_character_filtered(text_for_filter):
    text = re.sub(r'[^a-zA-Z0-9\s\t{}{}{}{}{}{}{}{}{}{}]'.format(Reg_Exp.pattern_punctuation[1:-1],
                                                                 Reg_Exp.pattern_arabic[1:-1],
                                                                 Reg_Exp.pattern_chinese[1:-1],
                                                                 Reg_Exp.pattern_tamil[1:-1],
                                                                 Reg_Exp.pattern_thai[1:-1],
                                                                 Reg_Exp.pattern_russian[1:-1],
                                                                 Reg_Exp.pattern_korean[1:-1],
                                                                 Reg_Exp.pattern_japanese[1:-1],
                                                                 Reg_Exp.pattern_vietnamese[1:-1],
                                                                 Reg_Exp.pattern_emoji[1:-1],
                                                                 ), '', text_for_filter)
    return text


def unwanted_character_detected(text_for_detect):
    matchs = re.search(
        r'[^a-zA-Z0-9\s\t{}{}{}{}{}{}{}{}{}{}]'.format(
            Reg_Exp.pattern_punctuation[1:-1],
            Reg_Exp.pattern_arabic[1:-1],
            Reg_Exp.pattern_chinese[1:-1],
            Reg_Exp.pattern_tamil[1:-1],
            Reg_Exp.pattern_thai[1:-1],
            Reg_Exp.pattern_russian[1:-1],
            Reg_Exp.pattern_korean[1:-1],
            Reg_Exp.pattern_japanese[1:-1],
            Reg_Exp.pattern_vietnamese[1:-1],
            Reg_Exp.pattern_emoji[1:-1],
        ), text_for_detect, re.I)
    if matchs:
        return True
    return False


def tamil_detected(text):
    if re.search(Reg_Exp.pattern_tamil, text):
        return True
    return False


def vietnamese_detected(text):
    if re.search(Reg_Exp.pattern_vietnamese, text):
        return True
    return False


def chinese_detected(text):
    if re.search(Reg_Exp.pattern_chinese, text):
        return True
    return False


def thai_detected(text):
    if re.search(Reg_Exp.pattern_thai, text):
        return True
    return False


def put_news_article(input_path="/home/xuancong/web_crawl/data"):
    for rootdir, dirs, files in os.walk(input_path):
        source = rootdir.split("/")[-1]
        language_type = source[:2]
        files.sort(reverse=True)
        if language_type in ['en', 'zh', 'vi', 'th', 'ta', 'ms', 'id']:
            index = 'news_articles_'+language_type
            for file in files:
                if file.endswith('.jsonl'):
                    input_file = os.path.join(rootdir, file)
                    with open(input_file, encoding='utf8') as file_in:
                        for line in file_in:
                            item = json.loads(line)

                            if unwanted_character_detected(item['text']):
                                item['text'] = unwanted_character_filtered(item['text'])

                            if 'ta' in [language_type] and not tamil_detected(item['text']):
                                continue
                            if 'vi' in [language_type] and not vietnamese_detected(item['text']):
                                continue
                            if 'zh' in [language_type] and not chinese_detected(item['text']):
                                continue
                            if not item['text'].strip():
                                continue

                            doc = {
                                '_index': index,
                                '_id': Simhash(item['text'], f=64, reg=r'[\S]').value,
                                'language_type': language_type,
                                'source': source,
                                'title': item['title'],
                                'text': item['text'],
                                'date': item['date'],
                            }

                            yield doc


response = bulk(client=es, actions=put_news_article())
print(response, flush=True)


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
