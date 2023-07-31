import json
import re
from simhash import Simhash
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk, bulk
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

                            item['text'] = unwanted_character_filtered(
                                item['text'])

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


def put_forum_detik_id(input_path):
    for rootdir, dirs, files in os.walk(input_path):
        source = rootdir.split("/")[-1]
        language_type = source[:2]
        files.sort(reverse=True)
        if language_type in ['en', 'zh', 'vi', 'th', 'ta', 'ms', 'id']:
            index = 'social_media_'+language_type
            for file in files:
                if file.endswith('.jsonl'):
                    input_file = os.path.join(rootdir, file)
                    with open(input_file, encoding='utf8') as file_in:
                        for i, line in enumerate(file_in):
                            if i < 1490059:
                                continue
                            item = json.loads(line)

                            item['text'] = unwanted_character_filtered(
                                item.pop('post_text'))
                            item['title'] = item.pop(
                                'channel_title')+' / '+item.pop('thread_title')

                            item['source'] = source
                            item['language_type'] = language_type

                            if not item['title'] or not item['title'].strip():
                                continue
                            if not item['text'] or not item['text'].strip():
                                continue
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
                                '_id': item['thread_id'],
                                'language_type': item['language_type'],
                                'source': item['source'],
                                'url': item['thread_url'],
                                'title': item['title'],
                                'text': item['text'],
                            }

                            yield doc


def transfer_kaskus_id(input_path):

    source = 'id_kaskus'
    language_type = 'id'
    # index = 'social_media_id'
    with open('/home/xuancong/web_crawl/data/social_media/id_kaskus/id_kaskus.jsonl', 'w', encoding='utf8') as file_out:

        for rootdir, dirs, files in os.walk(input_path):

            files.sort()
            for file in files:
                item = {}

                input_file = os.path.join(rootdir, file)
                text = open(input_file, encoding='utf8').read()

                text = unwanted_character_filtered(text).strip()
                item['text'] = '\n\n---POST---\n\n'.join([post for i, post in enumerate(text.split(
                    '\n\n---POST---\n\n')) if (i == 0 or i % 21 != 0) and not post.startswith('Quote:')])
                item['thread_id'] = file.split('.')[0]
                item['source'] = source
                item['language_type'] = language_type
                item['url'] = 'https://www.kaskus.co.id/thread/' + \
                    item['thread_id']

                if item['text'] and item['thread_id']:
                    file_out.write(json.dumps(item)+"\n")


def put_bt_data(input_path):

    for rootdir, dirs, files in os.walk(input_path):
        source = rootdir.split("/")[-1]
        source = source.split("_")[-1]+'_'+source.split("_")[0]
        files.sort(reverse=True)
        index = 'bt_data'
        for file in files:
            language_type = file.split('.')[-1]
            lang_src, lang_tgt = language_type.split('2')
            domain = 'news'
            input_file = os.path.join(rootdir, file)
            with open(input_file, encoding='utf8') as file_in:
                for line in file_in:
                  
                    item = {}
                    score, sentence_src, sentence_tgt = line.split('|||')

                    item['lang_src'] = lang_src
                    item['lang_tgt'] = lang_tgt
                    item['language_type'] = language_type
                    item['source'] = source
                    item['domain'] = domain
                    item['LaBSE'] = float(score)
                    item['sentence_'+lang_src] = sentence_src.strip()
                    item['sentence_'+lang_tgt] = sentence_tgt.strip()

                    doc = {
                        '_index': index,
                        '_id': Simhash(sentence_src.strip() + sentence_tgt.strip(), f=64, reg=r'[\S]').value,
                        'language_type': item['language_type'],
                        'source': item['source'],
                        'lang_src': item['lang_src'],
                        'lang_tgt': item['lang_tgt'],
                        'domain': item['domain'],
                        'LaBSE': item['LaBSE'],
                        'sentence_'+lang_src: item['sentence_'+lang_src],
                        'sentence_'+lang_tgt: item['sentence_'+lang_tgt],
                    }

                    yield doc


res = bulk(client=es,
           actions=put_bt_data("/home/xuanlong/dataclean/data/stage5/abc_en"),
           )

print(res, flush=True)

# transfer_kaskus_id("/home/xuancong/airflow/data/id_kaskus")
# print('finished all', flush=True)

# doc = {
#     'author': 'author_name',
#     'text': 'Interesting content...',
# }
# res = es.index(index="python_es01", id="1", document=doc)
# if res.meta.status!=200:
#     print(res)
