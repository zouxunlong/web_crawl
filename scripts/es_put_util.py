import json
import re
from simhash import Simhash
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import os


ES_CONNECTION_STRING = "http://10.2.56.42:9200"
es = Elasticsearch(hosts=ES_CONNECTION_STRING, http_auth=(
    'elastic', 'elastic_pw')).options(ignore_status=400, request_timeout=30)


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


def put_news_article(input_file="/home/xuancong/web_crawl/data"):
    source = 'zh_8world'
    language_type = 'zh'

    if language_type in ['en', 'zh', 'vi', 'th', 'ta', 'ms', 'id']:
        index = 'news_articles_'+language_type
        if input_file.endswith('.jsonl'):
            with open(input_file, encoding='utf8') as file_in:
                for i, line in enumerate(file_in):
                    if i % 2000 == 0:
                        print(i, flush=True)
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


def put_forum_kaskus_id(input_path):

    index = 'social_media_id'
    with open(input_path, encoding='utf8') as file_in:
        for i, line in enumerate(file_in):
            item = json.loads(line)
            if not item['text'] or not item['text'].strip():
                continue
            doc = {
                '_index': index,
                '_id': item['thread_id'],
                'language_type': item['language_type'],
                'source': item['source'],
                'url': item['url'],
                'text': item['text'],
            }
            yield doc


def put_forum_hardwarezone_en(input_path):

    index = 'social_media_en'
    source = 'en_hardwarezone'
    language_type = 'en'
    with open(input_path, encoding='utf8') as file_in:
        for i, line in enumerate(file_in):
            item = json.loads(line)
            if not item['post_text'] or not item['post_text'].strip():
                continue
            doc = {
                '_index': index,
                '_id': Simhash(item['post_text'].strip(), f=64, reg=r'[\S]').value,
                'language_type': language_type,
                'source': source,
                'url': item['thread_url'],
                'title': item['channel_title'] + ' / ' + item['thread_title'],
                'text': item['post_text'].strip(),
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


def transfer_hardwarezone_en(input_path, output_path):

    with open(input_path, encoding='utf8') as file_in, \
            open(output_path, 'w', encoding='utf8') as file_out:
        for i, line in enumerate(file_in):
            item = json.loads(line)
            if item['post_text'].strip() and item['thread_url'].strip() and item['channel_title'].strip() and item['thread_title'].strip():
                item['text'] = item.pop('post_text')
                file_out.write(json.dumps(item)+"\n")
        print(i, flush=True)


def put_bt_data(input_path):
    for rootdir, dirs, files in os.walk(input_path):
        source = rootdir.split("/")[-1]
        files.sort(reverse=True)
        index = 'bt_data'
        for file in files:
            if len(file.split('.')) != 3:
                continue
            language_type = file.split('.')[-1]
            if language_type not in ['th2en']:
                continue
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


def put_newslink(input_path, index, src, lang):

    with open(input_path) as file_in:
        for i, line in enumerate(file_in):
            if i % 20000 == 0:
                print(i, flush=True)
            item = json.loads(line)
            if "bodyarticle" in item.keys():
                bodyarticle = item['bodyarticle']
                text = re.sub('<br[^>]*/>', "\n", bodyarticle, 0,
                              re.I).strip().replace('\xa0', ' ').replace('\u3000', ' ')
                if text:
                    doc = {
                        '_index': index,
                        '_id': item['documentid'],
                        'text': text,
                        'source': src,
                        'language_type': lang,
                        'date': item['publicationdate']
                    }
                    yield doc


if __name__ == "__main__":

    mapping_src=json.load(open("/home/xuanlong/web_crawl/mapping_src.json"))
    mapping_lang=json.load(open("/home/xuanlong/web_crawl/mapping_lang.json"))

    for root, dirs, files in os.walk("/home/xuanlong/web_crawl/data/newslink"):
        for file in files:
            media=file.split(".")[-3]
            src=mapping_src[media]
            lang=mapping_lang[media]
            index="newslink_{}".format(lang)
            res = bulk(client=es,
                       actions=put_newslink(os.path.join(root, file), index, src, lang)
                       )
            print("complete {}".format(file), flush=True)
            print(res, flush=True)
    print("complete all".format(file), flush=True)
