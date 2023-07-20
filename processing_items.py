import html
import itertools
import json
import re
from simhash import Simhash
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import os
from sentsplit.segment import SentSplit
from thai_segmenter import sentence_segment

ES_CONNECTION_STRING = "http://localhost:9200"
es = Elasticsearch(ES_CONNECTION_STRING).options(ignore_status=400)

sentence_splitter_en = SentSplit('en', strip_spaces=True, maxcut=512)
sentence_splitter_zh = SentSplit('zh', strip_spaces=True, maxcut=150)
sentence_splitter_th = sentence_segment

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


def sentence_split(language_type, text):

    text = html.unescape(text.strip().replace('「', '“').replace('」', '”'))

    texts = [' '.join(text.split())
                for text in text.split('\n') if text.strip()]

    if language_type in {'en', 'vi', 'ta', 'id', 'ms'}:
        return list(itertools.chain.from_iterable(sentence_splitter_en.segment(texts)))
    if language_type in {'zh'}:
        return list(itertools.chain.from_iterable(sentence_splitter_zh.segment(texts)))
    if language_type in {'th'}:
        sentences_list = [[str(sentence) for sentence in sentence_splitter_th.segment(
            text.strip())] for text in texts]
        return list(itertools.chain.from_iterable(sentences_list))


def process_news_article(input_path):
    for rootdir, dirs, files in os.walk(input_path):
        source = rootdir.split("/")[-1]
        language_type = source[:2]
        files.sort(reverse=True)
        if language_type in ['en', 'zh', 'vi', 'th', 'ta', 'ms', 'id']:

            for file in files:
                if file.endswith('.jsonl'):
                    input_file = os.path.join(rootdir, file)
                    output_file = os.path.join(rootdir, file.replace('jsonl', 'jl'))
                    with open(input_file, encoding='utf8') as file_in, \
                        open(output_file, 'w', encoding='utf8') as file_out:
                        for line in file_in:
                            item = json.loads(line)

                            item['text'] = unwanted_character_filtered(item['text'])

                            if not item['text'].strip():
                                continue
                            if not item['title'].strip():
                                continue
                            if 'ta' in [language_type] and not tamil_detected(item['text']):
                                continue
                            if 'vi' in [language_type] and not vietnamese_detected(item['text']):
                                continue
                            if 'zh' in [language_type] and not chinese_detected(item['text']):
                                continue
                            if 'th' in [language_type] and not thai_detected(item['text']):
                                continue

                            item['source']=source
                            item['language_type']=language_type
                            item['split_sentences']=sentence_split(item['language_type'], item['text'])


                            file_out.write(json.dumps(item, ensure_ascii=False)+'\n')


if __name__=="__main__":
    process_news_article("/home/xuancong/web_crawl/data")
    print("all finished", flush=True)