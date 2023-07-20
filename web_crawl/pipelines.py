# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import html
import itertools
from itemadapter import ItemAdapter
import re
from simhash import Simhash
from elasticsearch import Elasticsearch
from scrapy.exceptions import DropItem
from sentsplit.segment import SentSplit
from thai_segmenter import sentence_segment


class ElasticSearchPipeline:

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

    def __init__(self, ES_CONNECTION_STRING):
        self.ES_CONNECTION_STRING = ES_CONNECTION_STRING
        self.sentence_splitter_en = SentSplit(
            'en', strip_spaces=True, maxcut=512)
        self.sentence_splitter_zh = SentSplit(
            'zh', strip_spaces=True, maxcut=150)
        self.sentence_splitter_th = sentence_segment

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            ES_CONNECTION_STRING=crawler.settings.get("ES_CONNECTION_STRING"),
        )

    def open_spider(self, spider):
        self.client = Elasticsearch(
            self.ES_CONNECTION_STRING).options(ignore_status=400)

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):

        language_type = spider.name[:2]

        item['text'] = self.unwanted_character_filtered(item['text'])
        item['language_type'] = language_type

        if not item['title'].strip():
            raise DropItem(f"no title detected: {item!r}")
        if not item['text'].strip():
            raise DropItem(f"no text detected: {item!r}")
        if 'ta' in [language_type] and not self.tamil_detected(item['text']):
            raise DropItem(f"no tamil detected: {item!r}")
        if 'vi' in [language_type] and not self.vietnamese_detected(item['text']):
            raise DropItem(f"no vietnamese detected: {item!r}")
        if 'zh' in [language_type] and not self.chinese_detected(item['text']):
            raise DropItem(f"no chinese detected: {item!r}")

        index = 'news_articles_'+language_type
        id = Simhash(item['text'], f=64, reg=r'[\S]').value

        doc = {
            'language_type': item['language_type'],
            'source': item['source'],
            'title': item['title'],
            'text': item['text'],
            'date': item['date'],
        }

        res = self.client.index(index=index, id=id, document=doc)
        if res.meta.status not in [200, 201]:
            print(res, flush=True)

        item['split_sentences']=self.sentence_split(item['language_type'], item['text'])

        return item

    def unwanted_character_filtered(self, text_for_filter):
        text = re.sub(r'[^a-zA-Z0-9\s\t{}{}{}{}{}{}{}{}{}{}]'.format(self.pattern_punctuation[1:-1],
                                                                     self.pattern_arabic[1:-1],
                                                                     self.pattern_chinese[1:-1],
                                                                     self.pattern_tamil[1:-1],
                                                                     self.pattern_thai[1:-1],
                                                                     self.pattern_russian[1:-1],
                                                                     self.pattern_korean[1:-1],
                                                                     self.pattern_japanese[1:-1],
                                                                     self.pattern_vietnamese[1:-1],
                                                                     self.pattern_emoji[1:-1],
                                                                     ), '', text_for_filter)
        return text

    def tamil_detected(self, text):
        if re.search(self.pattern_tamil, text):
            return True
        return False

    def vietnamese_detected(self, text):
        if re.search(self.pattern_vietnamese, text):
            return True
        return False

    def chinese_detected(self, text):
        if re.search(self.pattern_chinese, text):
            return True
        return False

    def thai_detected(self, text):
        if re.search(self.pattern_thai, text):
            return True
        return False

    def sentence_split(self, language_type, text):

        text = html.unescape(text.strip().replace('「', '“').replace('」', '”'))

        texts = [' '.join(text.split())
                 for text in text.split('\n') if text.strip()]

        if language_type in {'en', 'vi', 'ta', 'id', 'ms'}:
            return list(itertools.chain.from_iterable(self.sentence_splitter_en.segment(texts)))
        if language_type in {'zh'}:
            return list(itertools.chain.from_iterable(self.sentence_splitter_zh.segment(texts)))
        if language_type in {'th'}:
            sentences_list = [[str(sentence) for sentence in self.sentence_splitter_th.segment(
                text.strip())] for text in texts]
            return list(itertools.chain.from_iterable(sentences_list))
