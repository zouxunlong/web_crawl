import html
import itertools
import re
from simhash import Simhash
from elasticsearch import Elasticsearch
from scrapy.exceptions import DropItem
from sentsplit.segment import SentSplit
from thai_segmenter import sentence_segment
import scrapy
from datetime import datetime
from scrapy.crawler import CrawlerProcess


class th_thansettakij_Spider(scrapy.Spider):

    name = 'th_thansettakij'
    allowed_domains = ['thansettakij.com']
    start_urls = ['https://thansettakij.com/world/{}'.format(str(id)) for id in range(20000, 571368)]
    translation = {"มกราคม": 1,
                   "กุมภาพันธ์": 2,
                   "มีนาคม": 3,
                   "เมษายน": 4,
                   "พฤษภาคม": 5,
                   "มิถุนายน": 6,
                   "กรกฎาคม": 7,
                   "สิงหาคม": 8,
                   "กันยายน": 9,
                   "ตุลาคม": 10,
                   "พฤศจิกายน": 11,
                   "ธันวาคม": 12,
                   }
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

    def __init__(self):
        self.ES_CONNECTION_STRING = "http://localhost:9200"
        self.sentence_splitter_en = SentSplit(
            'en', strip_spaces=True, maxcut=512)
        self.sentence_splitter_zh = SentSplit(
            'zh', strip_spaces=True, maxcut=150)
        self.sentence_splitter_th = sentence_segment
        self.client = Elasticsearch(
            self.ES_CONNECTION_STRING).options(ignore_status=400)

    def parse(self, response):

        date_time_str = response.xpath('//div[@class="date"]/text()').get()
        if not date_time_str:
            return
        day, month_thai, year_thai = date_time_str.split()
        month = self.translation[month_thai]
        year = int(year_thai) - 543
        date_time = datetime(year, month, int(day))

        date = str(date_time.date())

        title = response.xpath('//div[@class="contents"]//h1/text()').get()

        text_nodes = response.xpath('//div[@class="detail"]//div[@class="content-detail"]')

        texts = [''.join(text_node.xpath(".//text()").getall()) for text_node in text_nodes]
        text = "\n".join([t.strip() for t in texts if t.strip()]).replace(
            u'\xa0', "").replace(u'\u3000', " ").replace(u'\u200b', '')

        if text and title:

            item = {"date": date,
                    "source": self.name,
                    "title": title.strip(),
                    "text": text.strip()}

            language_type = self.name[:2]
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
            if 'th' in [language_type] and not self.thai_detected(item['text']):
                raise DropItem(f"no thai detected: {item!r}")

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

            item['split_sentences'] = self.sentence_split(
                item['language_type'], item['text'])

            yield item

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
            sentences_list = [[str(sentence) for sentence in self.sentence_splitter_th(
                text.strip())] for text in texts]
            return list(itertools.chain.from_iterable(sentences_list))

    def closed(self, reason):
        self.crawler.stats.set_value("spider_name", self.name)

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass
    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub


def main():

    process = CrawlerProcess(
        settings={
            "FEEDS": {
                '/home/xuancong/web_crawl/data/%(name)s/%(name)s.jsonl': {
                    "format": "jsonlines",
                    "overwrite": True,
                    "encoding": "utf8",
                },
            },
            "LOG_LEVEL": "INFO",
            "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36",
        }
    )
    process.crawl(th_thansettakij_Spider)
    process.start()


if __name__ == "__main__":
    main()
