import scrapy
from datetime import time, datetime
import json


class zh_Sina_Spider(scrapy.Spider):
    name = 'zh_Sina'
    allowed_domains = ['sina.com.cn']

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())
        self.start_urls = ['https://feed.mix.sina.com.cn/api/roll/get?pageid=153&lid=2509&k=&num=50&page={}'.format(str(x+1)) for x in range(50)]

    def parse(self, response):
        json_resp = json.loads(response.body)
        for item in json_resp['result']['data']:
            date_time = datetime.fromtimestamp(int(item['intime']))

            if date_time < self.start_time:
                return

            elif date_time >= self.end_time:
                continue

            url = item['url']
            date = str(date_time.date())
            title = item['title']

            yield scrapy.Request(url=url,
                                 callback=self.parse_article,
                                 cb_kwargs={"date": date, "title": title})

    def parse_article(self, response, *args, **kwargs):

        date = kwargs["date"]
        title = kwargs["title"]

        text_nodes = response.xpath('//div[@class="article"]//p')
        texts = [''.join(text_node.xpath(".//text()").getall()).replace('\n', " ")
                 for text_node in text_nodes if not text_node.xpath('.//script')]
        text = "\n".join([t.strip() for t in texts if t.strip()]).replace(
            u'\xa0', " ").replace(u'\u3000', " ")
        if text and title:
            yield {"date": date,
                   "source": self.name,
                   "title": title,
                   "text": text}

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass
    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
