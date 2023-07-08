import scrapy
from datetime import time, datetime
import json


class zh_CCTV_Spider(scrapy.Spider):
    name = 'zh_CCTV'
    allowed_domains = ['cctv.com']
    start_urls = [
        'https://news.cctv.com/2019/07/gaiban/cmsdatainterface/page/news_1.jsonp?cb=news']
    base_url_1 = 'https://news.cctv.com/2019/07/gaiban/cmsdatainterface/page/news_'
    base_url_2 = '.jsonp?cb=news'
    page = 1

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):
        json_resp = json.loads(response.text[5:-1])
        for item in json_resp['data']['list']:
            date_time_str = item["focus_date"]
            date_time = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M:%S")

            if date_time < self.start_time:
                return
            elif date_time < self.end_time:
                date = str(date_time.date())
                title = item["title"]
                yield response.follow(url=item["url"],
                                      callback=self.parse_article,
                                      cb_kwargs={"date": date, "title": title})
        if self.page == 8:
            return
        self.page += 1
        next_page_link = self.base_url_1 + str(self.page) + self.base_url_2
        yield scrapy.Request(next_page_link, callback=self.parse)

    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[@class="content_area" or @class="cnt_bd" or @class="text_area"]/p')
        texts=[''.join(text_node.xpath(".//text()").getall()).replace(u'\xa0', " ").replace(u'\u3000', " ") for text_node in text_nodes if not text_node.xpath('.//script')]
        text = "\n".join(texts[:])
        if text:
            yield {"date": date,
                   "title": title,
                   "text": text}

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass
    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
