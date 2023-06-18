import scrapy
from datetime import time, datetime
import json


class zh_ABC_Spider(scrapy.Spider):
    name = 'zh_ABC'
    allowed_domains = ['abc.net.au']
    start_urls = [
        'https://www.abc.net.au/news-web/api/loader/channelrefetch?name=PaginationArticles&documentId=13544740&size=250&total=250']

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):
        json_resp = json.loads(response.body)
        for item in json_resp['collection']:
            date_time_str = item["timestamp"]["dates"]["firstPublished"]
            date_time = datetime.strptime(date_time_str[:19], "%Y-%m-%dT%X")

            if date_time < self.start_time:
                continue

            elif date_time < self.end_time:
                date = str(date_time.date())
                yield response.follow(url=item["link"]["to"],
                                      callback=self.parse_article,
                                      cb_kwargs={"date": date})

    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = response.xpath('//h1[@data-component="Heading"]/text()').get()
        texts = response.xpath(
            '//div[@id="body"]/div/div/div/p/text()').getall()
        text = "\n".join(texts)

        if text:
            yield {"date": date,
                   "title": title,
                   "text": text}

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub

