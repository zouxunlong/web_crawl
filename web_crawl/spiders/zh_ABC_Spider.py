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
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):
        json_resp = json.loads(response.body)
        for item in json_resp['collection']:
            date_time_str = item["timestamp"]["dates"]["firstPublished"]
            date_time = datetime.strptime(date_time_str[:19], "%Y-%m-%dT%X")

            if date_time < self.start_time:
                continue

            elif date_time >= self.end_time:
                continue

            date = str(date_time.date())
            url=item["link"]["to"]

            yield response.follow(url=url,
                                  callback=self.parse_article,
                                  cb_kwargs={"date": date})

    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = response.xpath('//h1[@data-component="Heading"]/text()').get()
        text_nodes = response.xpath('//div[@id="body"]/div/div/div/p')
        texts = [''.join(text_node.xpath(".//text()").getall()).replace('\n', " ")
                 for text_node in text_nodes if not text_node.xpath('.//script')]
        text = "\n".join([t.strip() for t in texts if t.strip()]).replace(
            u'\xa0', " ").replace(u'\u3000', " ")
        if text and title:
            yield {"date": date,
                   "source": self.name,
                   "title": title.strip(),
                   "text": text.strip()}

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
