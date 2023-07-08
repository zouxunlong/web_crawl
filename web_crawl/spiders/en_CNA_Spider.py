import json
import scrapy
from datetime import time, datetime


class en_CNA_Spider(scrapy.Spider):

    name = "en_CNA"
    allowed_domains = ["www.channelnewsasia.com"]
    custom_settings = {"DOWNLOAD_DELAY": 0.5}

    start_urls_0 = ["https://www.channelnewsasia.com/api/v1/infinitelisting/94f7cd75-c28b-4c0a-8d21-09c6ba3dd3fc?_format=json&viewMode=infinite_scroll_listing&page=%d" %
                    n for n in range(1, 1685)]
    start_urls_1 = ["https://www.channelnewsasia.com/api/v1/infinitelisting/18e56af5-db43-434c-b76b-8c7a766235ef?_format=json&viewMode=infinite_scroll_listing&page=%d" %
                    n for n in range(1, 89)]
    start_urls_2 = ["https://www.channelnewsasia.com/api/v1/infinitelisting/f8f0f8b1-004c-486f-ac72-0c927b7b539d?_format=json&viewMode=infinite_scroll_listing&page=%d" %
                    n for n in range(1, 385)]
    start_urls_3 = ["https://www.channelnewsasia.com/api/v1/infinitelisting/1da7e932-70b3-4a2e-891f-88f7dd72c9d6?_format=json&viewMode=infinite_scroll_listing&page=%d" %
                    n for n in range(1, 956)]
    start_urls = start_urls_0+start_urls_1+start_urls_2+start_urls_3

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):
        data = json.loads(response.body)

        for item in data["result"]:

            date_time = datetime.strptime(
                item["release_date"], "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)

            # if date_time < self.start_time:
            #     return
            # elif date_time < self.end_time:
            date = str(date_time.date())
            title = item["title"]

            yield scrapy.Request(url=item["absolute_url"],
                                    callback=self.parse_article,
                                    cb_kwargs={"date": date, "title": title})

    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[@class="text-long"]/p')
        texts = [''.join(text_node.xpath(".//text()").getall()).replace(u'\xa0',
                                                                        " ").replace(u'\u3000', " ") for text_node in text_nodes]
        text = "\n".join(texts[:])
        if text:
            yield {"date": date,
                   "title": title,
                   "text": text}

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass
    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
