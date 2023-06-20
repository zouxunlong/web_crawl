import json
import scrapy
from datetime import time, datetime


class en_TechCrunch_Spider(scrapy.Spider):
    name = 'en_techcrunch'
    allowed_domains = ['techcrunch.com']
    start_urls = ['https://techcrunch.com/wp-json/tc/v1/magazine?page=1']


    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())
        self.base_url = 'https://techcrunch.com/wp-json/tc/v1/magazine?page='
        self.page = 1
        
    def parse(self, response):
        json_resp = json.loads(response.body)
        for item in json_resp:
            date_time_str = item["date"]
            date_time = datetime.strptime(date_time_str, "%Y-%m-%dT%X")

            if date_time < self.start_time:
                return
            elif date_time > self.end_time:

                date = str(date_time.date())
                url = item["link"]
                title = item["title"]["rendered"]

                yield response.follow(url=url,
                                      callback=self.parse_article,
                                      cb_kwargs={"date": date, "title": title})

        self.page += 1
        next_page_link = self.base_url + str(self.page)
        yield scrapy.Request(next_page_link, callback=self.parse)


    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[@class="article-content"]/p')
        texts=[''.join(text_node.xpath(".//text()").getall()).replace(u'\xa0', " ").replace(u'\u3000', " ") for text_node in text_nodes if not text_node.xpath('.//img')]
        text = "\n".join(texts[:])
        if text:
            yield {"date": date,
                   "title": title,
                   "text": text}   


    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
