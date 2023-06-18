import scrapy
from datetime import time, datetime
import json


class vi_Nhandan_Spider(scrapy.Spider):
    name = 'vi_nhandan'
    allowed_domains = ['nhandan.vn']
    start_urls = ['https://nhandan.vn/api/morenews-latest-0-0.html?phrase=']

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())
        self.i=0

    def parse(self, response):
        articles = json.loads(response.body)['data']['articles']
        sel = scrapy.Selector(text=articles)
        articles = sel.xpath('//article')
        for article in articles:

            date_time_str = article.xpath(
                './/a[@class="text text2"]/text()')[-1].get().strip()
            date_time = datetime.strptime(date_time_str, "%d/%m/%Y %H:%M")
            if date_time < self.start_time:
                return
            elif date_time < self.end_time:
                url = article.xpath('./h3/a/@href').get()
                date = str(date_time.date())
                title = article.xpath('./h3/a/@title').get()
                yield scrapy.Request(url=url,
                                     callback=self.parse_article,
                                     cb_kwargs={"date": date, "title": title})

        self.i += 1
        if self.i < 100:
            yield scrapy.Request('https://nhandan.vn/api/morenews-latest-0-{}.html?phrase='.format(str(self.i)), callback=self.parse)


    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[@class="article__body cms-body"]/p')
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
