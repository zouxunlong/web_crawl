import json
import scrapy
from datetime import date, time, datetime, timedelta
from scrapy.crawler import CrawlerProcess


class zh_8world_Spider(scrapy.Spider):

    name = "zh_8world"
    allowed_domains = ["www.8world.com"]
    start_urls = ["https://www.8world.com/realtime"]

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):

        articles = response.xpath(
            '//div[@class="layout--third-third-one-third custom-row"]//article')

        for article in articles:

            date_time_str = article.xpath('.//time/@datetime').get()
            date_time = datetime.strptime(date_time_str, "%d/%m/%Y %H:%M")

            if date_time < self.start_time:
                return
            elif date_time >= self.end_time:
                continue

            date = str(date_time.date())
            title = article.xpath('.//h3/a/span/text()').get()
            url = article.xpath('.//h3/a/@href').get()

            yield response.follow(url=url,
                                  callback=self.parse_article,
                                  cb_kwargs={"date": date, "title": title})

        next_page_link = response.xpath('//a[@title="Go to next page"]/@href').get()
        if next_page_link:
            yield response.follow(next_page_link, callback=self.parse)

    def parse_article(self, response, *args, **kwargs):

        date = kwargs["date"]
        title = kwargs["title"]
        byline_node=response.xpath('//a[@role="article"]')
        if not byline_node:
            return

        text_nodes = response.xpath('//div[@class="text-long"]//p')

        texts = [''.join(text_node.xpath("./text()").getall()).replace('\n', " ")
                 for text_node in text_nodes if not text_node.xpath('.//script')]
        text = "\n".join([t.strip() for t in texts if t.strip()]).replace(u'\xa0', " ").replace(u'\u3000', " ")

        if text and title:
            yield {"date": date,
                   "source": self.name,
                   "title": title.strip(),
                   "text": text.strip()}

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub

