import json
import scrapy
from datetime import time, datetime


class en_MoscowTimes_Spider(scrapy.Spider):
    name = 'en_TheMoscowTimes'
    allowed_domains = ['themoscowtimes.com']
    start_urls = ['https://themoscowtimes.com/news/0']

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())
        self.base_url = 'https://themoscowtimes.com/news/'
        self.page = 0

    def parse(self, response):
        articles = response.xpath('//a')
        for article in articles:
            url = article.xpath("./@href").get()
            date_time_str = "-".join(url.split("/")[-4:-1])
            date_time = datetime.strptime(date_time_str, "%Y-%m-%d")
            if date_time < self.start_time:
                return
            elif date_time < self.end_time:

                date = str(date_time.date())
                title = article.xpath("./@title").get()

                yield scrapy.Request(url=url,
                                     callback=self.parse_article,
                                     cb_kwargs={"date": date, "title": title})

        self.page += 1
        next_page_link = self.base_url + str(18 * self.page)
        yield scrapy.Request(next_page_link, callback=self.parse)

    def parse_article(self, response, *args, **kwargs):

        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[@class="article__content"]/div/p')
        texts = [''.join(text_node.xpath(".//text()").getall()).replace(u'\xa0', " ").strip() for text_node in text_nodes if not text_node.xpath('.//script')]
        text = "\n".join(texts[:])
        if text:
            yield {"date": date,
                   "title": title,
                   "text": text}

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
