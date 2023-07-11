import scrapy
from datetime import time, datetime, timedelta


class en_France24News_Spider(scrapy.Spider):
    name = 'en_france24news'
    allowed_domains = ['france24.com']
    start_urls = []

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())
        self.base_url = 'https://france24.com/en/archives/'
        incremented_date = self.start_time
        while incremented_date < self.end_time:
            day = incremented_date.strftime("%Y/%m/%d-%B-%Y")
            self.start_urls.append(self.base_url + day)
            incremented_date += timedelta(days=1)


    def parse(self, response):

        day_articles = response.xpath('//li[@class="o-archive-day__list__entry"]/a/@href').getall()
        for url in day_articles:
            yield response.follow(url=url, callback=self.parse_article)


    def parse_article(self, response):

        date_time_str = response.xpath('//time/@datetime').get()
        date_time = datetime.strptime(date_time_str, "%Y-%m-%dT%H:%M:%S%z")
        date = str(date_time.date())

        title = response.xpath('//h1[@class="t-content__title a-page-title"]/text()').get()
        text_nodes = response.xpath('//*[@class="t-content__body u-clearfix"]/*[self::p or self::h2]')
        texts = [''.join(text_node.xpath(".//text()").getall()).replace(u'\xa0', " ") for text_node in text_nodes]
        text = "\n".join(texts[:])
        if text:
            yield {"date": date,
                   "source": self.name,
                   "title": title,
                   "text": text}



    def warn_on_generator_with_return_value_stub(spider, callable):
        pass
    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub

