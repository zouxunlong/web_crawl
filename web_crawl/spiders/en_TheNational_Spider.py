from datetime import time, datetime, timedelta
import scrapy


class en_TheNational_Spider(scrapy.Spider):
    name = 'en_thenational'
    allowed_domains = ['thenational.scot']
    start_urls = []

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())
        self.base_url = 'https://www.thenational.scot/archive/'
        incremented_date = self.start_time
        while incremented_date < self.end_time:
            day = incremented_date.strftime('%Y/%m/%d')
            self.start_urls.append(self.base_url + day)
            incremented_date += timedelta(days=1)

    def parse(self, response):
        day_articles = response.xpath(
            '//li[@class="archive-module-list__article-list-item"]')
        for article in day_articles:
            date_time_str = response.url[-11:]
            date_time = datetime.strptime(date_time_str, "%Y/%m/%d/")
            date = str(date_time.date())
            url = article.xpath('.//h4/a/@href').get()
            title = article.xpath('.//h4/a/text()').get()
            yield response.follow(url=url,
                                  callback=self.parse_article,
                                  cb_kwargs={"date": date, "title": title})

    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[@class="article-body"]//p')
        texts=[''.join(text_node.xpath(".//text()").getall()).replace(u'\xa0', " ").replace(u'\u3000', " ").strip() for text_node in text_nodes]
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
