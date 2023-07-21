import scrapy
from scrapy.crawler import CrawlerRunner
from dateutil.relativedelta import relativedelta
from datetime import date, time, datetime, timedelta
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging
from twisted.internet import reactor


class en_CNNSpider_spider(scrapy.Spider):
    name = 'en_CNN'
    allowed_domains = ['cnn.com']
    start_urls = []

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

        start = self.start_time.replace(day=1)

        while start <= self.end_time:
            year = str(start.year)
            month = str(start.month)
            self.start_urls.append(f'https://edition.cnn.com/article/sitemap-{year}-{month}.html')
            start += relativedelta(months=1)


    def parse(self, response):

        articles = response.xpath('//div[@class="sitemap-entry"]/ul/li')
        for article in articles:

            date_time_str = article.xpath('./span[@class="date"]/text()').get()
            date_time = datetime.strptime(date_time_str, "%Y-%m-%d")
            
            if date_time < self.start_time:
                return
            elif date_time >= self.end_time:
                continue

            date = str(date_time.date())
            title = article.xpath('./span[@class="sitemap-link"]/a/text()').get()
            url=response.urljoin(article.xpath(".//@href").get())

            yield scrapy.Request(url=url,
                                 callback=self.parse_article,
                                 cb_kwargs={"date": date, "title": title})


    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]

        text_nodes = response.xpath('//div[@class="article__content"]/p[@class="paragraph inline-placeholder"]')
        texts=[''.join(text_node.xpath(".//text()").getall()).replace('\n', " ") for text_node in text_nodes if not text_node.xpath('.//script')]
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

