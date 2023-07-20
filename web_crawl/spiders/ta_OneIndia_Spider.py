import scrapy
from datetime import time, datetime


class ta_OneIndia_Spider(scrapy.Spider):
    name = 'ta_oneindia'
    allowed_domains = ['oneindia.com']
    start_urls = [
        'https://tamil.oneindia.com/news/india/',
        'https://tamil.oneindia.com/news/international/',
        'https://tamil.oneindia.com/news/srilanka/',
        'https://tamil.oneindia.com/news/tamilnadu/'
    ]

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):

        articles = response.xpath(
            '//div[@class="oi-cityblock-list"]/ul/li[@class="clearfix oilistcontainer"]')
        for article in articles:
            date = article.xpath(
                './/div[@class="cityblock-time oi-datetime-cat"]/text()').get()
            date_time_str = ' '.join(date.split()[1:-1]).strip()
            date_time = datetime.strptime(date_time_str, "%B %d, %Y, %H:%M")

            if date_time < self.start_time:
                return
            elif date_time < self.end_time:
                date = str(date_time.date())
                title = article.xpath('./div/div/a/text()').get()
                url = article.xpath('./div/div/a/@href').get()
                yield response.follow(url=url,
                                      callback=self.parse_article,
                                      cb_kwargs={"date": date, "title": title})

        next_page_link = response.xpath(
            '//a[@class="oi-city-next"]/@href').get()
        if next_page_link:
            yield scrapy.Request(response.urljoin(next_page_link), callback=self.parse)


    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[@class="oi-article-lt" or @class="listicalSliderContainer"]/p')
        texts=[''.join(text_node.xpath(".//text()").getall()).replace('\n', " ") for text_node in text_nodes if not text_node.xpath('.//script')]
        text = "\n".join([t.strip() for t in texts if t.strip()]).replace(u'\xa0', " ").replace(u'\u3000', " ")
        if text:
            yield {"date": date,
                   "source": self.name,
                   "title": title,
                   "text": text}

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
