import scrapy
from datetime import time, datetime


class en_StraitsTimes_Spider(scrapy.Spider):
    name = 'en_straitstimes'
    allowed_domains = ['straitstimes.com']
    start_urls = [
        'https://www.straitstimes.com/singapore/latest',
        'https://www.straitstimes.com/asia/latest',
        'https://www.straitstimes.com/world/latest',
        'https://www.straitstimes.com/multimedia/latest',
        'https://www.straitstimes.com/tech/latest',
        'https://www.straitstimes.com/sport/latest',
        'https://www.straitstimes.com/business/latest',
        'https://www.straitstimes.com/life/latest',
    ]

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):

        articles = response.xpath('//div[@class="card-body"]')
        for article in articles:

            date_time_str = article.xpath('.//@data-created-timestamp').get()
            date_time = datetime.fromtimestamp(int(date_time_str))

            if date_time < self.start_time:
                return
            elif date_time < self.end_time:
                
                date = str(date_time.date())
                title = article.xpath('./h5/a/text()').get()
                url = article.xpath('./h5/a/@href').get()

                yield response.follow(url=url,
                                      callback=self.parse_article,
                                      cb_kwargs={"date": date, "title": title})

        next_page_link = response.xpath(
            '//a[@title="Go to next page"]/@href').get()
        if next_page_link:
            yield scrapy.Request(response.urljoin(next_page_link), callback=self.parse)

    def parse_article(self, response, *args, **kwargs):

        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath(
            '//div[@class="clearfix text-formatted field field--name-field-paragraph-text field--type-text-long field--label-hidden field__item"]/p')
        texts = [''.join(text_node.xpath(".//text()").getall()
                         ).replace(u'\xa0', " ") for text_node in text_nodes]
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
