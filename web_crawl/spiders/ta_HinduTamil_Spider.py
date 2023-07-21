import scrapy
from datetime import time, datetime


class ta_HinduTamil_Spider(scrapy.Spider):
    name = 'ta_hindutamil'
    allowed_domains = ['hindutamil.in']
    start_urls = [
        'https://www.hindutamil.in/news/tamilnadu',
        'https://www.hindutamil.in/news/india',
        'https://www.hindutamil.in/news/world',
        'https://www.hindutamil.in/news/business',
        'https://www.hindutamil.in/news/sports',
        'https://www.hindutamil.in/news/spiritual',
        'https://www.hindutamil.in/news/social-media',
        'https://www.hindutamil.in/news/technology'
    ]

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):

        articles = response.xpath('//div[@class="card"]')

        for article in articles:

            date_time_str = article.xpath('.//a[@class="date link-grey"]/text()').get()
            date_time = datetime.strptime(date_time_str, "%d %b, %Y")

            if date_time < self.start_time:
                return
            elif date_time >= self.end_time:
                continue

            date = str(date_time.date())
            title = article.xpath('.//a[@class="link-black"]/@title').get()
            url = article.xpath('.//a[@class="link-black"]/@href').get()

            yield scrapy.Request(url=url,
                                    callback=self.parse_article,
                                    cb_kwargs={"date": date, "title": title})

        next_page_link = response.xpath('//a[@title="Next"]/@href').get()
        if next_page_link:
            yield scrapy.Request(next_page_link, callback=self.parse)

    def parse_article(self, response, *args, **kwargs):

        date = kwargs["date"]
        title = kwargs["title"]

        text_nodes = response.xpath('//div[@id="pgContentPrint"]//p')
        texts=[''.join(text_node.xpath(".//text()").getall()).replace('\n', " ") for text_node in text_nodes if not text_node.xpath('.//script')]
        text = "\n".join([t.strip() for t in texts if t.strip()]).replace(u'\xa0', " ").replace(u'\u3000', " ")
        
        if text and title:
            yield {"date": date,
                   "source": self.name,
                   "title": title,
                   "text": text}

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
