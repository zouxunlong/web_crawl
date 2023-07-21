import scrapy
from datetime import time, datetime


class en_Weekender_Spider(scrapy.Spider):
    name = 'en_Weekender'
    allowed_domains = ['weekender.com.sg']
    start_urls = ['https://weekender.com.sg/?post_type=post']

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):

        articles = response.xpath('//div[@class="rb-col-m12"]')

        for article in articles:

            date_time_str = article.xpath('.//abbr[@class="date published"]/@title').get()
            date_time = datetime.strptime(date_time_str[:-6], "%Y-%m-%dT%H:%M:%S")

            if date_time < self.start_time:
                return
            elif date_time >= self.end_time:
                continue

            date = str(date_time.date())
            url = article.xpath('.//h2/a/@href').get()
            title = article.xpath('.//h2/a/@title').get()

            yield scrapy.Request(url=url,
                                 callback=self.parse_article,
                                 cb_kwargs={"date": date, "title": title})
                
        next_page_link = response.xpath('//a[@class="next page-numbers"]/@href').get()
        if next_page_link:
            yield scrapy.Request(next_page_link, callback=self.parse)


    def parse_article(self, response, *args, **kwargs):

        date = kwargs["date"]
        title = kwargs["title"]

        text_nodes = response.xpath('//div[@class="theiaPostSlider_slides"]/div/*[self::p or self::h3 or self::h2]')
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

