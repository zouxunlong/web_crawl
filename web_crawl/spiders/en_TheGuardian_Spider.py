from datetime import time, datetime
import scrapy


class en_TheGuardian_Spider(scrapy.Spider):
    name = 'en_theguardian'
    allowed_domains = ['theguardian.com']
    start_urls = [
        'https://www.theguardian.com/world/africa',
        'https://www.theguardian.com/world/europe-news/all',
        'https://www.theguardian.com/world/americas',
        'https://www.theguardian.com/australia-news/all',
        'https://www.theguardian.com/world/middleeast'
    ]

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):
        days = response.xpath('//section[@id]')
        for day in days:
            date_time_str = day.xpath('./@data-id').get()
            date_time = datetime.strptime(date_time_str, "%d %B %Y")

            if date_time < self.start_time:
                return
            elif date_time < self.end_time:
                date = str(date_time.date())
                articles = day.xpath('.//h3[@class="fc-item__title"]')
                for article in articles:
                    url = article.xpath('./a/@href').get()
                    title = ''.join(article.xpath('./a//text()').getall()).strip()
                    yield scrapy.Request(url=url,
                                         callback=self.parse_article,
                                         cb_kwargs={"date": date, "title": title})
        next_page_link = response.xpath('//link[@rel="next"]/@href').get()
        if next_page_link:
            yield scrapy.Request(response.urljoin(next_page_link), callback=self.parse)

    def parse_article(self, response, *args, **kwargs):

        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath(
            '//div[contains(@class, "article-body-commercial-selector")]/p')
        texts = [''.join(text_node.xpath(".//text()").getall()
                         ).replace(u'\xa0', " ") for text_node in text_nodes]
        text = "\n".join(texts[:])
        if text:
            yield {"date": date,
                   "title": title,
                   "text": text}


    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
