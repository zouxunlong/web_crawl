import scrapy
from datetime import time, datetime



class vi_NguoiViet_Spider(scrapy.Spider):
    name = 'vi_nguoiviet'
    allowed_domains = ['nguoi-viet.com']
    start_urls = ['https://www.nguoi-viet.com/?s=+']

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):
        articles = response.xpath(
            '//div[@class="td-ss-main-content"]//div[@class="item-details"]')
        for article in articles:
            date_time_str = article.xpath('.//@datetime').get()
            date_time = datetime.strptime(date_time_str[:10], "%Y-%m-%d")

            if date_time < self.start_time:
                return
            elif date_time < self.end_time:
                date = str(date_time.date())
                title = article.xpath('.//@title').get()
                yield scrapy.Request(url=article.xpath('.//@href').get(),
                                     callback=self.parse_article,
                                     cb_kwargs={"date": date, "title": title})

        next_page_link = response.xpath(
            '//a[@aria-label="next-page"]/@href').get()
        if next_page_link:
            yield scrapy.Request(next_page_link, callback=self.parse)

    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[@class="tdb-block-inner td-fix-index"]/p')
        texts=[''.join(text_node.xpath(".//text()").getall()).replace(u'\xa0', " ").replace(u'\u3000', " ") for text_node in text_nodes if not text_node.xpath('.//script')]
        text = "\n".join(texts[:])
        if text:
            yield {"date": date,
                   "title": title,
                   "text": text}

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub

