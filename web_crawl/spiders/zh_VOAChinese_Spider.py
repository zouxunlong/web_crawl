import scrapy
from datetime import time, datetime


class zh_VOAChinese_Spider(scrapy.Spider):
    name = 'zh_voachinese'
    allowed_domains = ['voachinese.com']
    start_urls = ['https://www.voachinese.com/z/1740']

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())
        self.page = 1
        self.base_url = 'https://www.voachinese.com/z/1740?p='

    def parse(self, response):

        articles = response.xpath('//ul[@id="ordinaryItems"]/li')

        for article in articles:

            date_time_str = article.xpath('.//span/text()').get()
            date_time = datetime.strptime(date_time_str, "%Y年%m月%d日")

            if date_time < self.start_time:
                return
            elif date_time >= self.end_time:
                continue

            date = str(date_time.date())
            title = article.xpath('.//h4/@title').get()
            url = article.xpath('.//@href').get()

            yield response.follow(url=url,
                                    callback=self.parse_article,
                                    cb_kwargs={"date": date, "title": title})

        next_page_link = self.base_url + str(self.page)
        yield scrapy.Request(next_page_link, callback=self.parse)
        self.page += 1

    def parse_article(self, response, *args, **kwargs):

        date = kwargs["date"]
        title = kwargs["title"]

        text_nodes = response.xpath('//div[@class="wsw"]/p')
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
