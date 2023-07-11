import scrapy
from datetime import time, datetime, timedelta


class zh_Zaobao_Spider(scrapy.Spider):
    name = 'zh_zaobao'
    allowed_domains = ['zaobao.com.sg']
    start_urls = [
        'https://zaobao.com.sg/realtime/singapore',
        'https://zaobao.com.sg/realtime/china',
        'https://zaobao.com.sg/realtime/world',
        'https://zaobao.com.sg/realtime/finance'
    ]

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):

        articles = response.xpath('//div[@class="col col-lg-12 "]')
        for article in articles:
            date_time_str = article.xpath(
                './/span[@class="meta meta-published-date"]/text()').get().strip()

            if date_time_str[2] == '/':
                date_time = datetime.strptime(date_time_str, "%d/%m/%Y")
            elif date_time_str[-3:] == "分钟前":
                date_time = datetime.now() - \
                    timedelta(minutes=int(date_time_str[:-3]))
            elif date_time_str[-3:] == "小时前":
                date_time = datetime.now() - \
                    timedelta(hours=int(date_time_str[:-3]))
            else:
                return
            if date_time < self.start_time:
                return
            elif date_time < self.end_time:
                date = str(date_time.date())
                title = article.xpath(".//a//text()").get()
                yield response.follow(url=article.xpath(".//@href").get(),
                                      callback=self.parse_article,
                                      cb_kwargs={"date": date, "title": title})

        next_page_link = response.xpath(
            '//a[@class="pagination-link pagination-link-next"]/@href').get()
        if next_page_link:
            yield scrapy.Request(response.urljoin(next_page_link), callback=self.parse)

    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]

        texts = response.xpath('//div[@class="article-content-rawhtml"]/p//text()').getall()
        text = "\n".join(texts[:]).replace(u'\u3000', " ")
        if text:
            yield {"date": date,
                   "source": self.name,
                   "title": title,
                   "text": text}
        

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
