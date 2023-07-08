import scrapy
from datetime import time, datetime, timedelta


class zh_ChinaNews_Spider(scrapy.Spider):
    name = 'zh_Chinanews'
    allowed_domains = ['chinanews.com.cn']
    start_urls = []

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

        date = self.start_time
        date_str = datetime.strftime(date, '%Y/%m%d')

        while date < self.end_time:
            self.start_urls.append(
                f'https://www.chinanews.com.cn/scroll-news/{date_str}/news.shtml')
            date += timedelta(days=1)
            date_str = datetime.strftime(date, '%Y/%m%d')

    def parse(self, response):
        response_url = response.request.url
        date_string = response_url[41:50]
        date = str(datetime.strptime(date_string, "%Y/%m%d").date())
        articles = response.xpath('//div[@class="content_list"]//li')
        for article in articles:
            url = article.xpath('./div[@class="dd_bt"]/a/@href').get()
            title = article.xpath('./div[@class="dd_bt"]/a/text()').get()
            if url:
                yield response.follow(url=url,
                                    callback=self.parse_article,
                                    cb_kwargs={"date": date, "title": title})

    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[@class="left_zw"]/p')
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
