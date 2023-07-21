import scrapy
from datetime import time, datetime


class zh_Newsmarket_Spider(scrapy.Spider):
    name = 'zh_Newsmarket'
    allowed_domains = ['www.newsmarket.com.tw']
    start_urls = [
        'https://www.newsmarket.com.tw/blog/category/news-policy/',
        'https://www.newsmarket.com.tw/blog/category/food-safety/',
        'https://www.newsmarket.com.tw/blog/category/knowledge/',
        'https://www.newsmarket.com.tw/blog/category/country-life/',
        'https://www.newsmarket.com.tw/blog/category/good-farming/',
        'https://www.newsmarket.com.tw/blog/category/food-education/',
        'https://www.newsmarket.com.tw/blog/category/people-and-history/',
        'https://www.newsmarket.com.tw/blog/category/raising-and-breeding/',
        'https://www.newsmarket.com.tw/blog/category/living-green-travel/',
        'https://www.newsmarket.com.tw/blog/category/opinion/',
    ]

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):

        articles = response.xpath('//*[@id="block-wrap-0"]//article')
        for article in articles:

            date_time_str = article.xpath(
                './div/div/div[3]/span/time/@datetime').get()
            date_time = datetime.strptime(
                date_time_str[:-6], "%Y-%m-%dT%H:%M:%S")

            if date_time < self.start_time:
                return
            elif date_time >= self.end_time:
                continue

            url = article.xpath("./div/div/div[2]/h3/a/@href").get()
            date = str(date_time.date())
            title = article.xpath("./div/div/div[2]/h3/a/text()").get()

            yield scrapy.Request(url=url,
                                 callback=self.parse_article,
                                 cb_kwargs={"date": date, "title": title})

        next_page_link = response.xpath(
            '//*[@id="block-wrap-0"]/div/div/div[2]/a[@class="next page-numbers"]/@href').get()
        if next_page_link and len(next_page_link) > 1:
            yield scrapy.Request(url=next_page_link, callback=self.parse)

    def parse_article(self, response, *args, **kwargs):

        date = kwargs["date"]
        title = kwargs["title"]

        text_nodes = response.xpath(
            '//article/div/div/*[self::p or self::h3][position() < last()]')
        texts = [''.join(text_node.xpath(".//text()").getall()).replace('\n', " ")
                 for text_node in text_nodes if not text_node.xpath('.//script')]
        text = "\n".join([t.strip() for t in texts if t.strip()]).replace(
            u'\xa0', " ").replace(u'\u3000', " ")
        if text and title:
            yield {"date": date,
                   "source": self.name,
                   "title": title,
                   "text": text}

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
