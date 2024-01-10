import json
import scrapy
from datetime import date, time, datetime, timedelta
from scrapy.crawler import CrawlerProcess


class zh_8world_Spider(scrapy.Spider):

    name = "zh_8world"
    allowed_domains = ["www.8world.com"]
    start_urls = ["https://www.8world.com/realtime"]

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):

        articles = response.xpath(
            '//div[@class="layout--third-third-one-third custom-row"]//article')

        for article in articles:

            date_time_str = article.xpath('.//time/@datetime').get()
            date_time = datetime.strptime(date_time_str, "%d/%m/%Y %H:%M")

            if date_time < self.start_time:
                return
            elif date_time >= self.end_time:
                continue

            date = str(date_time.date())
            title = article.xpath('.//h3/a/span/text()').get()
            url = article.xpath('.//h3/a/@href').get()

            yield response.follow(url=url,
                                  callback=self.parse_article,
                                  cb_kwargs={"date": date, "title": title})

        next_page_link = response.xpath(
            '//a[@title="Go to next page"]/@href').get()
        if next_page_link:
            yield response.follow(next_page_link, callback=self.parse)

    def parse_article(self, response, *args, **kwargs):

        date = kwargs["date"]
        title = kwargs["title"]

        text_nodes = response.xpath('//div[@class="text-long"]//p')
        texts = [''.join(text_node.xpath("./text()").getall()).replace('\n', " ")
                 for text_node in text_nodes if not text_node.xpath('.//script')]
        text = "\n".join([t.strip() for t in texts if t.strip()]).replace(
            u'\xa0', " ").replace(u'\u3000', " ")

        if text and title:
            yield {"date": date,
                   "source": self.name,
                   "title": title.strip(),
                   "text": text.strip()}

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub


def main():

    process = CrawlerProcess(
        settings={
            "FEEDS": {
                "/home/xuanlong/web_crawl/data/news_article/%(name)s/%(start_date)s_%(end_date)s.jsonl": {
                    "format": "jsonlines",
                    "overwrite": True,
                    "encoding": "utf8",
                }
            },
            "AUTOTHROTTLE_ENABLED": True,
            "LOG_LEVEL": "INFO",
            "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36",
        }
    )
    process.crawl(zh_8world_Spider, start_date=date.today() -
                  timedelta(3650), end_date=date.today() - timedelta(1))
    process.start()


if __name__ == "__main__":
    main()
